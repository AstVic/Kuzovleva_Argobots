#include "abt_workstealing_scheduler_cost_aware.h"
#include <stdio.h>
#include <stdlib.h>
#include <abt.h>

/* ===================== ГЛОБАЛЬНАЯ СТАТИСТИКА ===================== */
typedef struct {
    double total_time;          // Суммарное время выполнения задач (история)
    int task_count;             // Количество выполненных задач (история)
} ws_global_load_t;

static ws_global_load_t *g_loads = NULL;
static ABT_mutex g_loads_mutex;
static int g_num_xstreams = 0;

/* ===================== МЕТАДАННЫЕ О ТЕКУЩИХ ОЧЕРЕДЯХ ===================== */
/* Для каждой очереди храним суммарную оценочную стоимость и FIFO буфер оценок.
   Доступ защищён mutex'ом пула. */
typedef struct {
    ABT_mutex mutex;
    double sum_estimated;   /* суммарная оценочная стоимость задач, которые сейчас в очереди */
    int count;              /* количество задач в очереди (оценочно) */

    double *est_buffer;     /* кольцевой буфер оценок (FIFO) */
    int buf_head, buf_tail;
    int buf_capacity;
} pool_meta_t;

static pool_meta_t *g_pool_meta = NULL;

/* ===================== ДАННЫЕ ПЛАНИРОВЩИКА ===================== */
typedef struct {
    uint32_t event_freq;
    int rank;                   // Идентификатор исполнительного потока
    double local_total_time;    // Локальное суммарное время (историческое)
    int local_task_count;       // Локальное количество выполненных задач (историческое)
} ws_sched_data_t;

/* ===================== УТИЛИТЫ ДЛЯ pool_meta ===================== */

static int pool_meta_init_one(pool_meta_t *pm, int initial_capacity) {
    pm->sum_estimated = 0.0;
    pm->count = 0;
    pm->buf_capacity = initial_capacity > 0 ? initial_capacity : 1024;
    pm->est_buffer = (double*)malloc(sizeof(double) * pm->buf_capacity);
    if (!pm->est_buffer) return -1;
    pm->buf_head = pm->buf_tail = 0;
    if (ABT_mutex_create(&pm->mutex) != ABT_SUCCESS) {
        free(pm->est_buffer);
        pm->est_buffer = NULL;
        return -1;
    }
    return 0;
}

/* push: добавляет оценку в хвост буфера */
void ws_push_task_estimate(int rank, double est) {
    if (!g_pool_meta) return;
    if (rank < 0 || rank >= g_num_xstreams) return;
    pool_meta_t *pm = &g_pool_meta[rank];
    ABT_mutex_lock(pm->mutex);

    /* ресайз если переполнение */
    int next_tail = (pm->buf_tail + 1) % pm->buf_capacity;
    if (next_tail == pm->buf_head) {
        /* расширяем буфер в 2 раза */
        int newcap = pm->buf_capacity * 2;
        double *nb = (double*)malloc(sizeof(double) * newcap);
        int i = 0;
        /* скопировать элементы FIFO */
        int idx = pm->buf_head;
        while (idx != pm->buf_tail) {
            nb[i++] = pm->est_buffer[idx];
            idx = (idx + 1) % pm->buf_capacity;
        }
        free(pm->est_buffer);
        pm->est_buffer = nb;
        pm->buf_capacity = newcap;
        pm->buf_head = 0;
        pm->buf_tail = i;
    }

    pm->est_buffer[pm->buf_tail] = est;
    pm->buf_tail = (pm->buf_tail + 1) % pm->buf_capacity;
    pm->sum_estimated += est;
    pm->count++;

    ABT_mutex_unlock(pm->mutex);
}

/* pop: удаляет оценку с головы (FIFO). Возвращает -1.0 если пусто. */
double ws_pop_task_estimate(int rank) {
    if (!g_pool_meta) return -1.0;
    if (rank < 0 || rank >= g_num_xstreams) return -1.0;
    pool_meta_t *pm = &g_pool_meta[rank];
    double est = -1.0;
    ABT_mutex_lock(pm->mutex);

    if (pm->buf_head != pm->buf_tail) {
        est = pm->est_buffer[pm->buf_head];
        pm->buf_head = (pm->buf_head + 1) % pm->buf_capacity;
        pm->sum_estimated -= est;
        pm->count--;
        if (pm->count < 0) pm->count = 0;
        if (pm->sum_estimated < 0.0) pm->sum_estimated = 0.0;
    }

    ABT_mutex_unlock(pm->mutex);
    return est;
}

/* Возвращает суммарную оценочную стоимость очереди (thread-safe read) */
double ws_get_pool_estimated_load(int rank) {
    if (!g_pool_meta) return 0.0;
    if (rank < 0 || rank >= g_num_xstreams) return 0.0;
    pool_meta_t *pm = &g_pool_meta[rank];
    double val;
    ABT_mutex_lock(pm->mutex);
    val = pm->sum_estimated;
    ABT_mutex_unlock(pm->mutex);
    return val;
}

/* ===================== УТИЛИТЫ ===================== */

/* Поиск жертвы — теперь на основе текущих оценочных сумм задач в очередях */
static int ws_find_heaviest_pool(int self, int num) {
    int i, victim = -1;
    double max_load = 0.0;

    if (g_pool_meta) {
        /* читаем sum_estimated для каждого пула */
        for (i = 0; i < num; i++) {
            if (i == self) continue;
            double cur = ws_get_pool_estimated_load(i);
            if (cur > max_load && cur > 0.0) {
                max_load = cur;
                victim = i;
            }
        }
        return victim;
    } else {
        /* fallback — использовать исторические данные g_loads */
        ABT_mutex_lock(g_loads_mutex);
        for (i = 0; i < num; i++) {
            if (i == self) continue;
            if (g_loads[i].total_time > max_load && g_loads[i].task_count > 0) {
                max_load = g_loads[i].total_time;
                victim = i;
            }
        }
        ABT_mutex_unlock(g_loads_mutex);
        return victim;
    }
}

/* Обновление исторической статистики (после выполнения задачи) */
void ws_update_task_time(double elapsed, int rank) {
    ABT_mutex_lock(g_loads_mutex);
    if (rank >= 0 && rank < g_num_xstreams) {
        g_loads[rank].total_time += elapsed;
        g_loads[rank].task_count++;
    }
    ABT_mutex_unlock(g_loads_mutex);
}

/* ===================== ФУНКЦИИ ПЛАНИРОВЩИКА ===================== */

static int sched_init(ABT_sched sched, ABT_sched_config config) {
    ws_sched_data_t *p_data = (ws_sched_data_t *)calloc(1, sizeof(ws_sched_data_t));
    
    /* Читаем конфигурацию */
    ABT_sched_config_read(config, 1, &p_data->event_freq);
    
    /* Получаем rank текущего исполнительного потока */
    ABT_xstream x;
    ABT_xstream_self(&x);
    ABT_xstream_get_rank(x, &p_data->rank);
    
    p_data->local_total_time = 0.0;
    p_data->local_task_count = 0;
    
    ABT_sched_set_data(sched, (void *)p_data);
    return ABT_SUCCESS;
}

static void sched_run(ABT_sched sched) {
    uint32_t work_count = 0;
    ws_sched_data_t *p_data;
    int num_pools;
    ABT_pool *pools;
    ABT_bool stop;

    ABT_sched_get_data(sched, (void **)&p_data);
    ABT_sched_get_num_pools(sched, &num_pools);
    pools = (ABT_pool *)malloc(num_pools * sizeof(ABT_pool));
    ABT_sched_get_pools(sched, num_pools, 0, pools);

    while (1) {
        ABT_thread thread;
        
        /* 1) Пытаемся взять задачу из локальной очереди */
        ABT_pool_pop_thread(pools[p_data->rank], &thread);
        
        if (thread == ABT_THREAD_NULL) {
            /* Локальная очередь пуста - ищем, у кого красть (по текущим оценкам) */
            int victim = ws_find_heaviest_pool(p_data->rank, num_pools);
            
            if (victim >= 0) {
                /* Пытаемся красть у выбранной жертвы */
                ABT_pool_pop_thread(pools[victim], &thread);
                if (thread != ABT_THREAD_NULL) {
                    /* Мы успешно взяли задачу из жертвы — уменьшаем её метаданные */
                    /* Попытка удалить одну оценку из жертвы */
                    (void)ws_pop_task_estimate(victim);
                    
                    /* Выполняем задачу на текущем ES (вор) */
                    ABT_self_schedule(thread, ABT_POOL_NULL);
                }
            } 
            /* если victim < 0 или поп неуспешен — просто продолжим */
        } else {
            /* Мы взяли локальную задачу — удаляем соответствующую оценку из локальных метаданных */
            (void)ws_pop_task_estimate(p_data->rank);
            /* Выполняем задачу */
            ABT_self_schedule(thread, ABT_POOL_NULL);
        }
        
        if (++work_count >= p_data->event_freq) {
            work_count = 0;
            ABT_sched_has_to_stop(sched, &stop);
            if (stop == ABT_TRUE) break;
            ABT_xstream_check_events(sched);
        }
    }
    
    free(pools);
}

static int sched_free(ABT_sched sched) {
    ws_sched_data_t *p_data;
    ABT_sched_get_data(sched, (void **)&p_data);
    free(p_data);
    return ABT_SUCCESS;
}

/* ===================== ПУБЛИЧНЫЙ ИНТЕРФЕЙС ===================== */

void ABT_create_ws_scheds_cost_aware(int num, ABT_pool *pools, ABT_sched *scheds) {
    int i, k;
    ABT_sched_config config;
    ABT_pool *sched_pools;

    ABT_sched_config_var cv_event_freq = { 
        .idx = 0, 
        .type = ABT_SCHED_CONFIG_INT 
    };

    ABT_sched_def sched_def = {
        .type = ABT_SCHED_TYPE_ULT,
        .init = sched_init,
        .run = sched_run,
        .free = sched_free,
        .get_migr_pool = NULL
    };

    /* Инициализируем глобальную статистику */
    g_num_xstreams = num;
    g_loads = (ws_global_load_t *)calloc(num, sizeof(ws_global_load_t));
    for (i = 0; i < num; i++) {
        g_loads[i].total_time = 0.0;
        g_loads[i].task_count = 0;
    }
    ABT_mutex_create(&g_loads_mutex);

    /* Инициализируем pool_meta для каждого пула */
    g_pool_meta = (pool_meta_t*)calloc(num, sizeof(pool_meta_t));
    for (i = 0; i < num; ++i) {
        if (pool_meta_init_one(&g_pool_meta[i], 1024) != 0) {
            fprintf(stderr, "Ошибка инициализации pool_meta для пула %d\n", i);
            /* продолжаем, но это плохо */
        }
    }

    /* Создаем конфигурацию планировщика */
    ABT_sched_config_create(&config, cv_event_freq, 10, ABT_sched_config_var_end);

    sched_pools = (ABT_pool *)malloc(num * sizeof(ABT_pool));
    for (i = 0; i < num; i++) {
        for (k = 0; k < num; k++) {
            sched_pools[k] = pools[(i + k) % num];
        }
        ABT_sched_create(&sched_def, num, sched_pools, config, &scheds[i]);
    }
    
    free(sched_pools);
    ABT_sched_config_free(&config);
}

/* Функция для получения текущей статистики (исторической) */
void ws_print_global_stats(void) {
    ABT_mutex_lock(g_loads_mutex);
    printf("\n=== Глобальная историческая статистика планировщика ===\n");
    for (int i = 0; i < g_num_xstreams; i++) {
        printf("Поток %d: время=%.6f, задачи=%d, текущая_оценка=%.6f, текущие_задачи=%d\n", 
               i, g_loads[i].total_time, g_loads[i].task_count,
               g_pool_meta ? g_pool_meta[i].sum_estimated : 0.0,
               g_pool_meta ? g_pool_meta[i].count : 0);
    }
    ABT_mutex_unlock(g_loads_mutex);
}
