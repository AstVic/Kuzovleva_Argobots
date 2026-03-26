#include "abt_workstealing_scheduler_cost_aware.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <abt.h>

#define WS_VICTIM_SAMPLE_SIZE 3
#define WS_LOAD_IMBALANCE_RATIO 1.15
#define WS_MIN_STEAL_COST 1.0
#define WS_CHEAP_TASK_COST 32.0

/* ===================== ГЛОБАЛЬНАЯ СТАТИСТИКА ===================== */
typedef struct {
    double total_time;          // Суммарное время выполнения задач (история)
    int task_count;             // Количество выполненных задач (история)
} ws_global_load_t;

static ws_global_load_t *g_loads = NULL;
static ABT_mutex g_loads_mutex;
static ABT_mutex g_steal_mutex;
static int g_num_xstreams = 0;
static long long g_steal_operations = 0;
static long long g_stolen_tasks = 0;

/* ===================== МЕТАДАННЫЕ О ТЕКУЩИХ ОЧЕРЕДЯХ ===================== */
/* Для каждой очереди храним суммарную оценочную стоимость и FIFO буфер оценок.
   Доступ защищён mutex'ом пула. */
typedef struct {
    ABT_mutex mutex;
    double queued_estimated;    /* суммарная оценочная стоимость задач, которые сейчас в очереди */
    int queued_count;           /* количество задач в очереди (оценочно) */
    double running_estimated;   /* суммарная оценочная стоимость задач, которые сейчас выполняются на ES */
    int running_count;          /* количество задач, которые сейчас выполняются на ES */
    double *est_buffer;     /* кольцевой FIFO буфер точных оценок задач */
    int buf_head;
    int buf_tail;
    int buf_capacity;
} pool_meta_t;

static pool_meta_t *g_pool_meta = NULL;

/* ===================== ДАННЫЕ ПЛАНИРОВЩИКА ===================== */
typedef struct {
    uint32_t event_freq;
    int rank;                   // Идентификатор исполнительного потока
    double local_total_time;    // Локальное суммарное время (историческое)
    int local_task_count;       // Локальное количество выполненных задач (историческое)
    unsigned int rng_state;     // Локальный генератор для sampled victim selection
} ws_sched_data_t;

/* ===================== УТИЛИТЫ ДЛЯ pool_meta ===================== */

static int pool_meta_init_one(pool_meta_t *pm, int initial_capacity) {
    pm->queued_estimated = 0.0;
    pm->queued_count = 0;
    pm->running_estimated = 0.0;
    pm->running_count = 0;
    pm->buf_capacity = (initial_capacity > 0) ? initial_capacity : 1024;
    pm->est_buffer = (double *)malloc(sizeof(double) * pm->buf_capacity);
    if (!pm->est_buffer) {
        return -1;
    }
    pm->buf_head = 0;
    pm->buf_tail = 0;
    if (ABT_mutex_create(&pm->mutex) != ABT_SUCCESS) {
        free(pm->est_buffer);
        pm->est_buffer = NULL;
        return -1;
    }
    return 0;
}

static int pool_meta_grow_locked(pool_meta_t *pm) {
    int newcap = pm->buf_capacity * 2;
    double *nb = (double *)malloc(sizeof(double) * newcap);
    if (!nb) {
        return -1;
    }

    int i = 0;
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
    return 0;
}

/* Увеличивает суммарную оценку и счётчик задач очереди, сохраняя точную оценку задачи в FIFO. */
void ws_push_task_estimate(int rank, double est) {
    if (!g_pool_meta) return;
    if (rank < 0 || rank >= g_num_xstreams) return;
    pool_meta_t *pm = &g_pool_meta[rank];
    ABT_mutex_lock(pm->mutex);

    int next_tail = (pm->buf_tail + 1) % pm->buf_capacity;
    if (next_tail == pm->buf_head) {
        if (pool_meta_grow_locked(pm) != 0) {
            ABT_mutex_unlock(pm->mutex);
            return;
        }
    }

    pm->est_buffer[pm->buf_tail] = est;
    pm->buf_tail = (pm->buf_tail + 1) % pm->buf_capacity;
    pm->queued_estimated += est;
    pm->queued_count++;
    ABT_mutex_unlock(pm->mutex);
}

/* Списывает точную оценку задачи из FIFO очереди. */
double ws_pop_task_estimate(int rank) {
    if (!g_pool_meta) return -1.0;
    if (rank < 0 || rank >= g_num_xstreams) return -1.0;
    pool_meta_t *pm = &g_pool_meta[rank];
    double est = -1.0;
    ABT_mutex_lock(pm->mutex);
    if (pm->buf_head != pm->buf_tail) {
        est = pm->est_buffer[pm->buf_head];
        pm->buf_head = (pm->buf_head + 1) % pm->buf_capacity;
        pm->queued_estimated -= est;
        pm->queued_count--;
        if (pm->queued_count < 0) pm->queued_count = 0;
        if (pm->queued_estimated < 0.0) pm->queued_estimated = 0.0;
    }
    ABT_mutex_unlock(pm->mutex);
    return est;
}

static void ws_start_task_execution(int rank, double est) {
    pool_meta_t *pm;
    if (!g_pool_meta || est <= 0.0) return;
    if (rank < 0 || rank >= g_num_xstreams) return;
    pm = &g_pool_meta[rank];
    ABT_mutex_lock(pm->mutex);
    pm->running_estimated += est;
    pm->running_count++;
    ABT_mutex_unlock(pm->mutex);
}

static void ws_finish_task_execution(int rank, double est) {
    pool_meta_t *pm;
    if (!g_pool_meta || est <= 0.0) return;
    if (rank < 0 || rank >= g_num_xstreams) return;
    pm = &g_pool_meta[rank];
    ABT_mutex_lock(pm->mutex);
    pm->running_estimated -= est;
    pm->running_count--;
    if (pm->running_count < 0) pm->running_count = 0;
    if (pm->running_estimated < 0.0) pm->running_estimated = 0.0;
    ABT_mutex_unlock(pm->mutex);
}

/* Возвращает суммарную оценочную стоимость очереди (без running work). */
double ws_get_pool_estimated_load(int rank) {
    if (!g_pool_meta) return 0.0;
    if (rank < 0 || rank >= g_num_xstreams) return 0.0;
    pool_meta_t *pm = &g_pool_meta[rank];
    double val;
    ABT_mutex_lock(pm->mutex);
    val = pm->queued_estimated;
    ABT_mutex_unlock(pm->mutex);
    return val;
}

static double ws_get_pool_total_load(int rank) {
    if (!g_pool_meta) return 0.0;
    if (rank < 0 || rank >= g_num_xstreams) return 0.0;
    pool_meta_t *pm = &g_pool_meta[rank];
    double val;
    ABT_mutex_lock(pm->mutex);
    val = pm->queued_estimated + pm->running_estimated;
    ABT_mutex_unlock(pm->mutex);
    return val;
}

/* ===================== УТИЛИТЫ ===================== */

static int ws_pick_random_other_pool(unsigned int *rng_state, int self, int num)
{
    int victim;
    if (num <= 1) {
        return -1;
    }
    victim = (int)(rand_r(rng_state) % (unsigned int)(num - 1));
    if (victim >= self) {
        victim++;
    }
    return victim;
}

static int ws_find_victim_sampled(int self, int num, double local_load,
                                  unsigned int *rng_state, double *victim_load_out) {
    int victim = -1;
    double best_load = 0.0;

    if (g_pool_meta) {
        int sample_count = num - 1;
        if (sample_count > WS_VICTIM_SAMPLE_SIZE) {
            sample_count = WS_VICTIM_SAMPLE_SIZE;
        }
        for (int s = 0; s < sample_count; s++) {
            int candidate = ws_pick_random_other_pool(rng_state, self, num);
            double cur;
            if (candidate < 0) {
                continue;
            }
            cur = ws_get_pool_total_load(candidate);
            if (cur > best_load) {
                best_load = cur;
                victim = candidate;
            }
        }
        if (victim >= 0 &&
            best_load > WS_MIN_STEAL_COST &&
            best_load > local_load * WS_LOAD_IMBALANCE_RATIO) {
            *victim_load_out = best_load;
            return victim;
        }
        return -1;
    }

    ABT_mutex_lock(g_loads_mutex);
    for (int i = 0; i < num; i++) {
        if (i == self) continue;
        if (g_loads[i].total_time > best_load && g_loads[i].task_count > 0) {
            best_load = g_loads[i].total_time;
            victim = i;
        }
    }
    ABT_mutex_unlock(g_loads_mutex);
    if (victim >= 0 && best_load > local_load * WS_LOAD_IMBALANCE_RATIO) {
        *victim_load_out = best_load;
        return victim;
    }
    return -1;
}

static int ws_find_fallback_victim(int self, int num, ABT_pool *pools)
{
    (void)self;
    for (int target = 1; target < num; target++) {
        size_t victim_size = 0;
        if (ABT_pool_get_size(pools[target], &victim_size) == ABT_SUCCESS &&
            victim_size > 0) {
            return target;
        }
    }
    return -1;
}

static void ws_execute_task_with_estimate(ABT_thread thread, int exec_rank, double est)
{
    if (est < 0.0) {
        est = 0.0;
    }
    ws_start_task_execution(exec_rank, est);
    ABT_self_schedule(thread, ABT_POOL_NULL);
    ws_finish_task_execution(exec_rank, est);
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

void ws_reset_steal_count(void) {
    ABT_mutex_lock(g_steal_mutex);
    g_steal_operations = 0;
    g_stolen_tasks = 0;
    ABT_mutex_unlock(g_steal_mutex);
}

long long ws_get_steal_count(void) {
    long long value;
    ABT_mutex_lock(g_steal_mutex);
    value = g_stolen_tasks;
    ABT_mutex_unlock(g_steal_mutex);
    return value;
}

long long ws_get_steal_ops_count(void) {
    long long value;
    ABT_mutex_lock(g_steal_mutex);
    value = g_steal_operations;
    ABT_mutex_unlock(g_steal_mutex);
    return value;
}

long long ws_get_stolen_tasks_count(void) {
    long long value;
    ABT_mutex_lock(g_steal_mutex);
    value = g_stolen_tasks;
    ABT_mutex_unlock(g_steal_mutex);
    return value;
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
    p_data->rng_state =
        (unsigned int)time(NULL) ^ (unsigned int)(p_data->rank * 2654435761u);
    
    ABT_sched_set_data(sched, (void *)p_data);
    return ABT_SUCCESS;
}

/* For scheduler with rotated pool order:
 * local index 0 corresponds to global pool [self_rank],
 * local index k corresponds to global pool [(self_rank + k) % num_pools]. */
static inline int global_to_local_pool_index(int self_rank, int global_pool_id, int num_pools) {
    int idx = (global_pool_id - self_rank) % num_pools;
    if (idx < 0) idx += num_pools;
    return idx;
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

         /* 1) Пытаемся взять задачу из локальной очереди (local index 0). */
        ABT_pool_pop_thread(pools[0], &thread);
        
        if (thread == ABT_THREAD_NULL) {
            /* Локальная очередь пуста - ищем, у кого красть (по текущим оценкам) */
            double local_load = ws_get_pool_total_load(p_data->rank);
            double victim_load = 0.0;
            int victim = ws_find_victim_sampled(p_data->rank, num_pools, local_load,
                                               &p_data->rng_state, &victim_load);
            
            if (victim >= 0) {
                int victim_local_idx = global_to_local_pool_index(p_data->rank, victim, num_pools);
                double target_cost = (victim_load - local_load) * 0.5;
                double stolen_cost = 0.0;
                long long stolen_from_victim = 0;
                if (target_cost < WS_MIN_STEAL_COST) {
                    target_cost = WS_MIN_STEAL_COST;
                }

                while (stolen_cost < target_cost) {
                    ABT_pool_pop_thread(pools[victim_local_idx], &thread);
                    if (thread == ABT_THREAD_NULL) {
                        break;
                    }

                    /* Мы успешно взяли задачу из жертвы — уменьшаем её метаданные */
                    double est = ws_pop_task_estimate(victim);
                    stolen_from_victim++;
                    if (est > 0.0) {
                        stolen_cost += est;
                    }

                    /* Выполняем задачу на текущем ES (вор) */
                    ws_execute_task_with_estimate(thread, p_data->rank, est);

                    if (est > 0.0 && est <= WS_CHEAP_TASK_COST) {
                        break;
                    }

                    ABT_pool_pop_thread(pools[0], &thread);
                    if (thread != ABT_THREAD_NULL) {
                        double local_est = ws_pop_task_estimate(p_data->rank);
                        ws_execute_task_with_estimate(thread, p_data->rank, local_est);
                        break;
                    }
                }

                if (stolen_from_victim > 0) {
                    ABT_mutex_lock(g_steal_mutex);
                    g_steal_operations++;
                    g_stolen_tasks += stolen_from_victim;
                    ABT_mutex_unlock(g_steal_mutex);
                }
            } else {
                int fallback_local_idx = ws_find_fallback_victim(p_data->rank, num_pools, pools);
                if (fallback_local_idx >= 1) {
                    long long stolen_from_victim = 0;
                    size_t victim_size = 0;
                    int steal_target = 1;
                    int victim_rank = (p_data->rank + fallback_local_idx) % num_pools;
                    if (ABT_pool_get_size(pools[fallback_local_idx], &victim_size) ==
                            ABT_SUCCESS &&
                        victim_size > 1) {
                        steal_target = (int)(victim_size / 2);
                    }
                    for (int s = 0; s < steal_target; s++) {
                        ABT_pool_pop_thread(pools[fallback_local_idx], &thread);
                        if (thread == ABT_THREAD_NULL) {
                            break;
                        }
                        stolen_from_victim++;
                        ws_execute_task_with_estimate(
                            thread, p_data->rank, ws_pop_task_estimate(victim_rank));
                    }
                    if (stolen_from_victim > 0) {
                        ABT_mutex_lock(g_steal_mutex);
                        g_steal_operations++;
                        g_stolen_tasks += stolen_from_victim;
                        ABT_mutex_unlock(g_steal_mutex);
                    }
                }
            }
        } else {
            /* Мы взяли локальную задачу — удаляем соответствующую оценку из локальных метаданных */
            double est = ws_pop_task_estimate(p_data->rank);
            /* Выполняем задачу */
            ws_execute_task_with_estimate(thread, p_data->rank, est);
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
    g_steal_operations = 0;
    g_stolen_tasks = 0;
    g_loads = (ws_global_load_t *)calloc(num, sizeof(ws_global_load_t));
    for (i = 0; i < num; i++) {
        g_loads[i].total_time = 0.0;
        g_loads[i].task_count = 0;
    }
    ABT_mutex_create(&g_loads_mutex);
    ABT_mutex_create(&g_steal_mutex);

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
               g_pool_meta ? (g_pool_meta[i].queued_estimated + g_pool_meta[i].running_estimated) : 0.0,
               g_pool_meta ? (g_pool_meta[i].queued_count + g_pool_meta[i].running_count) : 0);
    }
    ABT_mutex_unlock(g_loads_mutex);
}
