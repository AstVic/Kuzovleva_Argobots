#include "abt_workstealing_scheduler_cost_aware.h"
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <abt.h>

#define WS_VICTIM_SAMPLE_SIZE 3
#define WS_LOAD_IMBALANCE_RATIO 1.15
#define WS_MIN_STEAL_COST 1LL
#define WS_CHEAP_TASK_COST 32LL

/* ===================== ГЛОБАЛЬНАЯ СТАТИСТИКА ===================== */
static int g_num_xstreams = 0;
static atomic_llong g_steal_operations;
static atomic_llong g_stolen_tasks;

/* ===================== МЕТАДАННЫЕ О ТЕКУЩИХ ОЧЕРЕДЯХ ===================== */
/* Для каждой очереди храним атомарные агрегаты и FIFO буфер точных оценок.
   Mutex защищает только кольцевой FIFO буфер. */
typedef struct {
    ABT_mutex mutex;
    atomic_llong queued_estimated;    /* суммарная оценочная стоимость задач, которые сейчас в очереди */
    atomic_llong queued_count;        /* количество задач в очереди (оценочно) */
    atomic_llong running_estimated;   /* суммарная оценочная стоимость задач, которые сейчас выполняются на ES */
    atomic_llong running_count;       /* количество задач, которые сейчас выполняются на ES */
    long long *est_buffer;            /* кольцевой FIFO буфер точных оценок задач */
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
    atomic_init(&pm->queued_estimated, 0);
    atomic_init(&pm->queued_count, 0);
    atomic_init(&pm->running_estimated, 0);
    atomic_init(&pm->running_count, 0);
    pm->buf_capacity = (initial_capacity > 0) ? initial_capacity : 1024;
    pm->est_buffer = (long long *)malloc(sizeof(long long) * pm->buf_capacity);
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
    long long *nb = (long long *)malloc(sizeof(long long) * newcap);
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
void ws_push_task_estimate(int rank, long long est) {
    if (!g_pool_meta) return;
    if (rank < 0 || rank >= g_num_xstreams) return;
    if (est < 0) est = 0;
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
    ABT_mutex_unlock(pm->mutex);

    atomic_fetch_add_explicit(&pm->queued_estimated, est, memory_order_relaxed);
    atomic_fetch_add_explicit(&pm->queued_count, 1, memory_order_relaxed);
}

/* Списывает точную оценку задачи из FIFO очереди. */
long long ws_pop_task_estimate(int rank) {
    if (!g_pool_meta) return -1;
    if (rank < 0 || rank >= g_num_xstreams) return -1;
    pool_meta_t *pm = &g_pool_meta[rank];
    long long est = -1;
    ABT_mutex_lock(pm->mutex);
    if (pm->buf_head != pm->buf_tail) {
        est = pm->est_buffer[pm->buf_head];
        pm->buf_head = (pm->buf_head + 1) % pm->buf_capacity;
    }
    ABT_mutex_unlock(pm->mutex);
    if (est >= 0) {
        atomic_fetch_sub_explicit(&pm->queued_estimated, est, memory_order_relaxed);
        atomic_fetch_sub_explicit(&pm->queued_count, 1, memory_order_relaxed);
    }
    return est;
}

static void ws_start_task_execution(int rank, long long est) {
    pool_meta_t *pm;
    if (!g_pool_meta || est <= 0) return;
    if (rank < 0 || rank >= g_num_xstreams) return;
    pm = &g_pool_meta[rank];
    atomic_fetch_add_explicit(&pm->running_estimated, est, memory_order_relaxed);
    atomic_fetch_add_explicit(&pm->running_count, 1, memory_order_relaxed);
}

static void ws_finish_task_execution(int rank, long long est) {
    pool_meta_t *pm;
    if (!g_pool_meta || est <= 0) return;
    if (rank < 0 || rank >= g_num_xstreams) return;
    pm = &g_pool_meta[rank];
    atomic_fetch_sub_explicit(&pm->running_estimated, est, memory_order_relaxed);
    atomic_fetch_sub_explicit(&pm->running_count, 1, memory_order_relaxed);
}

/* Возвращает суммарную оценочную стоимость очереди (без running work). */
long long ws_get_pool_estimated_load(int rank) {
    if (!g_pool_meta) return 0;
    if (rank < 0 || rank >= g_num_xstreams) return 0;
    pool_meta_t *pm = &g_pool_meta[rank];
    return atomic_load_explicit(&pm->queued_estimated, memory_order_relaxed);
}

static long long ws_get_pool_total_load(int rank) {
    if (!g_pool_meta) return 0;
    if (rank < 0 || rank >= g_num_xstreams) return 0;
    pool_meta_t *pm = &g_pool_meta[rank];
    long long queued =
        atomic_load_explicit(&pm->queued_estimated, memory_order_relaxed);
    long long running =
        atomic_load_explicit(&pm->running_estimated, memory_order_relaxed);
    return queued + running;
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

static int ws_find_victim_sampled(int self, int num, long long local_load,
                                  unsigned int *rng_state, long long *victim_load_out) {
    int victim = -1;
    long long best_load = 0;

    if (g_pool_meta) {
        int sample_count = num - 1;
        if (sample_count > WS_VICTIM_SAMPLE_SIZE) {
            sample_count = WS_VICTIM_SAMPLE_SIZE;
        }
        for (int s = 0; s < sample_count; s++) {
            int candidate = ws_pick_random_other_pool(rng_state, self, num);
            long long cur;
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
            (double)best_load > (double)local_load * WS_LOAD_IMBALANCE_RATIO) {
            *victim_load_out = best_load;
            return victim;
        }
        return -1;
    }

    /* Без метаданных пулов оценивать нагрузку нечем: кражу по стоимости
     * не делаем, остаётся запасной путь ws_find_fallback_victim. */
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

static void ws_execute_task_with_estimate(ABT_thread thread, int exec_rank, long long est)
{
    if (est < 0) {
        est = 0;
    }
    ws_start_task_execution(exec_rank, est);
    ABT_self_schedule(thread, ABT_POOL_NULL);
    ws_finish_task_execution(exec_rank, est);
}

void ws_reset_steal_count(void) {
    atomic_store_explicit(&g_steal_operations, 0, memory_order_relaxed);
    atomic_store_explicit(&g_stolen_tasks, 0, memory_order_relaxed);
}

long long ws_get_steal_count(void) {
    return atomic_load_explicit(&g_stolen_tasks, memory_order_relaxed);
}

long long ws_get_steal_ops_count(void) {
    return atomic_load_explicit(&g_steal_operations, memory_order_relaxed);
}

long long ws_get_stolen_tasks_count(void) {
    return atomic_load_explicit(&g_stolen_tasks, memory_order_relaxed);
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
            long long local_load = ws_get_pool_total_load(p_data->rank);
            long long victim_load = 0;
            int victim = ws_find_victim_sampled(p_data->rank, num_pools, local_load,
                                               &p_data->rng_state, &victim_load);
            
            if (victim >= 0) {
                int victim_local_idx = global_to_local_pool_index(p_data->rank, victim, num_pools);
                long long target_cost = (victim_load - local_load) / 2;
                long long stolen_cost = 0;
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
                    long long est = ws_pop_task_estimate(victim);
                    stolen_from_victim++;
                    if (est > 0) {
                        stolen_cost += est;
                    }

                    /* Выполняем задачу на текущем ES (вор) */
                    ws_execute_task_with_estimate(thread, p_data->rank, est);

                    if (est > 0 && est <= WS_CHEAP_TASK_COST) {
                        break;
                    }

                    ABT_pool_pop_thread(pools[0], &thread);
                    if (thread != ABT_THREAD_NULL) {
                        long long local_est = ws_pop_task_estimate(p_data->rank);
                        ws_execute_task_with_estimate(thread, p_data->rank, local_est);
                        break;
                    }
                }

                if (stolen_from_victim > 0) {
                    atomic_fetch_add_explicit(&g_steal_operations, 1, memory_order_relaxed);
                    atomic_fetch_add_explicit(&g_stolen_tasks, stolen_from_victim,
                                              memory_order_relaxed);
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
                        atomic_fetch_add_explicit(&g_steal_operations, 1, memory_order_relaxed);
                        atomic_fetch_add_explicit(&g_stolen_tasks, stolen_from_victim,
                                                  memory_order_relaxed);
                    }
                }
            }
        } else {
            /* Мы взяли локальную задачу — удаляем соответствующую оценку из локальных метаданных */
            long long est = ws_pop_task_estimate(p_data->rank);
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
    atomic_init(&g_steal_operations, 0);
    atomic_init(&g_stolen_tasks, 0);
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

/* Текущее состояние метаданных пулов: сколько оценочной работы стоит в
 * очереди и сколько выполняется прямо сейчас. */
void ws_print_global_stats(void) {
    if (!g_pool_meta) {
        printf("\n=== Метаданные пулов не инициализированы ===\n");
        return;
    }
    printf("\n=== Текущее состояние пулов планировщика ===\n");
    for (int i = 0; i < g_num_xstreams; i++) {
        long long queued_est =
            atomic_load_explicit(&g_pool_meta[i].queued_estimated, memory_order_relaxed);
        long long running_est =
            atomic_load_explicit(&g_pool_meta[i].running_estimated, memory_order_relaxed);
        long long queued_cnt =
            atomic_load_explicit(&g_pool_meta[i].queued_count, memory_order_relaxed);
        long long running_cnt =
            atomic_load_explicit(&g_pool_meta[i].running_count, memory_order_relaxed);
        printf("Пул %d: в очереди %lld задач на %lld, выполняется %lld задач на %lld\n",
               i, queued_cnt, queued_est, running_cnt, running_est);
    }
}
