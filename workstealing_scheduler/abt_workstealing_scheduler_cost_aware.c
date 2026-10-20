#include "abt_workstealing_scheduler_cost_aware.h"
#include "ws_task.h"
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <abt.h>

/* Прежний абсолютный порог дешёвой задачи; действует только при WS_CHEAP_ABSOLUTE=1. */
#define WS_CHEAP_TASK_COST 32LL

/* ===================== ГЛОБАЛЬНАЯ СТАТИСТИКА ===================== */
static int g_num_xstreams = 0;
static atomic_llong g_steal_operations;
static atomic_llong g_stolen_tasks;

/* Номера счётчиков диагностики на каждом ES:
 *   LOCAL - задача взята из своего пула;
 *   STEAL_OPS_EST / STOLEN_EST - кражи по оценкам и украденные в них задачи;
 *   STEAL_OPS_FB / STOLEN_FB - то же для запасного пути;
 *   EMPTY_VICTIM - жертва выбрана по оценкам, но её пул уже пуст;
 *   STOLEN_ZERO_EST - украдена задача с нулевой оценкой;
 *   STOLEN_RESUMED - украден ULT, созданный не рантаймом;
 *   IDLE - итерация цикла планировщика без выполненной задачи. */
enum {
    WS_STAT_LOCAL,
    WS_STAT_STEAL_OPS_EST,
    WS_STAT_STOLEN_EST,
    WS_STAT_STEAL_OPS_FB,
    WS_STAT_STOLEN_FB,
    WS_STAT_EMPTY_VICTIM,
    WS_STAT_STOLEN_ZERO_EST,
    WS_STAT_STOLEN_RESUMED,
    WS_STAT_IDLE,
    WS_STAT_COUNT
};

/* Счётчики одного ES. Пишет их только сам ES; записи разных ES лежат в разных
 * кэш-линиях (шаг g_es_stats_stride), поэтому обновления не мешают друг другу. */
typedef struct {
    atomic_llong v[WS_STAT_COUNT];
} ws_es_stats_t;

static char *g_es_stats = NULL;
static int g_es_stats_num = 0;
static size_t g_es_stats_stride = 0;

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
    int buf_head;                     /* голова буфера (только старый механизм) */
    int buf_tail;                     /* хвост буфера */
    int buf_capacity;                 /* ёмкость буфера */
} pool_meta_t;

static pool_meta_t *g_pool_meta = NULL;

/* Метаданные разных пулов обновляются разными ES. Если они лежат в одной
 * кэш-линии, каждое обновление вытесняет линию у соседей (ложное разделение).
 * 128 байт - кэш-линия Apple M-серии; на x86 это две линии по 64. */
#define WS_CACHE_LINE 128
static size_t g_pool_meta_stride = sizeof(pool_meta_t);

/* Метаданные пула ES rank с учётом выравнивания записей (WS_META_PADDING). */
static inline pool_meta_t *ws_meta(int rank) {
    return (pool_meta_t *)((char *)g_pool_meta + (size_t)rank * g_pool_meta_stride);
}

/* Прибавляет d к счётчику k своего ES. Пишет только сам ES, поэтому достаточно
 * relaxed-чтения и записи без атомарного сложения. */
static inline void ws_stat_add(int rank, int k, long long d) {
    atomic_llong *c;
    if (!g_es_stats || rank < 0 || rank >= g_es_stats_num) return;
    c = &((ws_es_stats_t *)(g_es_stats + (size_t)rank * g_es_stats_stride))->v[k];
    atomic_store_explicit(c, atomic_load_explicit(c, memory_order_relaxed) + d,
                          memory_order_relaxed);
}

/* Выделяет счётчики для num ES, каждый ES в своей кэш-линии; при повторном вызове
 * с тем же числом ES обнуляет их. */
static void ws_stats_alloc(int num) {
    size_t stride = (sizeof(ws_es_stats_t) + WS_CACHE_LINE - 1) / WS_CACHE_LINE * WS_CACHE_LINE;
    void *mem = NULL;
    if (g_es_stats && g_es_stats_num == num) {
        memset(g_es_stats, 0, (size_t)num * g_es_stats_stride);
        return;
    }
    free(g_es_stats);
    g_es_stats = NULL;
    g_es_stats_num = 0;
    if (posix_memalign(&mem, WS_CACHE_LINE, (size_t)num * stride) != 0) return;
    memset(mem, 0, (size_t)num * stride);
    g_es_stats = (char *)mem;
    g_es_stats_stride = stride;
    g_es_stats_num = num;
}

/* Отмечает в счётчиках украденную задачу без оценки: чужой ULT или задачу с нулевой оценкой. */
static void ws_stat_stolen(int rank, ABT_thread thread, long long est) {
    if (!ws_task_meta_of(thread)) {
        ws_stat_add(rank, WS_STAT_STOLEN_RESUMED, 1);
    } else if (est <= 0) {
        ws_stat_add(rank, WS_STAT_STOLEN_ZERO_EST, 1);
    }
}

/* Складывает счётчики всех ES в out. Вызывается после остановки ES. */
void ws_get_sched_stats(ws_sched_stats *out) {
    long long t[WS_STAT_COUNT] = {0};
    for (int r = 0; g_es_stats && r < g_es_stats_num; r++) {
        ws_es_stats_t *st = (ws_es_stats_t *)(g_es_stats + (size_t)r * g_es_stats_stride);
        for (int k = 0; k < WS_STAT_COUNT; k++) {
            t[k] += atomic_load_explicit(&st->v[k], memory_order_relaxed);
        }
    }
    out->local_tasks = t[WS_STAT_LOCAL];
    out->steal_ops_est = t[WS_STAT_STEAL_OPS_EST];
    out->stolen_est = t[WS_STAT_STOLEN_EST];
    out->steal_ops_fallback = t[WS_STAT_STEAL_OPS_FB];
    out->stolen_fallback = t[WS_STAT_STOLEN_FB];
    out->empty_victims = t[WS_STAT_EMPTY_VICTIM];
    out->stolen_zero_est = t[WS_STAT_STOLEN_ZERO_EST];
    out->stolen_resumed = t[WS_STAT_STOLEN_RESUMED];
    out->idle_iterations = t[WS_STAT_IDLE];
}

/* ===================== НАСТРОЙКИ ===================== */
/* Читаются из окружения один раз при создании планировщиков; 0 выключает. */
static int g_opt_fallback_stop_on_local = 1;   /* WS_FALLBACK_STOP_ON_LOCAL */
static int g_opt_fallback_random = 1;          /* WS_FALLBACK_RANDOM */
static int g_opt_continuation_on_thief = 1;    /* WS_CONTINUATION_ON_THIEF */
static int g_opt_meta_padding = 1;             /* WS_META_PADDING */
static int g_opt_steal_one = 0;               /* WS_STEAL_ONE: красть одну задачу */
static int g_opt_victim_queued = 1;           /* нагрузка жертвы только по очереди; WS_VICTIM_LOAD=total - 0 */
static int g_opt_cheap_absolute = 0;          /* WS_CHEAP_ABSOLUTE: прежний порог WS_CHEAP_TASK_COST */
static double g_opt_cheap_fraction = 0.0625;  /* WS_CHEAP_FRACTION: дешёвая задача - меньше этой доли цели кражи */
static int g_victim_sample = 1;               /* число кандидатов при выборе жертвы */

/* Число случайных кандидатов при выборе жертвы: WS_VICTIM_SAMPLE или round(log2 num),
 * но не больше num - 1 и не меньше 1. */
static int ws_victim_sample_for(int num) {
    const char *env = getenv("WS_VICTIM_SAMPLE");
    int k = 0;
    if (env && atoi(env) > 0) {
        k = atoi(env);
    } else {
        while ((1LL << (k + 1)) <= (long long)num) k++;
        if ((long long)num * num >= (1LL << (2 * k + 1))) k++;
    }
    if (k > num - 1) k = num - 1;
    if (k < 1) k = 1;
    return k;
}

/* Читает переключатель 0/1 из переменной окружения; def, если она не задана. */
static int ws_env_flag(const char *name, int def) {
    const char *v = getenv(name);
    if (!v || v[0] == '\0') return def;
    return atoi(v) != 0;
}

/* ===================== ОТЛАДОЧНЫЕ СЧЁТЧИКИ ===================== */
/* Обновляются только при сборке с -DWS_DEBUG_COST_CHECK; проверяют, что
   запущенная задача получила СВОЮ оценку стоимости. */
static atomic_llong g_dbg_dispatched;
static atomic_llong g_dbg_wrong_estimate;
static atomic_llong g_dbg_missing_estimate;
static atomic_llong g_dbg_foreign_with_estimate;

/* ===================== ДАННЫЕ ПЛАНИРОВЩИКА ===================== */
typedef struct {
    uint32_t event_freq;
    int rank;                   // Идентификатор исполнительного потока
    double local_total_time;    // Локальное суммарное время (историческое)
    int local_task_count;       // Локальное количество выполненных задач (историческое)
    unsigned int rng_state;     // Локальный генератор для sampled victim selection
} ws_sched_data_t;

/* ===================== УТИЛИТЫ ДЛЯ pool_meta ===================== */

/* Инициализирует атомарные агрегаты пула и буфер старого механизма оценок. */
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

#ifdef WS_LEGACY_EST_FIFO
/* Удваивает кольцевой буфер оценок старого механизма; вызывается под mutex пула. */
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
#endif /* WS_LEGACY_EST_FIFO */

#ifdef WS_LEGACY_EST_FIFO
/* Кладёт точную оценку в кольцевой FIFO пула (старый механизм сопоставления). */
static void ws_est_fifo_push(int rank, long long est) {
    if (!g_pool_meta) return;
    if (rank < 0 || rank >= g_num_xstreams) return;
    if (est < 0) est = 0;
    pool_meta_t *pm = ws_meta(rank);
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
}
#endif /* WS_LEGACY_EST_FIFO */

/* Задача поставлена в очередь пула rank: учитываем её в агрегатах. */
void ws_account_task_created(int rank, long long est) {
    if (!g_pool_meta) return;
    if (rank < 0 || rank >= g_num_xstreams) return;
    if (est < 0) est = 0;
#ifdef WS_LEGACY_EST_FIFO
    ws_est_fifo_push(rank, est);
#endif
    pool_meta_t *pm = ws_meta(rank);
    atomic_fetch_add_explicit(&pm->queued_estimated, est, memory_order_relaxed);
    atomic_fetch_add_explicit(&pm->queued_count, 1, memory_order_relaxed);
}

/* Задача покинула очередь пула rank (первый запуск). */
void ws_account_task_dispatched(int rank, long long est) {
    if (!g_pool_meta) return;
    if (rank < 0 || rank >= g_num_xstreams) return;
    if (est < 0) est = 0;
    pool_meta_t *pm = ws_meta(rank);
    atomic_fetch_sub_explicit(&pm->queued_estimated, est, memory_order_relaxed);
    atomic_fetch_sub_explicit(&pm->queued_count, 1, memory_order_relaxed);
}

/* УСТАРЕЛО: оценка теперь едет вместе с ULT (см. ws_thread_create).
 * Работает только в сборке со старым механизмом, иначе игнорируется. */
void ws_push_task_estimate(int rank, long long est) {
#ifdef WS_LEGACY_EST_FIFO
    ws_account_task_created(rank, est);
#else
    (void)rank;
    (void)est;
#endif
}

/* Списывает точную оценку задачи из FIFO очереди. */
long long ws_pop_task_estimate(int rank) {
    if (!g_pool_meta) return -1;
    if (rank < 0 || rank >= g_num_xstreams) return -1;
    pool_meta_t *pm = ws_meta(rank);
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

/* Задача начала выполняться на ES rank: учитывает её в агрегатах выполняемой работы. */
static void ws_start_task_execution(int rank, long long est) {
    pool_meta_t *pm;
    if (!g_pool_meta || est <= 0) return;
    if (rank < 0 || rank >= g_num_xstreams) return;
    pm = ws_meta(rank);
    atomic_fetch_add_explicit(&pm->running_estimated, est, memory_order_relaxed);
    atomic_fetch_add_explicit(&pm->running_count, 1, memory_order_relaxed);
}

/* Задача вернула управление планировщику ES rank: снимает её из выполняемой работы. */
static void ws_finish_task_execution(int rank, long long est) {
    pool_meta_t *pm;
    if (!g_pool_meta || est <= 0) return;
    if (rank < 0 || rank >= g_num_xstreams) return;
    pm = ws_meta(rank);
    atomic_fetch_sub_explicit(&pm->running_estimated, est, memory_order_relaxed);
    atomic_fetch_sub_explicit(&pm->running_count, 1, memory_order_relaxed);
}

/* Возвращает суммарную оценочную стоимость очереди (без running work). */
long long ws_get_pool_estimated_load(int rank) {
    if (!g_pool_meta) return 0;
    if (rank < 0 || rank >= g_num_xstreams) return 0;
    pool_meta_t *pm = ws_meta(rank);
    return atomic_load_explicit(&pm->queued_estimated, memory_order_relaxed);
}

/* Число задач в очереди пула rank по агрегатам планировщика. */
long long ws_get_pool_queued_count(int rank) {
    if (!g_pool_meta) return 0;
    if (rank < 0 || rank >= g_num_xstreams) return 0;
    return atomic_load_explicit(&ws_meta(rank)->queued_count, memory_order_relaxed);
}

/* Нагрузка пула rank вместе с выполняемыми задачами; используется при WS_VICTIM_LOAD=total. */
static long long ws_get_pool_total_load(int rank) {
    if (!g_pool_meta) return 0;
    if (rank < 0 || rank >= g_num_xstreams) return 0;
    pool_meta_t *pm = ws_meta(rank);
    long long queued =
        atomic_load_explicit(&pm->queued_estimated, memory_order_relaxed);
    long long running =
        atomic_load_explicit(&pm->running_estimated, memory_order_relaxed);
    return queued + running;
}

/* ===================== УТИЛИТЫ ===================== */

/* Случайный номер ES, отличный от self, или -1, если ES один. */
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

/* Выбор жертвы по оценкам: из нескольких случайных ES берёт ES с наибольшей суммой
 * оценок в очереди. Возвращает его номер и нагрузку или -1, если у всех кандидатов 0. */
static int ws_find_victim_sampled(int self, int num,
                                  unsigned int *rng_state, long long *victim_load_out) {
    int victim = -1;
    long long best_load = 0;

    if (g_pool_meta) {
        int sample_count = num - 1;
        if (sample_count > g_victim_sample) {
            sample_count = g_victim_sample;
        }
        for (int s = 0; s < sample_count; s++) {
            int candidate = ws_pick_random_other_pool(rng_state, self, num);
            long long cur;
            if (candidate < 0) {
                continue;
            }
            cur = g_opt_victim_queued ? ws_get_pool_estimated_load(candidate)
                                      : ws_get_pool_total_load(candidate);
            if (cur > best_load) {
                best_load = cur;
                victim = candidate;
            }
        }
        if (victim >= 0 && best_load > 0) {
            *victim_load_out = best_load;
            return victim;
        }
        return -1;
    }

    /* Без метаданных пулов оценивать нагрузку нечем: кражу по стоимости
     * не делаем, остаётся запасной путь ws_find_fallback_victim. */
    return -1;
}

/* Первый непустой пул среди чужих. Обход начинается со случайного пула:
 * иначе все простаивающие ES проверяют соседа первым и спорят за один пул. */
static int ws_find_fallback_victim(int num, ABT_pool *pools, unsigned int *rng_state)
{
    int start = 0;
    if (num <= 1) return -1;
    if (g_opt_fallback_random) {
        start = (int)(rand_r(rng_state) % (unsigned int)(num - 1));
    }
    for (int k = 0; k < num - 1; k++) {
        int target = 1 + (start + k) % (num - 1);
        size_t victim_size = 0;
        if (ABT_pool_get_size(pools[target], &victim_size) == ABT_SUCCESS &&
            victim_size > 0) {
            return target;
        }
    }
    return -1;
}

/* Оценка стоимости задачи, которую планировщик только что достал из пула
 * owner_rank. Основной путь читает её из самого ULT; старый путь (сборка с
 * -DWS_LEGACY_EST_FIFO) снимает голову отдельной очереди оценок. */
static long long ws_dispatch_cost(ABT_thread thread, int owner_rank)
{
    ws_task_meta *meta = ws_task_meta_of(thread);
    (void)meta;
#ifdef WS_DEBUG_COST_CHECK
    atomic_fetch_add_explicit(&g_dbg_dispatched, 1, memory_order_relaxed);
#endif

#ifdef WS_LEGACY_EST_FIFO
    long long est = ws_pop_task_estimate(owner_rank);
#ifdef WS_DEBUG_COST_CHECK
    if (!meta) {
        if (est >= 0) {
            atomic_fetch_add_explicit(&g_dbg_foreign_with_estimate, 1, memory_order_relaxed);
        }
    } else if (est < 0) {
        atomic_fetch_add_explicit(&g_dbg_missing_estimate, 1, memory_order_relaxed);
    } else if (est != meta->sched_est) {
        atomic_fetch_add_explicit(&g_dbg_wrong_estimate, 1, memory_order_relaxed);
    }
#endif
    return (est > 0) ? est : 0;
#else
    if (!meta) {
        /* Чужой ULT: primary, проснувшийся после join или барьера. Оценки у
         * него нет, и метаданные пула он не трогает. */
        return 0;
    }
    if (!meta->dispatched) {
        meta->dispatched = 1;
        ws_account_task_dispatched(owner_rank, meta->sched_est);
    }
    return meta->sched_est;
#endif
}

/* home_pool - пул, в который ULT вернётся, если заблокируется или уступит
 * управление; ABT_POOL_NULL оставляет прежний. */
static void ws_execute_task_with_estimate(ABT_thread thread, int exec_rank, long long est,
                                          ABT_pool home_pool)
{
    if (est < 0) {
        est = 0;
    }
    ws_start_task_execution(exec_rank, est);
    ABT_self_schedule(thread, home_pool);
    ws_finish_task_execution(exec_rank, est);
}

/* Обнуляет глобальные счётчики краж планировщика new. */
void ws_reset_steal_count(void) {
    atomic_store_explicit(&g_steal_operations, 0, memory_order_relaxed);
    atomic_store_explicit(&g_stolen_tasks, 0, memory_order_relaxed);
}

/* Число украденных задач (то же, что ws_get_stolen_tasks_count). */
long long ws_get_steal_count(void) {
    return atomic_load_explicit(&g_stolen_tasks, memory_order_relaxed);
}

/* Число успешных операций кражи. */
long long ws_get_steal_ops_count(void) {
    return atomic_load_explicit(&g_steal_operations, memory_order_relaxed);
}

/* Суммарное число украденных задач. */
long long ws_get_stolen_tasks_count(void) {
    return atomic_load_explicit(&g_stolen_tasks, memory_order_relaxed);
}


/* ===================== ФУНКЦИИ ПЛАНИРОВЩИКА ===================== */

/* Создаёт данные планировщика ES: частоту проверки событий и генератор случайных чисел. */
static int sched_init(ABT_sched sched, ABT_sched_config config) {
    ws_sched_data_t *p_data = (ws_sched_data_t *)calloc(1, sizeof(ws_sched_data_t));
    
    /* Читаем конфигурацию */
    ABT_sched_config_read(config, 1, &p_data->event_freq);
    
    /* Ранг ES здесь брать нельзя: по документации Argobots вызывающий init()
     * не определён, и на практике init выполняется на том ES, который создаёт
     * планировщик (обычно primary). Настоящий ранг берётся в sched_run. */
    p_data->rank = -1;
    
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


/* Цикл планировщика new на одном ES: своя задача; иначе кража по оценкам у самой
 * нагруженной из нескольких случайных жертв до половины её нагрузки; если оценённых жертв
 * нет - запасной путь: половина очереди первого непустого пула со случайного старта. */
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

    /* sched_run уже выполняется на своём ES, поэтому ранг определяется здесь.
     * От него зависит и учёт метаданных своего пула, и пересчёт глобального
     * номера жертвы в локальный индекс в повёрнутом списке пулов. */
    ABT_self_get_xstream_rank(&p_data->rank);
    p_data->rng_state =
        (unsigned int)time(NULL) ^ (unsigned int)(p_data->rank * 2654435761u);

    /* Украденная задача и её продолжение после join остаются у вора: иначе
     * проснувшийся ULT вернётся в пул жертвы, а его дочерние задачи уже
     * лежат в пуле вора. */
    ABT_pool stolen_home = g_opt_continuation_on_thief ? pools[0] : ABT_POOL_NULL;

    while (1) {
        ABT_thread thread;

         /* 1) Пытаемся взять задачу из локальной очереди (local index 0). */
        ABT_pool_pop_thread_ex(pools[0], &thread, ABT_POOL_CONTEXT_OWNER_PRIMARY);
        
        if (thread == ABT_THREAD_NULL) {
            /* Локальная очередь пуста - ищем, у кого красть (по текущим оценкам) */
            long long victim_load = 0;
            int victim = ws_find_victim_sampled(p_data->rank, num_pools,
                                               &p_data->rng_state, &victim_load);
            
            if (victim >= 0) {
                int victim_local_idx = global_to_local_pool_index(p_data->rank, victim, num_pools);
                long long target_cost = victim_load / 2;
                long long stolen_cost = 0;
                long long stolen_from_victim = 0;
                if (target_cost < 1) {
                    target_cost = 1;
                }

                while (stolen_cost < target_cost) {
                    ABT_pool_pop_thread_ex(pools[victim_local_idx], &thread, ABT_POOL_CONTEXT_OWNER_SECONDARY);
                    if (thread == ABT_THREAD_NULL) {
                        break;
                    }

                    /* Мы успешно взяли задачу из жертвы — уменьшаем её метаданные */
                    long long est = ws_dispatch_cost(thread, victim);
                    ws_stat_stolen(p_data->rank, thread, est);
                    stolen_from_victim++;
                    if (est > 0) {
                        stolen_cost += est;
                    }

                    /* Выполняем задачу на текущем ES (вор) */
                    ws_execute_task_with_estimate(thread, p_data->rank, est, stolen_home);

                    if (g_opt_steal_one) {
                        break;
                    }

                    if (est > 0 && (g_opt_cheap_absolute
                                        ? est <= WS_CHEAP_TASK_COST
                                        : (double)est < g_opt_cheap_fraction * (double)target_cost)) {
                        break;
                    }

                    ABT_pool_pop_thread_ex(pools[0], &thread, ABT_POOL_CONTEXT_OWNER_PRIMARY);
                    if (thread != ABT_THREAD_NULL) {
                        long long local_est = ws_dispatch_cost(thread, p_data->rank);
                        ws_stat_add(p_data->rank, WS_STAT_LOCAL, 1);
                        ws_execute_task_with_estimate(thread, p_data->rank, local_est,
                                                      ABT_POOL_NULL);
                        break;
                    }
                }

                if (stolen_from_victim > 0) {
                    atomic_fetch_add_explicit(&g_steal_operations, 1, memory_order_relaxed);
                    atomic_fetch_add_explicit(&g_stolen_tasks, stolen_from_victim,
                                              memory_order_relaxed);
                    ws_stat_add(p_data->rank, WS_STAT_STEAL_OPS_EST, 1);
                    ws_stat_add(p_data->rank, WS_STAT_STOLEN_EST, stolen_from_victim);
                } else {
                    ws_stat_add(p_data->rank, WS_STAT_EMPTY_VICTIM, 1);
                    ws_stat_add(p_data->rank, WS_STAT_IDLE, 1);
                }
            } else {
                int fallback_local_idx =
                    ws_find_fallback_victim(num_pools, pools, &p_data->rng_state);
                if (fallback_local_idx >= 1) {
                    long long stolen_from_victim = 0;
                    size_t victim_size = 0;
                    int steal_target = 1;
                    int victim_rank = (p_data->rank + fallback_local_idx) % num_pools;
                    if (!g_opt_steal_one &&
                        ABT_pool_get_size(pools[fallback_local_idx], &victim_size) ==
                            ABT_SUCCESS &&
                        victim_size > 1) {
                        steal_target = (int)(victim_size / 2);
                    }
                    for (int s = 0; s < steal_target; s++) {
                        ABT_pool_pop_thread_ex(pools[fallback_local_idx], &thread, ABT_POOL_CONTEXT_OWNER_SECONDARY);
                        if (thread == ABT_THREAD_NULL) {
                            break;
                        }
                        long long fb_est = ws_dispatch_cost(thread, victim_rank);
                        ws_stat_stolen(p_data->rank, thread, fb_est);
                        stolen_from_victim++;
                        ws_execute_task_with_estimate(thread, p_data->rank, fb_est, stolen_home);
                        if (g_opt_fallback_stop_on_local) {
                            size_t local_size = 0;
                            ABT_pool_get_size(pools[0], &local_size);
                            if (local_size > 0) {
                                break;
                            }
                        }
                    }
                    if (stolen_from_victim > 0) {
                        atomic_fetch_add_explicit(&g_steal_operations, 1, memory_order_relaxed);
                        atomic_fetch_add_explicit(&g_stolen_tasks, stolen_from_victim,
                                                  memory_order_relaxed);
                        ws_stat_add(p_data->rank, WS_STAT_STEAL_OPS_FB, 1);
                        ws_stat_add(p_data->rank, WS_STAT_STOLEN_FB, stolen_from_victim);
                    } else {
                        ws_stat_add(p_data->rank, WS_STAT_IDLE, 1);
                    }
                } else {
                    ws_stat_add(p_data->rank, WS_STAT_IDLE, 1);
                }
            }
        } else {
            /* Мы взяли локальную задачу — удаляем соответствующую оценку из локальных метаданных */
            long long est = ws_dispatch_cost(thread, p_data->rank);
            ws_stat_add(p_data->rank, WS_STAT_LOCAL, 1);
            /* Выполняем задачу */
            ws_execute_task_with_estimate(thread, p_data->rank, est, ABT_POOL_NULL);
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

/* Освобождает данные планировщика ES. */
static int sched_free(ABT_sched sched) {
    ws_sched_data_t *p_data;
    ABT_sched_get_data(sched, (void **)&p_data);
    free(p_data);
    return ABT_SUCCESS;
}

/* ===================== ПУБЛИЧНЫЙ ИНТЕРФЕЙС ===================== */

/* Создаёт num планировщиков new и метаданные пулов, читает все переключатели WS_*
 * и выделяет счётчики. ES i получает пулы со сдвигом, свой пул первым. */
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
    ws_debug_reset();
    g_opt_fallback_stop_on_local = ws_env_flag("WS_FALLBACK_STOP_ON_LOCAL", 1);
    g_opt_fallback_random = ws_env_flag("WS_FALLBACK_RANDOM", 1);
    g_opt_continuation_on_thief = ws_env_flag("WS_CONTINUATION_ON_THIEF", 1);
    g_opt_meta_padding = ws_env_flag("WS_META_PADDING", 1);
    g_opt_steal_one = ws_env_flag("WS_STEAL_ONE", 0);
    {
        const char *vl = getenv("WS_VICTIM_LOAD");
        const char *cf = getenv("WS_CHEAP_FRACTION");
        g_opt_victim_queued = !(vl && strcmp(vl, "total") == 0);
        g_opt_cheap_absolute = ws_env_flag("WS_CHEAP_ABSOLUTE", 0);
        g_opt_cheap_fraction = 0.0625;
        if (cf && cf[0] != '\0') {
            double v = strtod(cf, NULL);
            if (v >= 0) g_opt_cheap_fraction = v;
        }
    }
    g_victim_sample = ws_victim_sample_for(num);
    ws_stats_alloc(num);
    /* Инициализируем pool_meta для каждого пула */
    g_pool_meta_stride = sizeof(pool_meta_t);
    if (g_opt_meta_padding) {
        g_pool_meta_stride = (sizeof(pool_meta_t) + WS_CACHE_LINE - 1) / WS_CACHE_LINE * WS_CACHE_LINE;
    }
    {
        void *mem = NULL;
        if (posix_memalign(&mem, WS_CACHE_LINE, (size_t)num * g_pool_meta_stride) != 0) {
            mem = NULL;
        } else {
            memset(mem, 0, (size_t)num * g_pool_meta_stride);
        }
        g_pool_meta = (pool_meta_t *)mem;
    }
    for (i = 0; i < num; ++i) {
        if (pool_meta_init_one(ws_meta(i), 1024) != 0) {
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

/* ===================== ОТЛАДОЧНЫЕ СЧЁТЧИКИ ===================== */
/* Обнуляет отладочные счётчики соответствия задачи и оценки (сборка с WS_DEBUG_COST_CHECK). */
void ws_debug_reset(void) {
    atomic_init(&g_dbg_dispatched, 0);
    atomic_init(&g_dbg_wrong_estimate, 0);
    atomic_init(&g_dbg_missing_estimate, 0);
    atomic_init(&g_dbg_foreign_with_estimate, 0);
}

/* Сколько задач планировщик достал из пулов. */
long long ws_debug_dispatched(void) {
    return atomic_load_explicit(&g_dbg_dispatched, memory_order_relaxed);
}

/* Сколько задач получили чужую оценку (только старый механизм). */
long long ws_debug_wrong_estimate(void) {
    return atomic_load_explicit(&g_dbg_wrong_estimate, memory_order_relaxed);
}

/* Сколько задач рантайма остались без оценки. */
long long ws_debug_missing_estimate(void) {
    return atomic_load_explicit(&g_dbg_missing_estimate, memory_order_relaxed);
}

/* Сколько чужих ULT забрали чью-то оценку. */
long long ws_debug_foreign_with_estimate(void) {
    return atomic_load_explicit(&g_dbg_foreign_with_estimate, memory_order_relaxed);
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
            atomic_load_explicit(&ws_meta(i)->queued_estimated, memory_order_relaxed);
        long long running_est =
            atomic_load_explicit(&ws_meta(i)->running_estimated, memory_order_relaxed);
        long long queued_cnt =
            atomic_load_explicit(&ws_meta(i)->queued_count, memory_order_relaxed);
        long long running_cnt =
            atomic_load_explicit(&ws_meta(i)->running_count, memory_order_relaxed);
        printf("Пул %d: в очереди %lld задач на %lld, выполняется %lld задач на %lld\n",
               i, queued_cnt, queued_est, running_cnt, running_est);
    }
}
