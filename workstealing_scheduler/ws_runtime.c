#define _GNU_SOURCE
#include "ws_runtime.h"
#include "abt_workstealing_scheduler.h"
#include "abt_workstealing_scheduler_cost_aware.h"
#include "ws_task.h"

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>

/* Состояние рантайма: режим, ES, их пулы и планировщики (индекс - номер ES). */
static int g_active = 0;
static ws_mode_t g_mode = WS_MODE_DEFAULT;
static int g_num_xstreams = 0;
static ABT_xstream *g_xstreams = NULL;
static ABT_pool *g_pools = NULL;
static ABT_sched *g_scheds = NULL;
/* Счётчик раздачи задач по кругу в режиме default. */
static atomic_uint g_round_robin;

/* Режим планирования из ABT_WS_SCHEDULER; неизвестное значение означает old. */
static ws_mode_t mode_from_env(void)
{
    const char *mode = getenv("ABT_WS_SCHEDULER");
    if (!mode || mode[0] == '\0' || strcmp(mode, "default") == 0) return WS_MODE_DEFAULT;
    if (strcmp(mode, "randws") == 0) return WS_MODE_RANDWS;
    if (strcmp(mode, "randws_fifo") == 0) return WS_MODE_RANDWS_FIFO;
    if (strcmp(mode, "new") == 0 || strcmp(mode, "cost-aware") == 0) return WS_MODE_NEW;
    return WS_MODE_OLD;
}

/* Тип пула для режима: randws - дек, randws_fifo - FIFO, old и new - по WS_POOL. */
static ABT_pool_kind pool_kind_for_mode(ws_mode_t mode)
{
    const char *pool;
    if (mode == WS_MODE_RANDWS) return ABT_POOL_RANDWS;
    if (mode == WS_MODE_RANDWS_FIFO) return ABT_POOL_FIFO;
    pool = getenv("WS_POOL");
    if (pool && strcmp(pool, "randws") == 0) return ABT_POOL_RANDWS;
    return ABT_POOL_FIFO;
}

/* Создаёт по одному пулу MPMC на каждый ES. */
static void create_shared_pools(int n, ABT_pool_kind kind)
{
    for (int i = 0; i < n; i++) {
        ABT_pool_create_basic(kind, ABT_POOL_ACCESS_MPMC, ABT_TRUE, &g_pools[i]);
    }
}

/* Пулы для ES i передаются со сдвигом: локальный индекс 0 - свой пул. */
static void create_randws_scheds(int n)
{
    ABT_pool *rotated = (ABT_pool *)malloc(sizeof(ABT_pool) * n);
    for (int i = 0; i < n; i++) {
        for (int k = 0; k < n; k++) {
            rotated[k] = g_pools[(i + k) % n];
        }
        ABT_sched_create_basic(ABT_SCHED_RANDWS, n, rotated, ABT_SCHED_CONFIG_NULL, &g_scheds[i]);
    }
    free(rotated);
}

/* Запускает Argobots на num_xstreams ES в режиме из ABT_WS_SCHEDULER: пулы, планировщики,
 * а в режиме new - ещё и историю оценок (если не WS_COST=iters). Вызывающий становится
 * главным ULT на ES 0. */
int ws_runtime_init(int num_xstreams, size_t default_stack_size)
{
    int n = (num_xstreams > 0) ? num_xstreams : 1;
    char buf[32];
    int ret;

    if (g_active) return ABT_SUCCESS;

    if (default_stack_size > 0) {
        snprintf(buf, sizeof(buf), "%zu", default_stack_size);
        setenv("ABT_THREAD_STACKSIZE", buf, 0);
    }

    ret = ABT_init(0, NULL);
    if (ret != ABT_SUCCESS) return ret;

    g_mode = mode_from_env();
    g_num_xstreams = n;
    g_xstreams = (ABT_xstream *)calloc(n, sizeof(ABT_xstream));
    g_pools = (ABT_pool *)calloc(n, sizeof(ABT_pool));
    g_scheds = (ABT_sched *)calloc(n, sizeof(ABT_sched));
    atomic_init(&g_round_robin, 0);

    ABT_xstream_self(&g_xstreams[0]);

    if (g_mode == WS_MODE_DEFAULT) {
        for (int i = 1; i < n; i++) {
            ABT_xstream_create(ABT_SCHED_NULL, &g_xstreams[i]);
        }
        for (int i = 0; i < n; i++) {
            ABT_xstream_get_main_pools(g_xstreams[i], 1, &g_pools[i]);
        }
    } else {
        create_shared_pools(n, pool_kind_for_mode(g_mode));
        if (g_mode == WS_MODE_NEW) {
            const char *cost = getenv("WS_COST");
            ABT_create_ws_scheds_cost_aware(n, g_pools, g_scheds);
            ws_reset_steal_count();
            if (!(cost && strcmp(cost, "iters") == 0)) {
                ws_task_cost_init(n);
            }
        } else if (g_mode == WS_MODE_OLD) {
            ABT_create_ws_scheds(n, g_pools, g_scheds);
            ws_old_reset_steal_count();
        } else {
            create_randws_scheds(n);
        }
        ABT_xstream_set_main_sched(g_xstreams[0], g_scheds[0]);
        for (int i = 1; i < n; i++) {
            ABT_xstream_create(g_scheds[i], &g_xstreams[i]);
        }
    }

    g_active = 1;
    return ABT_SUCCESS;
}

/* Короткое имя программы для строк статистики. */
const char *ws_runtime_prog_name(void)
{
#if defined(__APPLE__)
    return getprogname();
#elif defined(__GLIBC__)
    return program_invocation_short_name;
#else
    return "-";
#endif
}

/* Дописывает счётчики планировщика в файл WS_STATS, если переменная задана. */
static void dump_stats(void)
{
    const char *path = getenv("WS_STATS");
    FILE *f;
    if (!path || path[0] == '\0') return;
    if (g_mode != WS_MODE_NEW && g_mode != WS_MODE_OLD) return;
    f = fopen(path, "a");
    if (!f) return;
    if (g_mode == WS_MODE_NEW) {
        ws_sched_stats st;
        ws_get_sched_stats(&st);
        fprintf(f,
                "prog=%s mode=new local=%lld steal_ops_est=%lld stolen_est=%lld "
                "steal_ops_fallback=%lld stolen_fallback=%lld empty_victims=%lld "
                "stolen_zero_est=%lld stolen_resumed=%lld idle=%lld",
                ws_runtime_prog_name(), st.local_tasks, st.steal_ops_est, st.stolen_est,
                st.steal_ops_fallback, st.stolen_fallback, st.empty_victims,
                st.stolen_zero_est, st.stolen_resumed, st.idle_iterations);
        if (ws_task_cost_enabled()) {
            double err, work;
            long long n;
            ws_task_cost_accuracy(&err, &work, &n);
            fprintf(f, " est_error=%.4f est_tasks=%lld", work > 0 ? err / work : 0.0, n);
        }
        fprintf(f, "\n");
    } else {
        fprintf(f, "prog=%s mode=old steal_ops=%lld stolen=%lld\n", ws_runtime_prog_name(),
                ws_old_get_steal_ops_count(), ws_old_get_stolen_tasks_count());
    }
    fclose(f);
}

/* Останавливает ES, выводит статистику, завершает Argobots и освобождает таблицы истории. */
void ws_runtime_finalize(void)
{
    if (!g_active) return;
    g_active = 0;

    for (int i = 1; i < g_num_xstreams; i++) {
        ABT_xstream_join(g_xstreams[i]);
        ABT_xstream_free(&g_xstreams[i]);
    }
    dump_stats();
    ABT_finalize();
    ws_task_cost_finalize();

    free(g_xstreams);
    free(g_pools);
    free(g_scheds);
    g_xstreams = NULL;
    g_pools = NULL;
    g_scheds = NULL;
    g_num_xstreams = 0;
}

/* 1, если рантайм инициализирован. */
int ws_runtime_is_active(void)
{
    return g_active;
}

/* Текущий режим планирования. */
ws_mode_t ws_runtime_mode(void)
{
    return g_mode;
}

/* Число ES. */
int ws_runtime_num_workers(void)
{
    return g_active ? g_num_xstreams : 1;
}

/* Номер ES вызывающего или -1 вне рантайма. */
int ws_runtime_self_rank(void)
{
    int rank;
    if (!g_active) return -1;
    if (ABT_self_get_xstream_rank(&rank) != ABT_SUCCESS) return -1;
    if (rank < 0 || rank >= g_num_xstreams) return -1;
    return rank;
}

/* Пул ES rank. */
ABT_pool ws_runtime_pool(int rank)
{
    if (!g_active || rank < 0 || rank >= g_num_xstreams) return ABT_POOL_NULL;
    return g_pools[rank];
}

/* Число готовых задач в пуле ES rank. */
size_t ws_runtime_pool_size(int rank)
{
    size_t size = 0;
    if (!g_active || rank < 0 || rank >= g_num_xstreams) return 0;
    ABT_pool_get_size(g_pools[rank], &size);
    return size;
}

/* Создаёт задачу в пуле ES rank. В режиме new - через ws_task с оценкой est и подсказкой
 * hint (метаданные в meta или в куче), в остальных режимах - обычный ULT. */
static int spawn_to(int rank, void (*fn)(void *), void *arg, long long est, long long hint,
                    const void *key, ws_task_meta *meta, ABT_thread *thread)
{
    if (!g_active || rank < 0 || rank >= g_num_xstreams) return ABT_ERR_INV_ARG;
    if (g_mode == WS_MODE_NEW) {
        if (meta) {
            meta->fn = fn;
            meta->arg = arg;
            meta->est = est;
            meta->hint = hint;
            meta->key = key;
            return ws_thread_create_with_meta(g_pools[rank], rank, meta, thread);
        }
        return ws_thread_create_hint(g_pools[rank], rank, fn, arg, est, hint, thread);
    }
    return ABT_thread_create(g_pools[rank], fn, arg, ABT_THREAD_ATTR_NULL, thread);
}

/* Пул для новой задачи: свой ES, а в режиме default - по кругу, так как кражи нет. */
static int spawn_rank(void)
{
    int rank;
    if (g_mode == WS_MODE_DEFAULT) {
        return (int)(atomic_fetch_add_explicit(&g_round_robin, 1, memory_order_relaxed) %
                     (unsigned)g_num_xstreams);
    }
    rank = ws_runtime_self_rank();
    return rank < 0 ? 0 : rank;
}

/* Создаёт задачу в пуле заданного ES. */
int ws_runtime_spawn_to(int rank, void (*fn)(void *), void *arg, long long est,
                        ABT_thread *thread)
{
    return spawn_to(rank, fn, arg, est, 0, NULL, NULL, thread);
}

/* Создаёт задачу в пуле своего ES с оценкой est. */
int ws_runtime_spawn(void (*fn)(void *), void *arg, long long est, ABT_thread *thread)
{
    if (!g_active) return ABT_ERR_UNINITIALIZED;
    return spawn_to(spawn_rank(), fn, arg, est, 0, NULL, NULL, thread);
}

/* Создаёт задачу с метаданными в памяти вызывающего (meta живёт до завершения задачи). */
int ws_runtime_spawn_meta(void (*fn)(void *), void *arg, long long est, ws_task_meta *meta,
                          ABT_thread *thread)
{
    if (!g_active) return ABT_ERR_UNINITIALIZED;
    return spawn_to(spawn_rank(), fn, arg, est, 0, NULL, meta, thread);
}

/* Создаёт задачу с метаданными в meta и ключом истории key (функция тела задачи). */
int ws_runtime_spawn_keyed(void (*fn)(void *), void *arg, long long est, const void *key,
                           ws_task_meta *meta, ABT_thread *thread)
{
    if (!g_active) return ABT_ERR_UNINITIALIZED;
    return spawn_to(spawn_rank(), fn, arg, est, 0, key, meta, thread);
}

/* Создаёт задачу с подсказкой стоимости hint в единицах программы. */
int ws_runtime_spawn_hint(void (*fn)(void *), void *arg, long long hint, ws_task_meta *meta,
                          ABT_thread *thread)
{
    if (!g_active) return ABT_ERR_UNINITIALIZED;
    return spawn_to(spawn_rank(), fn, arg, 0, hint, NULL, meta, thread);
}

/* Число ES по умолчанию: WS_NUM_THREADS или число ядер. */
int ws_runtime_default_workers(void)
{
    const char *env = getenv("WS_NUM_THREADS");
    long n;
    if (env && atoi(env) > 0) return atoi(env);
    n = sysconf(_SC_NPROCESSORS_ONLN);
    return n > 0 ? (int)n : 1;
}

/* Обнуляет счётчики краж текущего планировщика. */
void ws_runtime_reset_steal_stats(void)
{
    if (g_mode == WS_MODE_NEW) ws_reset_steal_count();
    else if (g_mode == WS_MODE_OLD) ws_old_reset_steal_count();
}

/* Возвращает число операций кражи и украденных задач текущего планировщика. */
void ws_runtime_get_steal_stats(long long *steal_operations, long long *stolen_tasks)
{
    long long ops = 0, tasks = 0;
    if (g_mode == WS_MODE_NEW) {
        ops = ws_get_steal_ops_count();
        tasks = ws_get_stolen_tasks_count();
    } else if (g_mode == WS_MODE_OLD) {
        ops = ws_old_get_steal_ops_count();
        tasks = ws_old_get_stolen_tasks_count();
    }
    if (steal_operations) *steal_operations = ops;
    if (stolen_tasks) *stolen_tasks = tasks;
}
