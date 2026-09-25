#include "ws_runtime.h"
#include "abt_workstealing_scheduler.h"
#include "abt_workstealing_scheduler_cost_aware.h"
#include "ws_task.h"

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int g_active = 0;
static ws_mode_t g_mode = WS_MODE_DEFAULT;
static int g_num_xstreams = 0;
static ABT_xstream *g_xstreams = NULL;
static ABT_pool *g_pools = NULL;
static ABT_sched *g_scheds = NULL;
static atomic_uint g_round_robin;

static ws_mode_t mode_from_env(void)
{
    const char *mode = getenv("ABT_WS_SCHEDULER");
    if (!mode || mode[0] == '\0' || strcmp(mode, "default") == 0) return WS_MODE_DEFAULT;
    if (strcmp(mode, "randws") == 0) return WS_MODE_RANDWS;
    if (strcmp(mode, "new") == 0 || strcmp(mode, "cost-aware") == 0) return WS_MODE_NEW;
    return WS_MODE_OLD;
}

static void create_shared_pools(int n)
{
    for (int i = 0; i < n; i++) {
        ABT_pool_create_basic(ABT_POOL_FIFO, ABT_POOL_ACCESS_MPMC, ABT_TRUE, &g_pools[i]);
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
        create_shared_pools(n);
        if (g_mode == WS_MODE_NEW) {
            ABT_create_ws_scheds_cost_aware(n, g_pools, g_scheds);
            ws_reset_steal_count();
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

void ws_runtime_finalize(void)
{
    if (!g_active) return;
    g_active = 0;

    for (int i = 1; i < g_num_xstreams; i++) {
        ABT_xstream_join(g_xstreams[i]);
        ABT_xstream_free(&g_xstreams[i]);
    }
    ABT_finalize();

    free(g_xstreams);
    free(g_pools);
    free(g_scheds);
    g_xstreams = NULL;
    g_pools = NULL;
    g_scheds = NULL;
    g_num_xstreams = 0;
}

int ws_runtime_is_active(void)
{
    return g_active;
}

ws_mode_t ws_runtime_mode(void)
{
    return g_mode;
}

int ws_runtime_num_workers(void)
{
    return g_active ? g_num_xstreams : 1;
}

int ws_runtime_self_rank(void)
{
    int rank;
    if (!g_active) return -1;
    if (ABT_self_get_xstream_rank(&rank) != ABT_SUCCESS) return -1;
    if (rank < 0 || rank >= g_num_xstreams) return -1;
    return rank;
}

ABT_pool ws_runtime_pool(int rank)
{
    if (!g_active || rank < 0 || rank >= g_num_xstreams) return ABT_POOL_NULL;
    return g_pools[rank];
}

size_t ws_runtime_pool_size(int rank)
{
    size_t size = 0;
    if (!g_active || rank < 0 || rank >= g_num_xstreams) return 0;
    ABT_pool_get_size(g_pools[rank], &size);
    return size;
}

static int spawn_to(int rank, void (*fn)(void *), void *arg, long long est,
                    ws_task_meta *meta, ABT_thread *thread)
{
    if (!g_active || rank < 0 || rank >= g_num_xstreams) return ABT_ERR_INV_ARG;
    if (g_mode == WS_MODE_NEW) {
        if (meta) {
            meta->fn = fn;
            meta->arg = arg;
            meta->est = est;
            return ws_thread_create_with_meta(g_pools[rank], rank, meta, thread);
        }
        return ws_thread_create(g_pools[rank], rank, fn, arg, est, thread);
    }
    return ABT_thread_create(g_pools[rank], fn, arg, ABT_THREAD_ATTR_NULL, thread);
}

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

int ws_runtime_spawn_to(int rank, void (*fn)(void *), void *arg, long long est,
                        ABT_thread *thread)
{
    return spawn_to(rank, fn, arg, est, NULL, thread);
}

int ws_runtime_spawn(void (*fn)(void *), void *arg, long long est, ABT_thread *thread)
{
    if (!g_active) return ABT_ERR_UNINITIALIZED;
    return spawn_to(spawn_rank(), fn, arg, est, NULL, thread);
}

int ws_runtime_spawn_meta(void (*fn)(void *), void *arg, long long est, ws_task_meta *meta,
                          ABT_thread *thread)
{
    if (!g_active) return ABT_ERR_UNINITIALIZED;
    return spawn_to(spawn_rank(), fn, arg, est, meta, thread);
}

void ws_runtime_reset_steal_stats(void)
{
    if (g_mode == WS_MODE_NEW) ws_reset_steal_count();
    else if (g_mode == WS_MODE_OLD) ws_old_reset_steal_count();
}

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
