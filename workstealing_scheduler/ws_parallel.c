#include "ws_parallel.h"
#include "ws_runtime.h"
#include "ws_task.h"

#include <abt.h>
#include <limits.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define WS_STACK_SIZE (1u << 20)

typedef struct {
    ws_range_body body;
    void *arg;
    ws_range_cost cost;
    void *cost_arg;
    long grain;
    double grain_cost;
    double unit;
} ws_pf_ctx;

typedef struct {
    const ws_pf_ctx *ctx;
    long lo;
    long hi;
} ws_pf_range;

struct ws_group {
    ABT_thread *threads;
    size_t count;
    size_t capacity;
};

static atomic_long g_fork_ns;

static long ws_now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long)ts.tv_sec * 1000000000L + ts.tv_nsec;
}

static void ws_noop(void *arg)
{
    (void)arg;
}

static int ws_tuned(void)
{
    ws_mode_t mode = ws_runtime_mode();
    return mode == WS_MODE_OLD || mode == WS_MODE_NEW;
}

static long ws_env_long(const char *name, long def)
{
    const char *env = getenv(name);
    long v;
    if (!env || env[0] == '\0') return def;
    v = atol(env);
    return v > 0 ? v : def;
}

static long ws_measure_fork_ns(void)
{
    long best = LONG_MAX;
    for (int rep = 0; rep < 3; rep++) {
        long start = ws_now_ns();
        long ns;
        for (int i = 0; i < 64; i++) {
            ws_task_meta meta;
            ABT_thread thread = ABT_THREAD_NULL;
            if (ws_runtime_spawn_meta(ws_noop, NULL, 0, &meta, &thread) == ABT_SUCCESS) {
                ABT_thread_free(&thread);
            }
        }
        ns = (ws_now_ns() - start) / 64;
        if (ns < best) best = ns;
    }
    return best > 0 ? best : 1;
}

static long ws_target_ns(void)
{
    long fork = atomic_load_explicit(&g_fork_ns, memory_order_relaxed);
    if (fork == 0) {
        fork = ws_measure_fork_ns();
        atomic_store_explicit(&g_fork_ns, fork, memory_order_relaxed);
    }
    return ws_env_long("WS_PROBE_FACTOR", 10) * fork;
}

static int ws_probe_enabled(void)
{
    const char *env = getenv("WS_GRAIN");
    if (env && strcmp(env, "fixed") == 0) return 0;
    if (env && strcmp(env, "probe") == 0) return 1;
    return ws_tuned();
}

static size_t ws_queue_limit(void)
{
    long def = ws_tuned() ? ws_runtime_num_workers() : 8;
    return (size_t)ws_env_long("WS_QUEUE_LIMIT", def > 0 ? def : 1);
}

static int ws_can_spawn(void)
{
    int rank = ws_runtime_self_rank();
    return rank >= 0 && ws_runtime_pool_size(rank) < ws_queue_limit();
}

static int ws_pf_leaf(const ws_pf_ctx *c, long lo, long hi)
{
    if (hi - lo <= 1) return 1;
    if (c->cost) return c->cost(lo, hi, c->cost_arg) <= c->grain_cost;
    return hi - lo <= c->grain;
}

static long ws_pf_split(const ws_pf_ctx *c, long lo, long hi)
{
    long a = lo + 1;
    long b = hi - 1;
    double half;
    if (!c->cost) return lo + (hi - lo) / 2;
    half = c->cost(lo, hi, c->cost_arg) / 2.0;
    while (a < b) {
        long m = a + (b - a) / 2;
        if (c->cost(lo, m, c->cost_arg) < half) {
            a = m + 1;
        } else {
            b = m;
        }
    }
    return a;
}

static long long ws_pf_estimate(const ws_pf_ctx *c, long lo, long hi)
{
    double work = c->cost ? c->cost(lo, hi, c->cost_arg) : (double)(hi - lo);
    long long est;
    if (c->unit == 0.0) return 0;
    est = (long long)(c->unit > 0 ? work * c->unit : work);
    return est > 0 ? est : 1;
}

static void ws_pf_run(const ws_pf_ctx *c, long lo, long hi);

static void ws_pf_task(void *p)
{
    ws_pf_range *r = (ws_pf_range *)p;
    ws_pf_run(r->ctx, r->lo, r->hi);
}

static void ws_pf_run(const ws_pf_ctx *c, long lo, long hi)
{
    long mid;
    ws_pf_range right;
    ws_task_meta meta;
    ABT_thread thread = ABT_THREAD_NULL;
    if (ws_pf_leaf(c, lo, hi)) {
        c->body(lo, hi, c->arg);
        return;
    }
    mid = ws_pf_split(c, lo, hi);
    right.ctx = c;
    right.lo = mid;
    right.hi = hi;
    if (!ws_can_spawn() ||
        ws_runtime_spawn_meta(ws_pf_task, &right, ws_pf_estimate(c, mid, hi), &meta, &thread) !=
            ABT_SUCCESS) {
        ws_pf_run(c, lo, mid);
        ws_pf_run(c, mid, hi);
        return;
    }
    ws_pf_run(c, lo, mid);
    ABT_thread_free(&thread);
}

int ws_init(int num_workers)
{
    return ws_runtime_init(num_workers > 0 ? num_workers : ws_runtime_default_workers(),
                           WS_STACK_SIZE);
}

void ws_finalize(void)
{
    ws_runtime_finalize();
    atomic_store_explicit(&g_fork_ns, 0, memory_order_relaxed);
}

int ws_num_workers(void)
{
    return ws_runtime_is_active() ? ws_runtime_num_workers() : 1;
}

void ws_parallel_for_cost(long lo, long hi, ws_range_body body, void *arg, ws_range_cost cost,
                          void *cost_arg)
{
    ws_pf_ctx c;
    long n = hi - lo;
    if (n <= 0) return;
    if (n == 1 || !ws_runtime_is_active() || ws_runtime_self_rank() < 0) {
        body(lo, hi, arg);
        return;
    }
    c.body = body;
    c.arg = arg;
    c.cost = ws_tuned() ? cost : NULL;
    c.cost_arg = cost_arg;
    c.unit = ws_task_cost_enabled() ? 0.0 : -1.0;
    if (ws_probe_enabled()) {
        long target = ws_target_ns();
        long done = 0;
        long sz = 1;
        while (1) {
            long start, ns;
            if (sz > n - done) sz = n - done;
            start = ws_now_ns();
            body(lo + done, lo + done + sz, arg);
            ns = ws_now_ns() - start;
            done += sz;
            if (done == n) return;
            if (ns >= target) {
                double work = c.cost ? c.cost(lo + done - sz, lo + done, cost_arg) : (double)sz;
                if (ws_task_cost_enabled() && work > 0) c.unit = (double)ns / work;
                break;
            }
            sz *= 2;
        }
        c.grain = done;
        c.grain_cost = c.cost ? c.cost(lo, lo + done, cost_arg) : 0.0;
        ws_pf_run(&c, lo + done, hi);
        return;
    }
    {
        long parts = 8L * ws_runtime_num_workers();
        c.grain = (n + parts - 1) / parts;
        c.grain_cost = c.cost ? c.cost(lo, hi, cost_arg) / (double)parts : 0.0;
    }
    ws_pf_run(&c, lo, hi);
}

void ws_parallel_for(long lo, long hi, ws_range_body body, void *arg)
{
    ws_parallel_for_cost(lo, hi, body, arg, NULL, NULL);
}

void ws_parallel_for_grain(long lo, long hi, long grain, ws_range_body body, void *arg)
{
    ws_pf_ctx c;
    if (hi <= lo) return;
    if (!ws_runtime_is_active() || ws_runtime_self_rank() < 0) {
        body(lo, hi, arg);
        return;
    }
    c.body = body;
    c.arg = arg;
    c.cost = NULL;
    c.cost_arg = NULL;
    c.grain = grain > 0 ? grain : 1;
    c.grain_cost = 0.0;
    c.unit = ws_task_cost_enabled() ? 0.0 : -1.0;
    ws_pf_run(&c, lo, hi);
}

typedef struct {
    long lo;
    long n;
    long blocks;
    size_t elem_size;
    char *partials;
    ws_reduce_body body;
    void *arg;
} ws_reduce_ctx;

static void ws_reduce_blocks(long blo, long bhi, void *p)
{
    ws_reduce_ctx *r = (ws_reduce_ctx *)p;
    for (long b = blo; b < bhi; b++) {
        long lo = r->lo + r->n * b / r->blocks;
        long hi = r->lo + r->n * (b + 1) / r->blocks;
        if (hi > lo) r->body(lo, hi, r->partials + (size_t)b * r->elem_size, r->arg);
    }
}

void ws_parallel_reduce(long lo, long hi, size_t elem_size, const void *identity,
                        ws_reduce_body body, ws_reduce_combine combine, void *result, void *arg)
{
    ws_reduce_ctx r;
    long n = hi - lo;
    memcpy(result, identity, elem_size);
    if (n <= 0) return;
    r.lo = lo;
    r.n = n;
    r.blocks = 8L * ws_num_workers();
    if (r.blocks > n) r.blocks = n;
    r.elem_size = elem_size;
    r.body = body;
    r.arg = arg;
    r.partials = (char *)malloc((size_t)r.blocks * elem_size);
    if (!r.partials) {
        char *tmp = (char *)malloc(elem_size);
        memcpy(tmp, identity, elem_size);
        body(lo, hi, tmp, arg);
        combine(result, tmp, arg);
        free(tmp);
        return;
    }
    for (long b = 0; b < r.blocks; b++) {
        memcpy(r.partials + (size_t)b * elem_size, identity, elem_size);
    }
    ws_parallel_for(0, r.blocks, ws_reduce_blocks, &r);
    for (long b = 0; b < r.blocks; b++) {
        combine(result, r.partials + (size_t)b * elem_size, arg);
    }
    free(r.partials);
}

ws_group *ws_group_create(void)
{
    return (ws_group *)calloc(1, sizeof(ws_group));
}

int ws_group_spawn(ws_group *group, void (*fn)(void *), void *arg, long long hint)
{
    ABT_thread thread = ABT_THREAD_NULL;
    if (!group) return ABT_ERR_INV_ARG;
    if (!ws_runtime_is_active() || ws_runtime_self_rank() < 0) {
        fn(arg);
        return ABT_SUCCESS;
    }
    if (group->count == group->capacity) {
        size_t cap = group->capacity ? group->capacity * 2 : 16;
        ABT_thread *grown = (ABT_thread *)realloc(group->threads, cap * sizeof(ABT_thread));
        if (!grown) {
            fn(arg);
            return ABT_SUCCESS;
        }
        group->threads = grown;
        group->capacity = cap;
    }
    if (ws_runtime_spawn_hint(fn, arg, hint, NULL, &thread) != ABT_SUCCESS) {
        fn(arg);
        return ABT_SUCCESS;
    }
    group->threads[group->count++] = thread;
    return ABT_SUCCESS;
}

void ws_group_wait(ws_group *group)
{
    if (!group) return;
    for (size_t i = 0; i < group->count; i++) {
        ABT_thread_free(&group->threads[i]);
    }
    group->count = 0;
}

void ws_group_free(ws_group *group)
{
    if (!group) return;
    ws_group_wait(group);
    free(group->threads);
    free(group);
}
