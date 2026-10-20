#include "ws_parallel.h"
#include "ws_task.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int failures = 0;

static void check(int ok, const char *name)
{
    printf("%-48s %s\n", name, ok ? "ok" : "FAIL");
    if (!ok) failures++;
}

typedef struct {
    int *hits;
} hits_arg;

static void mark(long lo, long hi, void *p)
{
    hits_arg *a = (hits_arg *)p;
    for (long i = lo; i < hi; i++) __atomic_fetch_add(&a->hits[i], 1, __ATOMIC_RELAXED);
}

static int all_once(const int *hits, long n)
{
    for (long i = 0; i < n; i++) {
        if (hits[i] != 1) return 0;
    }
    return 1;
}

static double triangle(long lo, long hi, void *p)
{
    (void)p;
    return ((double)hi * (hi + 1) - (double)lo * (lo + 1)) / 2.0;
}

static void heavy(long lo, long hi, void *p)
{
    hits_arg *a = (hits_arg *)p;
    for (long i = lo; i < hi; i++) {
        volatile double s = 0;
        for (long k = 0; k <= i; k++) s += (double)k * 1e-9;
        __atomic_fetch_add(&a->hits[i], 1, __ATOMIC_RELAXED);
    }
}

typedef struct {
    long hint;
    int order;
} order_arg;

static int g_order_next;

static void order_task(void *p)
{
    order_arg *a = (order_arg *)p;
    a->order = __atomic_fetch_add(&g_order_next, 1, __ATOMIC_RELAXED);
}

static int lpt_expected(void)
{
    const char *mode = getenv("ABT_WS_SCHEDULER");
    const char *lpt = getenv("WS_LPT");
    if (lpt && strcmp(lpt, "0") == 0) return 0;
    return mode && (strcmp(mode, "old") == 0 || strcmp(mode, "new") == 0);
}

static void sum_body(long lo, long hi, void *partial, void *p)
{
    const double *v = (const double *)p;
    double s = 0;
    for (long i = lo; i < hi; i++) s += v[i];
    *(double *)partial += s;
}

static void sum_combine(void *acc, const void *partial, void *p)
{
    (void)p;
    *(double *)acc += *(const double *)partial;
}

static void isum_body(long lo, long hi, void *partial, void *p)
{
    (void)p;
    long long s = 0;
    for (long i = lo; i < hi; i++) s += i;
    *(long long *)partial += s;
}

static void isum_combine(void *acc, const void *partial, void *p)
{
    (void)p;
    *(long long *)acc += *(const long long *)partial;
}

typedef struct {
    int *hits;
    long lo;
    long hi;
} zone_arg;

static void zone_task(void *p)
{
    zone_arg *z = (zone_arg *)p;
    hits_arg a = { z->hits };
    ws_parallel_for(z->lo, z->hi, mark, &a);
}

static volatile double work_sink;

static void busy(long lo, long hi, void *p)
{
    (void)p;
    double s = 0;
    for (long i = lo; i < hi; i++) {
        for (int k = 0; k < 200; k++) s += (double)(i ^ k) * 1e-9;
    }
    work_sink = s;
}

static void hinted_task(void *p)
{
    long n = *(long *)p;
    ws_parallel_for(0, n, busy, NULL);
}

int main(void)
{
    const long n = 200000;
    int *hits = (int *)calloc(n, sizeof(int));
    double *v = (double *)malloc(n * sizeof(double));
    hits_arg a = { hits };
    double r1, r2, zero = 0.0;
    long long ir, izero = 0;

    ws_init(0);

    for (long len = 0; len < 40; len++) {
        memset(hits, 0, n * sizeof(int));
        ws_parallel_for(0, len, mark, &a);
        if (!all_once(hits, len)) {
            check(0, "parallel_for short ranges");
            break;
        }
        if (len == 39) check(1, "parallel_for short ranges");
    }

    memset(hits, 0, n * sizeof(int));
    ws_parallel_for(0, n, mark, &a);
    check(all_once(hits, n), "parallel_for every index once");

    memset(hits, 0, n * sizeof(int));
    ws_parallel_for_grain(0, n, 7, mark, &a);
    check(all_once(hits, n), "parallel_for_grain every index once");

    memset(hits, 0, n * sizeof(int));
    ws_parallel_for_cost(0, 4000, heavy, &a, triangle, NULL);
    check(all_once(hits, 4000), "parallel_for_cost every index once");

    for (long i = 0; i < n; i++) v[i] = 1.0 / (double)(i + 1);
    ws_parallel_reduce(0, n, sizeof(double), &zero, sum_body, sum_combine, &r1, v);
    ws_parallel_reduce(0, n, sizeof(double), &zero, sum_body, sum_combine, &r2, v);
    check(r1 == r2, "parallel_reduce deterministic");
    ws_parallel_reduce(0, n, sizeof(long long), &izero, isum_body, isum_combine, &ir, NULL);
    check(ir == (long long)n * (n - 1) / 2, "parallel_reduce exact integer sum");

    {
        ws_group *g = ws_group_create();
        zone_arg z[13];
        long step = n / 13;
        memset(hits, 0, n * sizeof(int));
        for (int k = 0; k < 13; k++) {
            z[k].hits = hits;
            z[k].lo = k * step;
            z[k].hi = k == 12 ? n : (k + 1) * step;
            ws_group_spawn(g, zone_task, &z[k], z[k].hi - z[k].lo);
        }
        ws_group_wait(g);
        check(all_once(hits, n), "group of tasks with nested parallel_for");
        for (int round = 0; round < 3; round++) {
            memset(hits, 0, n * sizeof(int));
            for (int k = 0; k < 13; k++) ws_group_spawn(g, zone_task, &z[k], z[k].hi - z[k].lo);
            ws_group_wait(g);
        }
        check(all_once(hits, n), "group reused for several rounds");
        ws_group_free(g);
    }

    {
        static const long hints[8] = {3, 0, 7, 1, 7, 5, 2, 4};
        order_arg o[8];
        ws_group *g = ws_group_create();
        int all = 1, sorted = 1;
        g_order_next = 0;
        for (int k = 0; k < 8; k++) {
            o[k].hint = hints[k];
            o[k].order = -1;
            ws_group_spawn(g, order_task, &o[k], hints[k]);
        }
        ws_group_wait(g);
        ws_group_free(g);
        for (int k = 0; k < 8; k++) {
            if (o[k].order < 0) all = 0;
            for (int m = 0; m < 8; m++) {
                if (o[k].hint > o[m].hint && o[k].order > o[m].order) sorted = 0;
                if (o[k].hint == o[m].hint && k < m && o[k].order > o[m].order) sorted = 0;
            }
        }
        check(all, "group runs every task");
        if (ws_num_workers() == 1 && lpt_expected()) {
            check(sorted, "group starts tasks by decreasing hint (LPT)");
        }
    }

    {
        long sizes[12];
        ws_group *g = ws_group_create();
        double e0, w0, e1, w1, err;
        long long n0, n1, tasks;
        for (int k = 0; k < 12; k++) sizes[k] = 2000L * (k + 1);
        ws_task_cost_accuracy(&e0, &w0, &n0);
        for (int round = 0; round < 12; round++) {
            for (int k = 0; k < 12; k++) ws_group_spawn(g, hinted_task, &sizes[k], sizes[k]);
            ws_group_wait(g);
        }
        ws_group_free(g);
        ws_task_cost_accuracy(&e1, &w1, &n1);
        err = w1 > w0 ? (e1 - e0) / (w1 - w0) : 0.0;
        tasks = n1 - n0;
        printf("estimate error %.3f over %lld tasks\n", err, tasks);
        if (ws_num_workers() > sysconf(_SC_NPROCESSORS_ONLN)) {
            printf("%-48s skipped (more workers than cores)\n", "estimates of nested hinted tasks are accurate");
        } else {
            check(!ws_task_cost_enabled() || (tasks > 0 && err < 0.5),
                  "estimates of nested hinted tasks are accurate");
        }
    }

    ws_finalize();
    free(hits);
    free(v);
    return failures ? 1 : 0;
}
