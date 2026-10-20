#include "ws_parallel.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

    ws_finalize();
    free(hits);
    free(v);
    return failures ? 1 : 0;
}
