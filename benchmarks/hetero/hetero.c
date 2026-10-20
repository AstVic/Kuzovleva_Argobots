/* Синтетическая нагрузка из независимых задач разной стоимости.
 *
 * Каждый раунд - группа из HETERO_TASKS задач; стоимость задачи (число итераций)
 * распределена логарифмически равномерно в [1, HETERO_SPREAD] единиц по
 * HETERO_UNIT итераций и идёт в случайном порядке. Программа передаёт рантайму
 * подсказку - стоимость, искажённую на случайную величину до ±HETERO_NOISE
 * (0.5 = ±50 %); при HETERO_NOISE=0 подсказка точная.
 *
 * Варианты сборки: последовательный (по умолчанию), OpenMP (-fopenmp,
 * schedule(dynamic,1) в порядке создания) и рантайм (-DHETERO_WS, ws_group).
 * Результат каждой задачи сравнивается с последовательным пересчётом. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <math.h>

#ifdef HETERO_WS
#include "ws_parallel.h"
#endif
#ifdef _OPENMP
#include <omp.h>
#endif

typedef struct {
    long long iters;
    long long hint;
    double result;
} task_t;

static long env_long(const char *name, long def)
{
    const char *v = getenv(name);
    return v && v[0] ? atol(v) : def;
}

static double env_double(const char *name, double def)
{
    const char *v = getenv(name);
    return v && v[0] ? atof(v) : def;
}

static unsigned long long g_rng = 88172645463325252ULL;

static double rnd(void)
{
    g_rng ^= g_rng << 13;
    g_rng ^= g_rng >> 7;
    g_rng ^= g_rng << 17;
    return (double)(g_rng >> 11) / 9007199254740992.0;
}

static long long now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

static double work(long long iters, long long seed)
{
    double x = 1.0 + (double)(seed % 97) * 1e-3;
    for (long long i = 0; i < iters; i++) x = x * 0.999999 + 1e-6;
    return x;
}

static void run_task(void *p)
{
    task_t *t = (task_t *)p;
    t->result = work(t->iters, t->iters);
}

int main(void)
{
    long tasks = env_long("HETERO_TASKS", 64);
    long rounds = env_long("HETERO_ROUNDS", 20);
    double spread = env_double("HETERO_SPREAD", 100.0);
    double noise = env_double("HETERO_NOISE", 0.0);
    long unit = env_long("HETERO_UNIT", 50000);
    task_t *t = (task_t *)malloc((size_t)(tasks * rounds) * sizeof(task_t));
    long long start, elapsed;
    long bad = 0;
    double total = 0.0;

    g_rng ^= (unsigned long long)env_long("HETERO_SEED", 1) * 0x9E3779B97F4A7C15ULL;
    if (!t || tasks <= 0 || rounds <= 0 || spread < 1.0) return 1;
    for (long i = 0; i < tasks * rounds; i++) {
        double c = exp(rnd() * log(spread));
        double h;
        t[i].iters = (long long)(c * (double)unit);
        if (t[i].iters < 1) t[i].iters = 1;
        h = (double)t[i].iters * (1.0 + noise * (2.0 * rnd() - 1.0));
        t[i].hint = h >= 1.0 ? (long long)h : 1;
        t[i].result = 0.0;
        total += (double)t[i].iters;
    }

#ifdef HETERO_WS
    ws_init(0);
#endif
    start = now_ns();
    for (long r = 0; r < rounds; r++) {
        task_t *g = t + r * tasks;
#if defined(HETERO_WS)
        ws_group *group = ws_group_create();
        for (long k = 0; k < tasks; k++) ws_group_spawn(group, run_task, &g[k], g[k].hint);
        ws_group_free(group);
#elif defined(_OPENMP)
#pragma omp parallel for schedule(dynamic, 1)
        for (long k = 0; k < tasks; k++) run_task(&g[k]);
#else
        for (long k = 0; k < tasks; k++) run_task(&g[k]);
#endif
    }
    elapsed = now_ns() - start;
#ifdef HETERO_WS
    ws_finalize();
#endif

    for (long i = 0; i < tasks * rounds; i++) {
        if (t[i].result != work(t[i].iters, t[i].iters)) bad++;
    }
    printf(" Tasks per round = %ld, rounds = %ld, spread = %g, noise = %g, unit = %ld\n", tasks,
           rounds, spread, noise, unit);
    printf(" Total iterations = %.0f\n", total);
    printf(" Real time (nanos) = %lld\n", elapsed);
    printf(" Verification %s\n", bad == 0 ? "Successful" : "FAILED");
    free(t);
    return bad == 0 ? 0 : 1;
}
