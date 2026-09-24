/* test_cost_binding.c — проверка инварианта «ULT ↔ оценка стоимости».
 *
 * Запускает НАСТОЯЩИЙ cost-aware планировщик из диплома и проверяет, что
 * задача, которую планировщик взял из пула, получила именно свою оценку.
 * Для этого оценкой служит номер задачи: тогда несовпадение считается точно
 * (отладочные счётчики в планировщике, сборка с -DWS_DEBUG_COST_CHECK).
 *
 * Схема нагрузки повторяет приложения диплома:
 *   producers=1 — как jac3d.c: один ULT создаёт фазу задач и ждёт её join'ом;
 *   producers>1 — как jac3d_multi_runtime.c: несколько top-level ULT кладут
 *                 задачи в общие пулы одновременно;
 *   --barrier   — задачи дополнительно синхронизируются на ABT_barrier, как
 *                 ULT древовидной редукции: они блокируются и возвращаются
 *                 в пул мимо учёта.
 *
 * Использование: ./test_cost_binding <xstreams> <producers> <tasks> [--barrier]
 * Код возврата: 0 — инвариант держится и метаданные пулов сошлись в ноль. */
#include <abt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdlib.h>

#include "../../workstealing_scheduler/abt_workstealing_scheduler_cost_aware.h"
#include "../../workstealing_scheduler/ws_task.h"

#define MAX_XSTREAMS 16
#define MAX_PRODUCERS 8

static int g_xstreams = 2;
static int g_producers = 4;
static int g_tasks = 2000;
static int g_phases = 10;
static int g_use_barrier = 0;

static ABT_pool g_pools[MAX_XSTREAMS];
static ABT_barrier g_barrier = ABT_BARRIER_NULL;

typedef struct {
    long long id;
    int uses_barrier;
} task_arg_t;

static task_arg_t *g_args;
static ABT_thread *g_threads;

/* Тело задачи: немного неоднородной работы, опционально — барьер. */
static void task_body(void *arg)
{
    task_arg_t *t = (task_arg_t *)arg;
    volatile double acc = 0.0;
    long long iters = 2000 + (t->id % 7) * 3000;

    for (long long i = 0; i < iters; i++) {
        acc += (double)i;
    }
    if (t->uses_barrier) {
        ABT_barrier_wait(g_barrier);
    }
}

/* Top-level ULT: по фазам создаёт свои задачи и ждёт их, как это делают
 * jac3d.c и jac3d_multi_runtime.c. */
static void producer(void *arg)
{
    long long p = (long long)(size_t)arg;
    int per_producer = g_tasks / g_producers;
    int per_phase = per_producer / g_phases;

    for (int phase = 0; phase < g_phases; phase++) {
        int base = (int)p * per_producer + phase * per_phase;

        for (int j = 0; j < per_phase; j++) {
            int i = base + j;
            int pool_id = (int)((p + j) % g_xstreams);
            g_args[i].id = i;
            g_args[i].uses_barrier = 0;
            /* оценка стоимости = номер задачи: так видно, чья она */
            ws_thread_create(g_pools[pool_id], pool_id, task_body, &g_args[i],
                             (long long)i + 1, &g_threads[i]);
        }
        for (int j = 0; j < per_phase; j++) {
            ABT_thread_free(&g_threads[base + j]);
        }
    }
}

/* Отдельная фаза с барьером: задачи блокируются и просыпаются в пулах. */
static void run_barrier_phase(int num)
{
    ABT_thread *threads = (ABT_thread *)calloc((size_t)num, sizeof(ABT_thread));
    task_arg_t *args = (task_arg_t *)calloc((size_t)num, sizeof(task_arg_t));

    ABT_barrier_create((uint32_t)num, &g_barrier);
    for (int i = 0; i < num; i++) {
        args[i].id = 100000 + i;
        args[i].uses_barrier = 1;
        ws_thread_create(g_pools[i % g_xstreams], i % g_xstreams, task_body,
                         &args[i], (long long)args[i].id, &threads[i]);
    }
    for (int i = 0; i < num; i++) {
        ABT_thread_free(&threads[i]);
    }
    ABT_barrier_free(&g_barrier);
    free(threads);
    free(args);
}

int main(int argc, char **argv)
{
    if (argc > 1) g_xstreams = atoi(argv[1]);
    if (argc > 2) g_producers = atoi(argv[2]);
    if (argc > 3) g_tasks = atoi(argv[3]);
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--barrier") == 0) g_use_barrier = 1;
    }
    if (g_xstreams < 1 || g_xstreams > MAX_XSTREAMS) g_xstreams = 2;
    if (g_producers < 1 || g_producers > MAX_PRODUCERS) g_producers = 1;
    g_tasks = (g_tasks / (g_producers * g_phases)) * g_producers * g_phases;
    if (g_tasks < g_producers * g_phases) g_tasks = g_producers * g_phases;

    g_args = (task_arg_t *)calloc((size_t)g_tasks, sizeof(task_arg_t));
    g_threads = (ABT_thread *)calloc((size_t)g_tasks, sizeof(ABT_thread));

    ABT_init(argc, argv);

    ABT_xstream xstreams[MAX_XSTREAMS];
    ABT_sched scheds[MAX_XSTREAMS];
    for (int i = 0; i < g_xstreams; i++) {
        ABT_pool_create_basic(ABT_POOL_FIFO, ABT_POOL_ACCESS_MPMC, ABT_TRUE, &g_pools[i]);
    }
    ABT_create_ws_scheds_cost_aware(g_xstreams, g_pools, scheds);
    ABT_xstream_self(&xstreams[0]);
    ABT_xstream_set_main_sched(xstreams[0], scheds[0]);
    for (int i = 1; i < g_xstreams; i++) {
        ABT_xstream_create(scheds[i], &xstreams[i]);
    }
    ws_debug_reset();

    ABT_thread producers[MAX_PRODUCERS];
    for (long long p = 0; p < g_producers; p++) {
        ABT_thread_create(g_pools[p % g_xstreams], producer, (void *)(size_t)p,
                          ABT_THREAD_ATTR_NULL, &producers[p]);
    }
    for (int p = 0; p < g_producers; p++) {
        ABT_thread_free(&producers[p]);
    }
    if (g_use_barrier) {
        run_barrier_phase(g_xstreams * 4);
    }

    long long dispatched = ws_debug_dispatched();
    long long wrong = ws_debug_wrong_estimate();
    long long missing = ws_debug_missing_estimate();
    long long foreign = ws_debug_foreign_with_estimate();

    /* Дрейф проверяем по КАЖДОМУ пулу: если один ушёл в плюс, а другой в
     * минус на ту же величину, сумма обманчиво сходится в ноль. */
    long long queued_est = 0, queued_cnt = 0, worst_pool_cnt = 0, worst_pool_est = 0;
    for (int i = 0; i < g_xstreams; i++) {
        long long est_i = ws_get_pool_estimated_load(i);
        long long cnt_i = ws_get_pool_queued_count(i);
        printf("pool %-2d             : queued_count=%lld queued_estimated=%lld\n",
               i, cnt_i, est_i);
        queued_est += est_i;
        queued_cnt += cnt_i;
        if (llabs(cnt_i) > llabs(worst_pool_cnt)) worst_pool_cnt = cnt_i;
        if (llabs(est_i) > llabs(worst_pool_est)) worst_pool_est = est_i;
    }

    printf("configuration       : xstreams=%d producers=%d tasks=%d barrier=%s\n",
           g_xstreams, g_producers, g_tasks, g_use_barrier ? "yes" : "no");
#ifdef WS_LEGACY_EST_FIFO
    printf("cost binding        : legacy (separate FIFO of estimates)\n");
#else
    printf("cost binding        : new (estimate stored in the ULT)\n");
#endif
    printf("ULT dispatches      : %lld\n", dispatched);
    printf("wrong estimate      : %lld\n", wrong);
    printf("missing estimate    : %lld\n", missing);
    printf("foreign took estim. : %lld\n", foreign);
    printf("queued_count left   : %lld (худший пул: %lld)\n", queued_cnt, worst_pool_cnt);
    printf("queued_estimated    : %lld (худший пул: %lld)\n", queued_est, worst_pool_est);
    printf("steal operations    : %lld\n", ws_get_steal_ops_count());
    printf("stolen tasks        : %lld\n", ws_get_stolen_tasks_count());

    int failed = (wrong != 0) || (missing != 0) || (foreign != 0) ||
                 (queued_cnt != 0) || (queued_est != 0) ||
                 (worst_pool_cnt != 0) || (worst_pool_est != 0);
    printf("RESULT              : %s\n", failed ? "FAILED" : "PASSED");

    for (int i = 1; i < g_xstreams; i++) {
        ABT_xstream_join(xstreams[i]);
        ABT_xstream_free(&xstreams[i]);
    }
    ABT_finalize();
    free(g_args);
    free(g_threads);
    return failed ? 1 : 0;
}
