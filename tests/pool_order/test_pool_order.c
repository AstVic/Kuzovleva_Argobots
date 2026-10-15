#include <abt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ws_runtime.h"

#define NTASKS 6

static int order[NTASKS];
static int pos;

static void record(void *arg)
{
    order[pos++] = (int)(long)arg;
}

static int same(const int *a, const int *b, int n)
{
    return memcmp(a, b, sizeof(int) * (size_t)n) == 0;
}

static void print_order(const char *name, const int *a, int n)
{
    printf("%-28s", name);
    for (int i = 0; i < n; i++) printf(" %d", a[i]);
    printf("\n");
}

static int run_pool(ABT_pool_kind kind, const ABT_pool_context *ctx, int n)
{
    ABT_pool pool;
    pos = 0;
    ABT_pool_create_basic(kind, ABT_POOL_ACCESS_MPMC, ABT_FALSE, &pool);
    for (long i = 0; i < n; i++) {
        ABT_thread_create(pool, record, (void *)i, ABT_THREAD_ATTR_NULL, NULL);
    }
    for (int i = 0; i < n; i++) {
        ABT_thread thread = ABT_THREAD_NULL;
        ABT_pool_pop_thread_ex(pool, &thread, ctx[i]);
        if (thread == ABT_THREAD_NULL) return 0;
        ABT_self_schedule(thread, ABT_POOL_NULL);
    }
    ABT_pool_free(&pool);
    return pos == n;
}

static int test_pools(void)
{
    const ABT_pool_context P = ABT_POOL_CONTEXT_OWNER_PRIMARY;
    const ABT_pool_context S = ABT_POOL_CONTEXT_OWNER_SECONDARY;
    const ABT_pool_context ctx[4] = {P, S, P, S};
    const int deque_expected[4] = {3, 0, 2, 1};
    const int fifo_expected[4] = {0, 1, 2, 3};
    int ok = 1;

    ABT_init(0, NULL);

    ok &= run_pool(ABT_POOL_RANDWS, ctx, 4) && same(order, deque_expected, 4);
    print_order("pool randws owner/thief:", order, 4);
    ok &= run_pool(ABT_POOL_FIFO, ctx, 4) && same(order, fifo_expected, 4);
    print_order("pool fifo owner/thief:", order, 4);

    ABT_finalize();
    return ok;
}

static int test_runtime(int lifo)
{
    int expected[NTASKS];
    ABT_thread threads[NTASKS];
    int ok;

    ws_runtime_init(1, 0);
    pos = 0;
    for (long i = 0; i < NTASKS; i++) {
        ws_runtime_spawn(record, (void *)i, 1, &threads[i]);
    }
    for (int i = 0; i < NTASKS; i++) {
        ABT_thread_free(&threads[i]);
    }
    ws_runtime_finalize();

    for (int i = 0; i < NTASKS; i++) expected[i] = lifo ? NTASKS - 1 - i : i;
    ok = (pos == NTASKS) && same(order, expected, NTASKS);
    print_order("runtime owner order:", order, NTASKS);
    return ok;
}

int main(int argc, char **argv)
{
    int ok;
    if (argc > 1 && strcmp(argv[1], "pools") == 0) {
        ok = test_pools();
    } else {
        ok = test_runtime(argc > 1 && strcmp(argv[1], "lifo") == 0);
    }
    printf("RESULT: %s\n", ok ? "PASSED" : "FAILED");
    return ok ? 0 : 1;
}
