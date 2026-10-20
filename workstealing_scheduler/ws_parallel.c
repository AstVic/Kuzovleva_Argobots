#include "ws_parallel.h"
#include "ws_runtime.h"
#include "ws_task.h"

#include <abt.h>
#include <limits.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* Стек ULT: рекурсивное деление и вложенные циклы не помещаются в 16 КБ по умолчанию. */
#define WS_STACK_SIZE (1u << 20)

/* Общие параметры одного параллельного цикла. */
typedef struct {
    ws_range_body body;     /* тело цикла */
    const void *key;        /* ключ истории стоимости: функция тела пользователя */
    void *arg;              /* его аргумент */
    ws_range_cost cost;     /* стоимость диапазона или NULL - деление по числу итераций */
    void *cost_arg;
    long grain;             /* размер листа в итерациях */
    double grain_cost;      /* размер листа в единицах стоимости */
    double unit;            /* нс на единицу работы; 0 - оценку назначит рантайм, < 0 - WS_COST=iters */
} ws_pf_ctx;

/* Аргумент задачи для правой половины диапазона. */
typedef struct {
    const ws_pf_ctx *ctx;
    long lo;
    long hi;
} ws_pf_range;

/* Группа задач: созданные ULT, которые ещё нужно дождаться. */
typedef struct {
    void (*fn)(void *);
    void *arg;
    long long hint;
    size_t seq;
    ABT_thread thread;
} ws_group_item;

struct ws_group {
    ws_group_item *items;
    size_t count;
    size_t capacity;
};

/* Измеренная цена развилки в нс; 0 - ещё не измерена. */
static atomic_long g_fork_ns;

/* Монотонное время в наносекундах. */
static long ws_now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long)ts.tv_sec * 1000000000L + ts.tv_nsec;
}

/* Пустая задача для замера цены развилки. */
static void ws_noop(void *arg)
{
    (void)arg;
}

/* 1 для моих планировщиков old и new: только для них включаются проба,
 * деление по стоимости и лимит очереди P; чужие работают в исходной конфигурации. */
static int ws_tuned(void)
{
    ws_mode_t mode = ws_runtime_mode();
    return mode == WS_MODE_OLD || mode == WS_MODE_NEW;
}

/* Положительное число из переменной окружения или def. */
static long ws_env_long(const char *name, long def)
{
    const char *env = getenv(name);
    long v;
    if (!env || env[0] == '\0') return def;
    v = atol(env);
    return v > 0 ? v : def;
}

/* Цена одной развилки: 3 раза по 64 создания и ожидания пустой задачи, минимум среднего. */
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

/* Целевое время порции пробы: WS_PROBE_FACTOR (10) цен развилки; цена меряется один раз. */
static long ws_target_ns(void)
{
    long fork = atomic_load_explicit(&g_fork_ns, memory_order_relaxed);
    if (fork == 0) {
        fork = ws_measure_fork_ns();
        atomic_store_explicit(&g_fork_ns, fork, memory_order_relaxed);
    }
    return ws_env_long("WS_PROBE_FACTOR", 10) * fork;
}

/* Нужна ли проба размера куска: WS_GRAIN=probe|fixed, по умолчанию только для old и new. */
static int ws_probe_enabled(void)
{
    const char *env = getenv("WS_GRAIN");
    if (env && strcmp(env, "fixed") == 0) return 0;
    if (env && strcmp(env, "probe") == 0) return 1;
    return ws_tuned();
}

/* При стольких задачах в своём пуле развилка выполняется на месте: P для old и new, 8 для остальных. */
static size_t ws_queue_limit(void)
{
    long def = ws_tuned() ? ws_runtime_num_workers() : 8;
    return (size_t)ws_env_long("WS_QUEUE_LIMIT", def > 0 ? def : 1);
}

/* Можно ли создать задачу: вызывающий внутри рантайма и его пул не переполнен. */
static int ws_can_spawn(void)
{
    int rank = ws_runtime_self_rank();
    return rank >= 0 && ws_runtime_pool_size(rank) < ws_queue_limit();
}

/* Лист деления: один элемент или кусок не больше размера куска (в итерациях или в стоимости). */
static int ws_pf_leaf(const ws_pf_ctx *c, long lo, long hi)
{
    if (hi - lo <= 1) return 1;
    if (c->cost) return c->cost(lo, hi, c->cost_arg) <= c->grain_cost;
    return hi - lo <= c->grain;
}

/* Точка деления: середина по числу итераций или двоичным поиском - середина по стоимости. */
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

/* Оценка задачи для [lo, hi): работа × время единицы работы (нс), число итераций при
 * WS_COST=iters или 0, если оценку должен назначить рантайм. */
static long long ws_pf_estimate(const ws_pf_ctx *c, long lo, long hi)
{
    double work = c->cost ? c->cost(lo, hi, c->cost_arg) : (double)(hi - lo);
    long long est;
    if (c->unit == 0.0) return 0;
    est = (long long)(c->unit > 0 ? work * c->unit : work);
    return est > 0 ? est : 1;
}

static void ws_pf_run(const ws_pf_ctx *c, long lo, long hi);

/* Задача для правой половины диапазона. */
static void ws_pf_task(void *p)
{
    ws_pf_range *r = (ws_pf_range *)p;
    ws_pf_run(r->ctx, r->lo, r->hi);
}

/* Рекурсивное деление: правая половина - новая задача, левая - на месте, затем ожидание;
 * если задачу создать нельзя, обе половины выполняются на месте. */
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
        ws_runtime_spawn_keyed(ws_pf_task, &right, ws_pf_estimate(c, mid, hi), c->key,
                               &meta, &thread) !=
            ABT_SUCCESS) {
        ws_pf_run(c, lo, mid);
        ws_pf_run(c, mid, hi);
        return;
    }
    ws_pf_run(c, lo, mid);
    ws_task_join(&thread);
}

/* Запускает рантайм на num_workers ES (0 - WS_NUM_THREADS или число ядер), стек ULT 1 МБ. */
int ws_init(int num_workers)
{
    return ws_runtime_init(num_workers > 0 ? num_workers : ws_runtime_default_workers(),
                           WS_STACK_SIZE);
}

/* Останавливает рантайм и сбрасывает измеренную цену развилки. */
void ws_finalize(void)
{
    ws_runtime_finalize();
    atomic_store_explicit(&g_fork_ns, 0, memory_order_relaxed);
}

/* Число ES или 1 вне рантайма. */
int ws_num_workers(void)
{
    return ws_runtime_is_active() ? ws_runtime_num_workers() : 1;
}

/* Параллельный цикл: body вызывается на непересекающихся диапазонах, покрывающих [lo, hi).
 * Для old и new сначала проба: 1, 2, 4... итерации на месте до целевого времени, затем деление
 * остатка; если задана cost, деление и оценки идут по стоимости диапазона, а не по числу итераций.
 * Для чужих планировщиков - кусок n/(8P) по числу итераций. */
/* Порция пробы: выполняет итерации [lo, hi) в вызывающей задаче, пишет в ns её работу
 * (без ожидания вложенных задач) и возвращает время на единицу стоимости или 0. */
static double ws_probe_batch(const ws_pf_ctx *c, long lo, long hi, long long *ns)
{
    long long start = ws_task_work_clock();
    double work;
    c->body(lo, hi, c->arg);
    *ns = ws_task_work_clock() - start;
    work = c->cost ? c->cost(lo, hi, c->cost_arg) : (double)(hi - lo);
    return work > 0 ? (double)*ns / work : 0.0;
}

static void ws_parallel_for_keyed(long lo, long hi, ws_range_body body, void *arg,
                                  ws_range_cost cost, void *cost_arg, const void *key);

void ws_parallel_for_cost(long lo, long hi, ws_range_body body, void *arg, ws_range_cost cost,
                          void *cost_arg)
{
    ws_parallel_for_keyed(lo, hi, body, arg, cost, cost_arg, (const void *)body);
}

/* Параллельный цикл с явным ключом истории: им пользуются обёртки вроде редукции, чтобы
 * разные пользовательские тела не смешивались в одной записи. */
static void ws_parallel_for_keyed(long lo, long hi, ws_range_body body, void *arg,
                                  ws_range_cost cost, void *cost_arg, const void *key)
{
    ws_pf_ctx c;
    long n = hi - lo;
    if (n <= 0) return;
    if (n == 1 || !ws_runtime_is_active() || ws_runtime_self_rank() < 0) {
        body(lo, hi, arg);
        return;
    }
    c.body = body;
    c.key = key;
    c.arg = arg;
    c.cost = ws_tuned() ? cost : NULL;
    c.cost_arg = cost_arg;
    c.unit = ws_task_cost_enabled() ? 0.0 : -1.0;
    if (ws_probe_enabled()) {
        long target = ws_target_ns();
        long done = 0;
        long sz = 1;
        long grain = 0;
        double prev_rate = 0.0;
        while (1) {
            long long ns;
            double rate;
            if (sz > n - done) sz = n - done;
            rate = ws_probe_batch(&c, lo + done, lo + done + sz, &ns);
            done += sz;
            if (done == n) return;
            if (grain == 0 && ns < target) {
                prev_rate = rate;
                sz *= 2;
                continue;
            }
            if (grain == 0) {
                grain = done;
                /* Первая же порция дошла до цели: для оценки нужен второй замер. */
                if (prev_rate <= 0.0) {
                    prev_rate = rate;
                    continue;
                }
            }
            /* Вытеснение потока ОС только удлиняет замер, поэтому из двух последних
             * порций берётся меньшее время на единицу работы. */
            if (prev_rate > 0.0 && (rate <= 0.0 || prev_rate < rate)) rate = prev_rate;
            if (ws_task_cost_enabled() && rate > 0.0) c.unit = rate;
            break;
        }
        c.grain = grain;
        c.grain_cost = c.cost ? c.cost(lo, lo + grain, cost_arg) : 0.0;
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

/* Параллельный цикл без функции стоимости. */
void ws_parallel_for(long lo, long hi, ws_range_body body, void *arg)
{
    ws_parallel_for_cost(lo, hi, body, arg, NULL, NULL);
}

/* Параллельный цикл с явным размером куска, без пробы. */
void ws_parallel_for_grain(long lo, long hi, long grain, ws_range_body body, void *arg)
{
    ws_pf_ctx c;
    if (hi <= lo) return;
    if (!ws_runtime_is_active() || ws_runtime_self_rank() < 0) {
        body(lo, hi, arg);
        return;
    }
    c.body = body;
    c.key = (const void *)body;
    c.arg = arg;
    c.cost = NULL;
    c.cost_arg = NULL;
    c.grain = grain > 0 ? grain : 1;
    c.grain_cost = 0.0;
    c.unit = ws_task_cost_enabled() ? 0.0 : -1.0;
    ws_pf_run(&c, lo, hi);
}

/* Состояние редукции: [lo, lo + n) делится на blocks фиксированных блоков,
 * частичный результат блока b лежит в partials + b * elem_size. */
typedef struct {
    long lo;
    long n;
    long blocks;
    size_t elem_size;
    char *partials;
    ws_reduce_body body;
    void *arg;
} ws_reduce_ctx;

/* Считает частичные результаты для блоков с номерами [blo, bhi). */
static void ws_reduce_blocks(long blo, long bhi, void *p)
{
    ws_reduce_ctx *r = (ws_reduce_ctx *)p;
    for (long b = blo; b < bhi; b++) {
        long lo = r->lo + r->n * b / r->blocks;
        long hi = r->lo + r->n * (b + 1) / r->blocks;
        if (hi > lo) r->body(lo, hi, r->partials + (size_t)b * r->elem_size, r->arg);
    }
}

/* Детерминированная редукция: [lo, hi) делится на 8P фиксированных блоков, частичные
 * результаты считаются параллельно и объединяются по порядку блоков. */
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
    ws_parallel_for_keyed(0, r.blocks, ws_reduce_blocks, &r, NULL, NULL, (const void *)body);
    for (long b = 0; b < r.blocks; b++) {
        combine(result, r.partials + (size_t)b * elem_size, arg);
    }
    free(r.partials);
}

/* Создаёт пустую группу задач. */
ws_group *ws_group_create(void)
{
    return (ws_group *)calloc(1, sizeof(ws_group));
}

/* Порядок LPT: в режимах old и new задачи группы создаются в ws_group_wait по убыванию
 * подсказки, чтобы крупные задачи начинались первыми. WS_LPT=0 выключает. */
static int ws_lpt_enabled(void)
{
    const char *env = getenv("WS_LPT");
    if (env && strcmp(env, "0") == 0) return 0;
    return ws_tuned();
}

/* Сравнение для сортировки: подсказка по убыванию, при равенстве - порядок добавления. */
static int ws_group_item_cmp(const void *a, const void *b)
{
    const ws_group_item *x = (const ws_group_item *)a;
    const ws_group_item *y = (const ws_group_item *)b;
    if (x->hint != y->hint) return x->hint > y->hint ? -1 : 1;
    return x->seq < y->seq ? -1 : (x->seq > y->seq ? 1 : 0);
}

/* Создаёт ULT задачи группы; если не удалось, выполняет задачу сразу. */
static void ws_group_start(ws_group_item *item)
{
    item->thread = ABT_THREAD_NULL;
    if (ws_runtime_spawn_hint(item->fn, item->arg, item->hint, NULL, &item->thread) !=
        ABT_SUCCESS) {
        item->thread = ABT_THREAD_NULL;
        item->fn(item->arg);
    }
}

/* Добавляет задачу fn(arg) в группу с подсказкой стоимости hint (0 - нет подсказки).
 * При порядке LPT задача только запоминается и создаётся в ws_group_wait, иначе
 * создаётся сразу. Вне рантайма или при ошибке задача выполняется сразу. */
int ws_group_spawn(ws_group *group, void (*fn)(void *), void *arg, long long hint)
{
    ws_group_item *item;
    if (!group) return ABT_ERR_INV_ARG;
    if (!ws_runtime_is_active() || ws_runtime_self_rank() < 0) {
        fn(arg);
        return ABT_SUCCESS;
    }
    if (group->count == group->capacity) {
        size_t cap = group->capacity ? group->capacity * 2 : 16;
        ws_group_item *grown = (ws_group_item *)realloc(group->items, cap * sizeof(*grown));
        if (!grown) {
            fn(arg);
            return ABT_SUCCESS;
        }
        group->items = grown;
        group->capacity = cap;
    }
    item = &group->items[group->count];
    item->fn = fn;
    item->arg = arg;
    item->hint = hint > 0 ? hint : 0;
    item->seq = group->count;
    item->thread = ABT_THREAD_NULL;
    group->count++;
    if (!ws_lpt_enabled()) ws_group_start(item);
    return ABT_SUCCESS;
}

/* Ждёт все задачи группы; после возврата группу можно использовать снова. */
void ws_group_wait(ws_group *group)
{
    long long start;
    if (!group || group->count == 0) return;
    if (ws_lpt_enabled()) {
        qsort(group->items, group->count, sizeof(ws_group_item), ws_group_item_cmp);
        for (size_t i = 0; i < group->count; i++) {
            ws_group_start(&group->items[i]);
        }
    }
    start = ws_task_wait_begin();
    for (size_t i = 0; i < group->count; i++) {
        if (group->items[i].thread != ABT_THREAD_NULL) {
            ABT_thread_free(&group->items[i].thread);
        }
    }
    ws_task_wait_end(start);
    group->count = 0;
}

/* Ждёт задачи и освобождает группу. */
void ws_group_free(ws_group *group)
{
    if (!group) return;
    ws_group_wait(group);
    free(group->items);
    free(group);
}
