#include "ws_task.h"
#include "abt_workstealing_scheduler_cost_aware.h"

#include <stdint.h>
#include <stdlib.h>
#include <time.h>

/* Размер таблицы истории одного ES (степень двойки) и число проб при поиске. */
#define WS_COST_SLOTS 1024
#define WS_COST_PROBES 8
/* Записи «наносекунд на единицу подсказки» хранятся под этой глубиной,
 * в фиксированной точке с множителем WS_COST_HINT_SCALE. */
#define WS_COST_HINT_DEPTH (-1)
#define WS_COST_HINT_SCALE 1024

/* Запись истории: средняя работа задачи с ключом key на глубине depth. */
typedef struct {
    const void *key;
    int depth;
    long long ns;
} ws_cost_slot;

/* Таблицы истории всех ES подряд (NULL - оценки в наносекундах выключены). */
static ws_cost_slot *g_cost = NULL;
static int g_cost_ranks = 0;
/* Среднее время задач на каждом ES - последняя ступень оценки. */
static long long *g_cost_avg = NULL;

/* Точность оценок на каждом ES: сумма |оценка - работа|, сумма работы и число задач. */
typedef struct {
    double abs_err;
    double work;
    long long count;
} ws_cost_acc;

static ws_cost_acc *g_cost_acc = NULL;
/* 1 - планировщик получает нулевые оценки (WS_COST=off). */
static int g_cost_hidden = 0;

/* Монотонное время в наносекундах. */
static long long ws_now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

/* Номер ES вызывающего для таблицы истории или -1. */
static int ws_cost_rank(void)
{
    int rank = -1;
    if (!g_cost) return -1;
    if (ABT_self_get_xstream_rank(&rank) != ABT_SUCCESS || rank < 0 || rank >= g_cost_ranks) {
        return -1;
    }
    return rank;
}

/* Таблица истории ES rank: у каждого ES своя, поэтому блокировки не нужны. */
static ws_cost_slot *ws_cost_table(int rank)
{
    return rank < 0 ? NULL : g_cost + (size_t)rank * WS_COST_SLOTS;
}

/* Ищет запись по ключу (функция задачи, глубина) открытой адресацией;
 * при insert создаёт запись или вытесняет ячейку при переполнении. */
static ws_cost_slot *ws_cost_find(ws_cost_slot *table, const void *key, int depth, int insert)
{
    uintptr_t h = ((uintptr_t)key >> 4) * 2654435761u + (uintptr_t)depth * 40503u;
    for (int i = 0; i < WS_COST_PROBES; i++) {
        ws_cost_slot *slot = &table[(h + (uintptr_t)i) & (WS_COST_SLOTS - 1)];
        if (slot->key == key && slot->depth == depth) return slot;
        if (!slot->key) {
            if (!insert) return NULL;
            slot->key = key;
            slot->depth = depth;
            slot->ns = 0;
            return slot;
        }
    }
    if (!insert) return NULL;
    ws_cost_slot *slot = &table[h & (WS_COST_SLOTS - 1)];
    slot->key = key;
    slot->depth = depth;
    slot->ns = 0;
    return slot;
}

/* Обновляет историю: среднее с забыванием 1/8 по ключу и среднее по всему ES. */
static void ws_cost_record(const void *key, int depth, long long ns)
{
    int rank = ws_cost_rank();
    ws_cost_slot *table = ws_cost_table(rank);
    ws_cost_slot *slot;
    if (!table) return;
    slot = ws_cost_find(table, key, depth, 1);
    if (ns < 1) ns = 1;
    slot->ns = slot->ns > 0 ? slot->ns + (ns - slot->ns) / 8 : ns;
    g_cost_avg[rank] = g_cost_avg[rank] > 0 ? g_cost_avg[rank] + (ns - g_cost_avg[rank]) / 8 : ns;
}

/* Включает оценки в наносекундах: выделяет таблицы истории на num_ranks ES. */
int ws_task_cost_init(int num_ranks)
{
    ws_task_cost_finalize();
    if (num_ranks <= 0) return ABT_ERR_INV_ARG;
    g_cost = (ws_cost_slot *)calloc((size_t)num_ranks * WS_COST_SLOTS, sizeof(ws_cost_slot));
    g_cost_avg = (long long *)calloc((size_t)num_ranks, sizeof(long long));
    g_cost_acc = (ws_cost_acc *)calloc((size_t)num_ranks, sizeof(ws_cost_acc));
    if (!g_cost || !g_cost_avg || !g_cost_acc) {
        ws_task_cost_finalize();
        return ABT_ERR_MEM;
    }
    g_cost_ranks = num_ranks;
    return ABT_SUCCESS;
}

/* Освобождает таблицы истории и выключает оценки в наносекундах. */
void ws_task_cost_finalize(void)
{
    free(g_cost);
    free(g_cost_avg);
    free(g_cost_acc);
    g_cost = NULL;
    g_cost_avg = NULL;
    g_cost_acc = NULL;
    g_cost_ranks = 0;
}

/* 1, если оценки в наносекундах включены. */
int ws_task_cost_enabled(void)
{
    return g_cost != NULL;
}

/* Скрывает оценки от планировщика, не меняя их расчёт и замер времени. */
void ws_task_cost_set_hidden(int hidden)
{
    g_cost_hidden = hidden;
}

/* Метаданные выполняемой задачи или NULL, если вызывающий не задача рантайма. */
ws_task_meta *ws_task_self_meta(void)
{
    ABT_thread self = ABT_THREAD_NULL;
    if (ABT_self_get_thread(&self) != ABT_SUCCESS) return NULL;
    return ws_task_meta_of(self);
}

/* Поиск в таблице истории своего ES без создания записи. */
static ws_cost_slot *ws_cost_find_self(const void *key, int depth)
{
    ws_cost_slot *table = ws_cost_table(ws_cost_rank());
    return table ? ws_cost_find(table, key, depth, 0) : NULL;
}

/* Оценка из истории: ключ (key, depth), иначе соседняя глубина (×2 или /2),
 * иначе среднее время задач этого ES; 0, если истории ещё нет. */
long long ws_task_cost_lookup(const void *key, int depth)
{
    int rank = ws_cost_rank();
    ws_cost_slot *table = ws_cost_table(rank);
    ws_cost_slot *slot;
    if (!table) return 0;
    slot = ws_cost_find(table, key, depth, 0);
    if (slot) return slot->ns;
    slot = ws_cost_find(table, key, depth - 1, 0);
    if (slot) return slot->ns / 2;
    slot = ws_cost_find(table, key, depth + 1, 0);
    if (slot) return slot->ns * 2;
    return g_cost_avg[rank];
}

/* Назначает оценку новой задаче: явная; иначе подсказка × время на единицу подсказки;
 * иначе половина оценки родителя; иначе история. Оценка родителя уменьшается на отданную часть. */
static void ws_assign_cost(ws_task_meta *meta)
{
    ws_task_meta *parent = ws_task_self_meta();
    meta->parent = parent;
    meta->depth = parent ? parent->depth + 1 : 1;
    if (meta->est <= 0 && meta->hint > 0) {
        ws_cost_slot *slot = ws_cost_find_self(meta->key, WS_COST_HINT_DEPTH);
        if (slot && slot->ns > 0) {
            meta->est = meta->hint * slot->ns / WS_COST_HINT_SCALE;
            if (meta->est < 1) meta->est = 1;
        }
    }
    if (meta->est <= 0) {
        if (parent && parent->est > 1) {
            meta->est = parent->est / 2;
        } else {
            meta->est = ws_task_cost_lookup(meta->key, meta->depth);
        }
    }
    if (parent && meta->est > 0) {
        parent->est = parent->est > meta->est ? parent->est - meta->est : 0;
    }
    meta->est0 = meta->est;
}

/* Работа задачи завершена: собственное время (без ожидания детей) плюс работа всех детей.
 * Пишет её в историю и в калибровку подсказки, учитывает точность исходной оценки
 * и прибавляет работу к родителю, который ещё ждёт эту задачу. */
static void ws_cost_complete(ws_task_meta *meta, long long elapsed)
{
    long long own = elapsed - meta->wait_ns;
    long long work = (own > 0 ? own : 0) + __atomic_load_n(&meta->child_work, __ATOMIC_ACQUIRE);
    int rank = ws_cost_rank();
    ws_cost_record(meta->key, meta->depth, work);
    if (meta->hint > 0) {
        ws_cost_record(meta->key, WS_COST_HINT_DEPTH, work * WS_COST_HINT_SCALE / meta->hint);
    }
    if (rank >= 0 && meta->est0 > 0 && work > 0) {
        double d = (double)meta->est0 - (double)work;
        g_cost_acc[rank].abs_err += d < 0 ? -d : d;
        g_cost_acc[rank].work += (double)work;
        g_cost_acc[rank].count++;
    }
    if (meta->parent) {
        __atomic_fetch_add(&meta->parent->child_work, work, __ATOMIC_RELEASE);
    }
}

/* Ждёт и освобождает ULT; время ожидания вычитается из собственного времени
 * вызывающей задачи, чтобы в её работу не попала чужая работа, выполненная за это время. */
int ws_task_join(ABT_thread *thread)
{
    ws_task_meta *self;
    long long start;
    int ret;
    if (!g_cost) return ABT_thread_free(thread);
    self = ws_task_self_meta();
    start = ws_now_ns();
    ret = ABT_thread_free(thread);
    if (self) self->wait_ns += ws_now_ns() - start;
    return ret;
}

/* Начало ожидания группы задач: время, от которого считается ожидание. */
long long ws_task_wait_begin(void)
{
    return g_cost ? ws_now_ns() : 0;
}

/* Конец ожидания группы задач: время ожидания вычитается из собственного времени задачи. */
void ws_task_wait_end(long long start)
{
    ws_task_meta *self;
    if (!g_cost) return;
    self = ws_task_self_meta();
    if (self) self->wait_ns += ws_now_ns() - start;
}

/* Точность оценок по всем ES: сумма |оценка - работа|, сумма работы и число учтённых задач.
 * Взвешенная ошибка - abs_err / work. */
void ws_task_cost_accuracy(double *abs_err, double *work, long long *count)
{
    double e = 0.0, w = 0.0;
    long long n = 0;
    for (int r = 0; g_cost_acc && r < g_cost_ranks; r++) {
        e += g_cost_acc[r].abs_err;
        w += g_cost_acc[r].work;
        n += g_cost_acc[r].count;
    }
    *abs_err = e;
    *work = w;
    *count = n;
}

/* Переходник: разворачивает метаданные и вызывает настоящую функцию задачи.
 * Метаданные живут до конца задачи — планировщик читает оценку до запуска. */
static void ws_trampoline(void *p)
{
    ws_task_meta *meta = (ws_task_meta *)p;
    int heap = meta->heap;
    long long start = g_cost ? ws_now_ns() : 0;
    meta->fn(meta->arg);
    if (g_cost) {
        ws_cost_complete(meta, ws_now_ns() - start);
    }
    if (heap) {
        free(meta);
    }
}

/* Назначает оценку, учитывает задачу в очереди пула и создаёт ULT с переходником. */
static int ws_spawn_meta(ABT_pool pool, int pool_rank, ws_task_meta *meta,
                         ABT_thread *newthread)
{
    int ret;

    meta->est = (meta->est > 0) ? meta->est : 0;
    meta->dispatched = 0;
    meta->depth = 0;
    meta->parent = NULL;
    meta->wait_ns = 0;
    meta->child_work = 0;
    meta->est0 = 0;
    if (!meta->key) meta->key = (const void *)meta->fn;
    if (meta->hint < 0) meta->hint = 0;
    if (g_cost) {
        ws_assign_cost(meta);
    } else if (meta->est == 0) {
        meta->est = meta->hint;
    }

    /* Учитываем задачу в очереди ДО создания ULT: иначе планировщик может
     * забрать её раньше, чем она попадёт в счётчики, и списание уйдёт в минус. */
    meta->sched_est = g_cost_hidden ? 0 : meta->est;
    ws_account_task_created(pool_rank, meta->sched_est);

    ret = ABT_thread_create(pool, ws_trampoline, meta, ABT_THREAD_ATTR_NULL, newthread);
    if (ret != ABT_SUCCESS) {
        ws_account_task_dispatched(pool_rank, meta->sched_est);
    }
    return ret;
}

/* Создаёт задачу с оценкой est; метаданные в куче. */
int ws_thread_create(ABT_pool pool, int pool_rank, void (*fn)(void *), void *arg,
                     long long est, ABT_thread *newthread)
{
    return ws_thread_create_hint(pool, pool_rank, fn, arg, est, 0, newthread);
}

/* Создаёт задачу с оценкой est и подсказкой hint; метаданные в куче. */
int ws_thread_create_hint(ABT_pool pool, int pool_rank, void (*fn)(void *), void *arg,
                          long long est, long long hint, ABT_thread *newthread)
{
    ws_task_meta *meta;
    int ret;

    if (!fn) {
        return ABT_ERR_INV_ARG;
    }
    meta = (ws_task_meta *)malloc(sizeof(*meta));
    if (!meta) {
        return ABT_ERR_MEM;
    }
    meta->fn = fn;
    meta->arg = arg;
    meta->est = est;
    meta->hint = hint;
    meta->key = NULL;
    meta->heap = 1;
    ret = ws_spawn_meta(pool, pool_rank, meta, newthread);
    if (ret != ABT_SUCCESS) {
        free(meta);
    }
    return ret;
}

/* Создаёт задачу с метаданными в памяти вызывающего. */
int ws_thread_create_with_meta(ABT_pool pool, int pool_rank, ws_task_meta *meta,
                               ABT_thread *newthread)
{
    if (!meta || !meta->fn) {
        return ABT_ERR_INV_ARG;
    }
    meta->heap = 0;
    return ws_spawn_meta(pool, pool_rank, meta, newthread);
}

/* Метаданные ULT, если он создан рантаймом (узнаётся по функции-переходнику), иначе NULL. */
ws_task_meta *ws_task_meta_of(ABT_thread thread)
{
    void (*func)(void *) = NULL;
    void *arg = NULL;

    if (thread == ABT_THREAD_NULL) {
        return NULL;
    }
    /* Сравниваем указатель функции, а не читаем чужую память: у постороннего
     * ULT аргумент может указывать куда угодно. */
    if (ABT_thread_get_thread_func(thread, &func) != ABT_SUCCESS || func != ws_trampoline) {
        return NULL;
    }
    if (ABT_thread_get_arg(thread, &arg) != ABT_SUCCESS) {
        return NULL;
    }
    return (ws_task_meta *)arg;
}
