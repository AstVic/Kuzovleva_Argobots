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

/* Запись истории: среднее время задачи с функцией fn на глубине depth. */
typedef struct {
    void (*fn)(void *);
    int depth;
    long long ns;
} ws_cost_slot;

/* Таблицы истории всех ES подряд (NULL - оценки в наносекундах выключены). */
static ws_cost_slot *g_cost = NULL;
static int g_cost_ranks = 0;
/* Среднее время задач на каждом ES - последняя ступень оценки. */
static long long *g_cost_avg = NULL;

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
static ws_cost_slot *ws_cost_find(ws_cost_slot *table, void (*fn)(void *), int depth, int insert)
{
    uintptr_t h = ((uintptr_t)fn >> 4) * 2654435761u + (uintptr_t)depth * 40503u;
    for (int i = 0; i < WS_COST_PROBES; i++) {
        ws_cost_slot *slot = &table[(h + (uintptr_t)i) & (WS_COST_SLOTS - 1)];
        if (slot->fn == fn && slot->depth == depth) return slot;
        if (!slot->fn) {
            if (!insert) return NULL;
            slot->fn = fn;
            slot->depth = depth;
            slot->ns = 0;
            return slot;
        }
    }
    if (!insert) return NULL;
    ws_cost_slot *slot = &table[h & (WS_COST_SLOTS - 1)];
    slot->fn = fn;
    slot->depth = depth;
    slot->ns = 0;
    return slot;
}

/* Обновляет историю: среднее с забыванием 1/8 по ключу и среднее по всему ES. */
static void ws_cost_record(void (*fn)(void *), int depth, long long ns)
{
    int rank = ws_cost_rank();
    ws_cost_slot *table = ws_cost_table(rank);
    ws_cost_slot *slot;
    if (!table) return;
    slot = ws_cost_find(table, fn, depth, 1);
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
    if (!g_cost || !g_cost_avg) {
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
    g_cost = NULL;
    g_cost_avg = NULL;
    g_cost_ranks = 0;
}

/* 1, если оценки в наносекундах включены. */
int ws_task_cost_enabled(void)
{
    return g_cost != NULL;
}

/* Метаданные выполняемой задачи или NULL, если вызывающий не задача рантайма. */
ws_task_meta *ws_task_self_meta(void)
{
    ABT_thread self = ABT_THREAD_NULL;
    if (ABT_self_get_thread(&self) != ABT_SUCCESS) return NULL;
    return ws_task_meta_of(self);
}

/* Поиск в таблице истории своего ES без создания записи. */
static ws_cost_slot *ws_cost_find_self(void (*fn)(void *), int depth)
{
    ws_cost_slot *table = ws_cost_table(ws_cost_rank());
    return table ? ws_cost_find(table, fn, depth, 0) : NULL;
}

/* Оценка из истории: ключ (fn, depth), иначе соседняя глубина (×2 или /2),
 * иначе среднее время задач этого ES; 0, если истории ещё нет. */
long long ws_task_cost_lookup(void (*fn)(void *), int depth)
{
    int rank = ws_cost_rank();
    ws_cost_slot *table = ws_cost_table(rank);
    ws_cost_slot *slot;
    if (!table) return 0;
    slot = ws_cost_find(table, fn, depth, 0);
    if (slot) return slot->ns;
    slot = ws_cost_find(table, fn, depth - 1, 0);
    if (slot) return slot->ns / 2;
    slot = ws_cost_find(table, fn, depth + 1, 0);
    if (slot) return slot->ns * 2;
    return g_cost_avg[rank];
}

/* Назначает оценку новой задаче: явная; иначе подсказка × время на единицу подсказки;
 * иначе половина оценки родителя; иначе история. Оценка родителя уменьшается на отданную часть. */
static void ws_assign_cost(ws_task_meta *meta)
{
    ws_task_meta *parent = ws_task_self_meta();
    meta->depth = parent ? parent->depth + 1 : 1;
    if (meta->est <= 0 && meta->hint > 0) {
        ws_cost_slot *slot = ws_cost_find_self(meta->fn, WS_COST_HINT_DEPTH);
        if (slot && slot->ns > 0) {
            meta->est = meta->hint * slot->ns / WS_COST_HINT_SCALE;
            if (meta->est < 1) meta->est = 1;
        }
    }
    if (meta->est <= 0) {
        if (parent && parent->est > 1) {
            meta->est = parent->est / 2;
        } else {
            meta->est = ws_task_cost_lookup(meta->fn, meta->depth);
        }
    }
    if (parent && meta->est > 0) {
        parent->est = parent->est > meta->est ? parent->est - meta->est : 0;
    }
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
        long long ns = ws_now_ns() - start;
        ws_cost_record(meta->fn, meta->depth, ns);
        if (meta->hint > 0) {
            ws_cost_record(meta->fn, WS_COST_HINT_DEPTH, ns * WS_COST_HINT_SCALE / meta->hint);
        }
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
    if (meta->hint < 0) meta->hint = 0;
    if (g_cost) {
        ws_assign_cost(meta);
    } else if (meta->est == 0) {
        meta->est = meta->hint;
    }

    /* Учитываем задачу в очереди ДО создания ULT: иначе планировщик может
     * забрать её раньше, чем она попадёт в счётчики, и списание уйдёт в минус. */
    ws_account_task_created(pool_rank, meta->est);

    ret = ABT_thread_create(pool, ws_trampoline, meta, ABT_THREAD_ATTR_NULL, newthread);
    if (ret != ABT_SUCCESS) {
        ws_account_task_dispatched(pool_rank, meta->est);
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
