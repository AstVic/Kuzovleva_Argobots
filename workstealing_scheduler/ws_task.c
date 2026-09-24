#include "ws_task.h"
#include "abt_workstealing_scheduler_cost_aware.h"

#include <stdlib.h>

/* Переходник: разворачивает метаданные и вызывает настоящую функцию задачи.
 * Метаданные живут до конца задачи — планировщик читает оценку до запуска. */
static void ws_trampoline(void *p)
{
    ws_task_meta *meta = (ws_task_meta *)p;
    meta->fn(meta->arg);
    free(meta);
}

int ws_thread_create(ABT_pool pool, int pool_rank, void (*fn)(void *), void *arg,
                     long long est, ABT_thread *newthread)
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
    meta->est = (est > 0) ? est : 0;
    meta->dispatched = 0;

    /* Учитываем задачу в очереди ДО создания ULT: иначе планировщик может
     * забрать её раньше, чем она попадёт в счётчики, и списание уйдёт в минус. */
    ws_account_task_created(pool_rank, meta->est);

    ret = ABT_thread_create(pool, ws_trampoline, meta, ABT_THREAD_ATTR_NULL, newthread);
    if (ret != ABT_SUCCESS) {
        ws_account_task_dispatched(pool_rank, meta->est);
        free(meta);
    }
    return ret;
}

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
