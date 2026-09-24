#pragma once

#include <abt.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Режим планирования выбирается переменной окружения ABT_WS_SCHEDULER:
 *   default    - штатный планировщик Argobots, у каждого ES свой пул, кражи нет;
 *   randws     - встроенная случайная кража Argobots (ABT_SCHED_RANDWS);
 *   old        - baseline work stealing;
 *   new        - cost-aware work stealing (синоним: cost-aware). */
typedef enum {
    WS_MODE_DEFAULT = 0,
    WS_MODE_RANDWS,
    WS_MODE_OLD,
    WS_MODE_NEW
} ws_mode_t;

/* Инициализирует Argobots и создаёт num_xstreams execution streams.
 * Вызывающий поток становится primary ULT на ES с рангом 0.
 * Если ABT_THREAD_STACKSIZE не задан, стек ULT равен default_stack_size байт
 * (0 - оставить значение Argobots). Повторный вызов ничего не делает. */
int ws_runtime_init(int num_xstreams, size_t default_stack_size);

/* Останавливает ES и завершает Argobots. Вызывается с primary ULT. */
void ws_runtime_finalize(void);

int ws_runtime_is_active(void);
ws_mode_t ws_runtime_mode(void);
int ws_runtime_num_workers(void);

/* Ранг ES вызывающего, если он выполняется внутри рантайма; иначе -1. */
int ws_runtime_self_rank(void);

ABT_pool ws_runtime_pool(int rank);

/* Число готовых к запуску ULT в пуле ES. */
size_t ws_runtime_pool_size(int rank);

/* Создаёт ULT в пуле вызывающего ES (в режиме default - по кругу, так как
 * без кражи задачи из одного пула не попадут на другие ES).
 * est - оценка стоимости, используется только режимом new. */
int ws_runtime_spawn(void (*fn)(void *), void *arg, long long est, ABT_thread *thread);

/* То же, но в пул заданного ES. */
int ws_runtime_spawn_to(int rank, void (*fn)(void *), void *arg, long long est,
                        ABT_thread *thread);

void ws_runtime_reset_steal_stats(void);
void ws_runtime_get_steal_stats(long long *steal_operations, long long *stolen_tasks);

#ifdef __cplusplus
}
#endif
