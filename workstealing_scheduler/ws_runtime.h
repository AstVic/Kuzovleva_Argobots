#pragma once

#include <abt.h>
#include <stddef.h>

#include "ws_task.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Режим планирования выбирается переменной окружения ABT_WS_SCHEDULER:
 *   default    - штатный планировщик Argobots, у каждого ES свой пул, кражи нет;
 *   randws     - встроенная случайная кража Argobots (ABT_SCHED_RANDWS) на пулах ABT_POOL_RANDWS;
 *   randws_fifo - ABT_SCHED_RANDWS на пулах ABT_POOL_FIFO;
 *   old        - baseline work stealing;
 *   new        - cost-aware work stealing (синоним: cost-aware). */
typedef enum {
    WS_MODE_DEFAULT = 0,
    WS_MODE_RANDWS,
    WS_MODE_RANDWS_FIFO,
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

/* Короткое имя программы. */
const char *ws_runtime_prog_name(void);

/* 1, если рантайм инициализирован. */
int ws_runtime_is_active(void);
/* Текущий режим планирования. */
ws_mode_t ws_runtime_mode(void);
/* Число ES. */
int ws_runtime_num_workers(void);

/* Ранг ES вызывающего, если он выполняется внутри рантайма; иначе -1. */
int ws_runtime_self_rank(void);

/* Пул ES rank. */
ABT_pool ws_runtime_pool(int rank);

/* Число готовых к запуску ULT в пуле ES. */
size_t ws_runtime_pool_size(int rank);

/* Создаёт ULT в пуле вызывающего ES (в режиме default - по кругу, так как
 * без кражи задачи из одного пула не попадут на другие ES).
 * est - оценка стоимости, используется только режимом new. */
int ws_runtime_spawn(void (*fn)(void *), void *arg, long long est, ABT_thread *thread);

/* То же, но в режиме new метаданные задачи размещаются в meta, которую
 * вызывающий держит живой до завершения ULT; meta == NULL - как ws_runtime_spawn. */
int ws_runtime_spawn_meta(void (*fn)(void *), void *arg, long long est, ws_task_meta *meta,
                          ABT_thread *thread);

/* Создаёт задачу с метаданными в meta и ключом истории key (функция тела задачи). */
int ws_runtime_spawn_keyed(void (*fn)(void *), void *arg, long long est, const void *key,
                           ws_task_meta *meta, ABT_thread *thread);

/* Создаёт задачу в пуле своего ES с подсказкой стоимости hint (единицы программы). */
int ws_runtime_spawn_hint(void (*fn)(void *), void *arg, long long hint, ws_task_meta *meta,
                          ABT_thread *thread);

/* Кэш пробы размера куска: время на единицу работы цикла с ключом key (телом цикла)
 * на ES вызывающего; 0 - данных нет. WS_GRAIN_CACHE=0 выключает кэш. */
double ws_runtime_rate_get(const void *key);
/* Добавляет замер времени на единицу работы цикла key на ES вызывающего. */
void ws_runtime_rate_put(const void *key, double rate);

/* Число ES по умолчанию: WS_NUM_THREADS или число ядер. */
int ws_runtime_default_workers(void);

/* То же, но в пул заданного ES. */
int ws_runtime_spawn_to(int rank, void (*fn)(void *), void *arg, long long est,
                        ABT_thread *thread);

/* Обнуляет счётчики краж текущего планировщика. */
void ws_runtime_reset_steal_stats(void);
/* Число операций кражи и украденных задач текущего планировщика. */
void ws_runtime_get_steal_stats(long long *steal_operations, long long *stolen_tasks);

#ifdef __cplusplus
}
#endif
