#pragma once

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Тело цикла: обрабатывает итерации [lo, hi). */
typedef void (*ws_range_body)(long lo, long hi, void *arg);
/* Стоимость итераций [lo, hi) в единицах программы; должна быть аддитивной. */
typedef double (*ws_range_cost)(long lo, long hi, void *arg);
/* Тело редукции: добавляет вклад итераций [lo, hi) в partial. */
typedef void (*ws_reduce_body)(long lo, long hi, void *partial, void *arg);
/* Объединение: acc = acc + partial. */
typedef void (*ws_reduce_combine)(void *acc, const void *partial, void *arg);

/* Запуск рантайма на num_workers ES (0 - WS_NUM_THREADS или число ядер). */
int ws_init(int num_workers);
/* Остановка рантайма. */
void ws_finalize(void);
/* Число ES. */
int ws_num_workers(void);

/* Параллельный цикл по [lo, hi) с автоматическим размером куска. */
void ws_parallel_for(long lo, long hi, ws_range_body body, void *arg);
/* Параллельный цикл с заданным размером куска. */
void ws_parallel_for_grain(long lo, long hi, long grain, ws_range_body body, void *arg);
/* Параллельный цикл с делением и оценками по стоимости итераций. */
void ws_parallel_for_cost(long lo, long hi, ws_range_body body, void *arg, ws_range_cost cost,
                          void *cost_arg);

/* Детерминированная редукция значения размера elem_size с нейтральным элементом identity. */
void ws_parallel_reduce(long lo, long hi, size_t elem_size, const void *identity,
                        ws_reduce_body body, ws_reduce_combine combine, void *result, void *arg);

typedef struct ws_group ws_group;

/* Группа независимых задач: создать, добавить задачи, дождаться всех.
 * Задачи группы не должны ждать друг друга или создателя до ws_group_wait:
 * в режимах old и new они могут начаться только в ws_group_wait (порядок LPT). */
ws_group *ws_group_create(void);
int ws_group_spawn(ws_group *group, void (*fn)(void *), void *arg, long long hint);
void ws_group_wait(ws_group *group);
void ws_group_free(ws_group *group);

#ifdef __cplusplus
}
#endif
