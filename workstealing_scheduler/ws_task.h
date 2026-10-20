#pragma once
#include <abt.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Метаданные задачи runtime.
 *
 * Оценка стоимости хранится вместе с самим ULT, а не в отдельной очереди пула:
 * при нескольких производителях и при пробуждении заблокированных ULT две
 * независимые очереди (пул Argobots и FIFO оценок) расходятся, и задача
 * получает чужую оценку.
 *
 * Механизм: ULT создаётся не с функцией задачи, а с переходником
 * ws_trampoline, которому передаётся эта структура. Планировщик узнаёт
 * "свой" ULT по указателю функции (ABT_thread_get_thread_func) и читает
 * оценку через ABT_thread_get_arg — без блокировок и без аллокаций. */
typedef struct ws_task_meta {
    void (*fn)(void *);   /* настоящая функция задачи */
    void *arg;            /* её аргумент */
    long long est;        /* оценка стоимости задачи */
    int dispatched;       /* 1 = задача уже запускалась, из очереди списана */
    int heap;             /* 1 = выделена ws_thread_create, освобождается после задачи */
    int depth;            /* глубина вложенности: 1 у задач главного потока */
    long long hint;       /* подсказка стоимости в единицах программы, 0 - нет */
} ws_task_meta;

/* Создаёт ULT с привязанной оценкой стоимости и учитывает его в метаданных
 * пула pool_rank. Заменяет пару ws_push_task_estimate + ABT_thread_create. */
int ws_thread_create(ABT_pool pool, int pool_rank, void (*fn)(void *), void *arg,
                     long long est, ABT_thread *newthread);

/* То же, что ws_thread_create, плюс подсказка стоимости hint в единицах программы;
 * рантайм переводит её в наносекунды по истории выполнения задач с той же функцией. */
int ws_thread_create_hint(ABT_pool pool, int pool_rank, void (*fn)(void *), void *arg,
                          long long est, long long hint, ABT_thread *newthread);

/* То же, но память под метаданные даёт вызывающий: заполняет fn, arg, est
 * и держит структуру живой до завершения ULT (например, на своём стеке до
 * join). Избавляет от malloc на каждую задачу. */
int ws_thread_create_with_meta(ABT_pool pool, int pool_rank, ws_task_meta *meta,
                               ABT_thread *newthread);

/* Метаданные ULT, если он создан ws_thread_create; иначе NULL
 * (primary ULT, проснувшийся после join или барьера, любой чужой ULT). */
ws_task_meta *ws_task_meta_of(ABT_thread thread);

/* Включает оценки в наносекундах и историю выполнения для num_ranks ES. */
int ws_task_cost_init(int num_ranks);
/* Выключает оценки в наносекундах и освобождает историю. */
void ws_task_cost_finalize(void);
/* 1, если оценки в наносекундах включены. */
int ws_task_cost_enabled(void);
/* Метаданные выполняемой задачи или NULL. */
ws_task_meta *ws_task_self_meta(void);
/* Оценка из истории для задачи с функцией fn на глубине depth; 0, если истории нет. */
long long ws_task_cost_lookup(void (*fn)(void *), int depth);

#ifdef __cplusplus
}
#endif
