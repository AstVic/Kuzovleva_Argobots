#pragma once
#include <abt.h>

// Cost-aware work stealing scheduler
void ABT_create_ws_scheds_cost_aware(int num, ABT_pool *pools, ABT_sched *scheds);

/* Учёт задач в метаданных пула. Вызывается из ws_thread_create (ws_task.h),
 * который и является штатным способом создать задачу с оценкой стоимости. */
void ws_account_task_created(int rank, long long est);
void ws_account_task_dispatched(int rank, long long est);

/* УСТАРЕЛО: оценка теперь хранится в самом ULT. Функция работает только в
 * сборке со старым механизмом (-DWS_LEGACY_EST_FIFO), иначе игнорируется. */
void ws_push_task_estimate(int rank, long long est);
long long ws_pop_task_estimate(int rank);
long long ws_get_pool_estimated_load(int rank);
long long ws_get_pool_queued_count(int rank);
void ws_reset_steal_count(void);
long long ws_get_steal_count(void);
long long ws_get_steal_ops_count(void);
long long ws_get_stolen_tasks_count(void);

/* Печать текущего состояния метаданных пулов (отладочная). */
void ws_print_global_stats(void);

/* Проверка сопоставления "ULT <-> оценка". Счётчики меняются только при
 * сборке с -DWS_DEBUG_COST_CHECK, иначе остаются нулевыми. */
void ws_debug_reset(void);
long long ws_debug_dispatched(void);
long long ws_debug_wrong_estimate(void);
long long ws_debug_missing_estimate(void);
long long ws_debug_foreign_with_estimate(void);
