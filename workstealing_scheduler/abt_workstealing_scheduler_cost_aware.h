#pragma once
#include <abt.h>

// Cost-aware work stealing scheduler
void ABT_create_ws_scheds_cost_aware(int num, ABT_pool *pools, ABT_sched *scheds);

/* Метаданные задач для выбора жертвы по оценочной стоимости очередей. */
void ws_push_task_estimate(int rank, long long est);
long long ws_pop_task_estimate(int rank);
long long ws_get_pool_estimated_load(int rank);
void ws_reset_steal_count(void);
long long ws_get_steal_count(void);
long long ws_get_steal_ops_count(void);
long long ws_get_stolen_tasks_count(void);

/* Печать текущего состояния метаданных пулов (отладочная). */
void ws_print_global_stats(void);
