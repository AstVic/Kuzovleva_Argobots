#pragma once
#include <abt.h>

// Cost-aware work stealing scheduler
void ABT_create_ws_scheds_cost_aware(int num, ABT_pool *pools, ABT_sched *scheds);

/* Метаданные задач для выбора жертвы по оценочной стоимости очередей. */
void ws_push_task_estimate(int rank, double est);
double ws_pop_task_estimate(int rank);
double ws_get_pool_estimated_load(int rank);

/* Историческая статистика выполнения задач. */
void ws_update_task_time(double elapsed, int rank);
void ws_print_global_stats(void);
