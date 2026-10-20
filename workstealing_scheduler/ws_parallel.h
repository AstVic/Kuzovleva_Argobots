#pragma once

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void (*ws_range_body)(long lo, long hi, void *arg);
typedef double (*ws_range_cost)(long lo, long hi, void *arg);
typedef void (*ws_reduce_body)(long lo, long hi, void *partial, void *arg);
typedef void (*ws_reduce_combine)(void *acc, const void *partial, void *arg);

int ws_init(int num_workers);
void ws_finalize(void);
int ws_num_workers(void);

void ws_parallel_for(long lo, long hi, ws_range_body body, void *arg);
void ws_parallel_for_grain(long lo, long hi, long grain, ws_range_body body, void *arg);
void ws_parallel_for_cost(long lo, long hi, ws_range_body body, void *arg, ws_range_cost cost,
                          void *cost_arg);

void ws_parallel_reduce(long lo, long hi, size_t elem_size, const void *identity,
                        ws_reduce_body body, ws_reduce_combine combine, void *result, void *arg);

typedef struct ws_group ws_group;

ws_group *ws_group_create(void);
int ws_group_spawn(ws_group *group, void (*fn)(void *), void *arg, long long hint);
void ws_group_wait(ws_group *group);
void ws_group_free(ws_group *group);

#ifdef __cplusplus
}
#endif
