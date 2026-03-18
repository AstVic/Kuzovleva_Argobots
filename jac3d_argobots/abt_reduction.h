#pragma once

#include <abt.h>

#define USE_TREE_REDUCTION 1

typedef struct {
    ABT_xstream *xstreams;
    int num_xstreams;
    ABT_pool *pools;
    int num_pools;
    ABT_thread *threads;
    int num_threads;
} reduction_context_t;

void reduce_common(
    reduction_context_t *reduction_context,
    void *array,
    size_t num_elems,
    size_t elem_size,
    void *default_reduction_value,
    void (*reduce_func)(void *, void *),
    void *result
);

// =================== Declarations for reduction funcs ===================
#define DECLARE_REDFUNC(func, type, type_str) \
void reduce_##func##_##type_str(reduction_context_t *reduction_context, type *array, size_t num_elems, type *result)

// Use in case when type and it's string representation are the same (for example, int, float)
#define DECLARE_REDFUNC_SIMPLE(func, type) DECLARE_REDFUNC(func, type, type)

DECLARE_REDFUNC_SIMPLE(sum, char);
DECLARE_REDFUNC_SIMPLE(sub, char);
DECLARE_REDFUNC_SIMPLE(prod, char);
DECLARE_REDFUNC_SIMPLE(and, char);
DECLARE_REDFUNC_SIMPLE(or, char);
DECLARE_REDFUNC_SIMPLE(xor, char);
DECLARE_REDFUNC_SIMPLE(logical_and, char);
DECLARE_REDFUNC_SIMPLE(logical_or, char);
DECLARE_REDFUNC_SIMPLE(max, char);
DECLARE_REDFUNC_SIMPLE(min, char);

DECLARE_REDFUNC_SIMPLE(sum, int);
DECLARE_REDFUNC_SIMPLE(sub, int);
DECLARE_REDFUNC_SIMPLE(prod, int);
DECLARE_REDFUNC_SIMPLE(and, int);
DECLARE_REDFUNC_SIMPLE(or, int);
DECLARE_REDFUNC_SIMPLE(xor, int);
DECLARE_REDFUNC_SIMPLE(logical_and, int);
DECLARE_REDFUNC_SIMPLE(logical_or, int);
DECLARE_REDFUNC_SIMPLE(max, int);
DECLARE_REDFUNC_SIMPLE(min, int);

DECLARE_REDFUNC_SIMPLE(sum, long);
DECLARE_REDFUNC_SIMPLE(sub, long);
DECLARE_REDFUNC_SIMPLE(prod, long);
DECLARE_REDFUNC_SIMPLE(and, long);
DECLARE_REDFUNC_SIMPLE(or, long);
DECLARE_REDFUNC_SIMPLE(xor, long);
DECLARE_REDFUNC_SIMPLE(logical_and, long);
DECLARE_REDFUNC_SIMPLE(logical_or, long);
DECLARE_REDFUNC_SIMPLE(max, long);
DECLARE_REDFUNC_SIMPLE(min, long);

DECLARE_REDFUNC(sum, long long, long_long);
DECLARE_REDFUNC(sub, long long, long_long);
DECLARE_REDFUNC(prod, long long, long_long);
DECLARE_REDFUNC(and, long long, long_long);
DECLARE_REDFUNC(or, long long, long_long);
DECLARE_REDFUNC(xor, long long, long_long);
DECLARE_REDFUNC(logical_and, long long, long_long);
DECLARE_REDFUNC(logical_or, long long, long_long);
DECLARE_REDFUNC(max, long long, long_long);
DECLARE_REDFUNC(min, long long, long_long);

DECLARE_REDFUNC_SIMPLE(sum, float);
DECLARE_REDFUNC_SIMPLE(sub, float);
DECLARE_REDFUNC_SIMPLE(prod, float);
DECLARE_REDFUNC_SIMPLE(max, float);
DECLARE_REDFUNC_SIMPLE(min, float);

DECLARE_REDFUNC_SIMPLE(sum, double);
DECLARE_REDFUNC_SIMPLE(sub, double);
DECLARE_REDFUNC_SIMPLE(prod, double);
DECLARE_REDFUNC_SIMPLE(max, double);
DECLARE_REDFUNC_SIMPLE(min, double);
// =================== End Declarations for reduction funcs ===============
