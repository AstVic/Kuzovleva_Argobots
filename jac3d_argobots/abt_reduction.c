#include "abt_reduction.h"

#include <limits.h>
#include <float.h>
#include <stdlib.h>
#include <string.h>

#if USE_TREE_REDUCTION

typedef struct {
    void *array;                         /* array, on which reduction will be performed */
    size_t num_elems;                    /* number of elements to perform reduction on */
    size_t elem_size;                    /* size of a single element */
    void* default_reduction_value;       /* 0 for sum, 1 for multiplication, etc. */
    void *result;                        /* where to store the result of reduction */
    void (*reduce_func)(void *, void *); /* provided reduction function on 2 elements */
    void **thread_results;               /* array to store thread results */
    ABT_barrier barrier;                 /* barrier to synchronize threads */
    int num_threads;                     /* total number of threads */
    int thread_id;                       /* index of the current thread */
} reduction_args_t;

void reduction_thread(void *arg) {
    reduction_args_t *reduction_args = (reduction_args_t *)arg;
    size_t num_elems = reduction_args->num_elems;
    size_t elem_size = reduction_args->elem_size;
    char *array = (char *)reduction_args->array;
    int thread_id = reduction_args->thread_id;
    int num_threads = reduction_args->num_threads;

    // Initialize local result to default value of a reduction
    void *local_result = (void *)malloc(elem_size);
    memcpy(local_result, reduction_args->default_reduction_value, elem_size);

    for (size_t i = 0; i < num_elems; ++i) {
        reduction_args->reduce_func(local_result, array + i * elem_size);
    }
    reduction_args->thread_results[thread_id] = (void *)malloc(elem_size);
    memcpy(reduction_args->thread_results[thread_id], local_result, elem_size);

    ABT_barrier_wait(reduction_args->barrier);
    int step = 1;
    while (step < num_threads) {
        if (thread_id % (2 * step) == 0) {
            int partner_thread_id = thread_id + step;
            if (partner_thread_id < num_threads) {
                reduction_args->reduce_func(
                    reduction_args->thread_results[thread_id],
                    reduction_args->thread_results[partner_thread_id]
                );
            }
        }
        step *= 2;
        ABT_barrier_wait(reduction_args->barrier);
    }

    if (thread_id == 0) {
        memcpy(reduction_args->result, reduction_args->thread_results[0], elem_size);
    }

    free(local_result);
    free(reduction_args->thread_results[thread_id]);
}

void reduce_common(
    reduction_context_t *reduction_context,
    void *array,
    size_t num_elems,
    size_t elem_size,
    void *default_reduction_value,
    void (*reduce_func)(void *, void *),
    void *result
) {
    int num_threads = reduction_context->num_threads;
    size_t elems_per_thread = num_elems / num_threads;
    reduction_args_t *thread_args = 
        (reduction_args_t *)malloc(sizeof(reduction_args_t) * num_threads);
    void **thread_results = malloc(num_threads * sizeof(void *));
    ABT_barrier barrier;
    ABT_barrier_create(num_threads, &barrier);

    for (int i = 0; i < num_threads; ++i) {
        thread_args[i].array = (char *)array + i * elems_per_thread * elem_size;
        thread_args[i].num_elems = (i == num_threads - 1) ? (num_elems - i * elems_per_thread) : elems_per_thread;
        thread_args[i].elem_size = elem_size;
        thread_args[i].default_reduction_value = default_reduction_value;
        thread_args[i].result = result;
        thread_args[i].reduce_func = reduce_func;
        thread_args[i].thread_results = thread_results;
        thread_args[i].barrier = barrier;
        thread_args[i].num_threads = num_threads;
        thread_args[i].thread_id = i;
    }

    for (int i = 0; i < num_threads; ++i) {
        int pool_id = i % reduction_context->num_pools;
        ABT_thread_create(
            reduction_context->pools[pool_id],
            reduction_thread,
            &thread_args[i],
            ABT_THREAD_ATTR_NULL,
            &reduction_context->threads[i]
        );
    }

    for (int i = 0; i < num_threads; ++i) {
        ABT_thread_join(reduction_context->threads[i]);
        ABT_thread_free(&reduction_context->threads[i]);
    }

    ABT_barrier_free(&barrier);
    free(thread_results);
    free(thread_args);
}

#else

typedef struct {
    void *array;                         /* array, on which reduction will be performed */
    size_t num_elems;                    /* number of elements to perform reduction on */
    size_t elem_size;                    /* size of a single element */
    void* default_reduction_value;       /* 0 for sum, 1 for multiplication, etc. */
    void *result;                        /* where to store the result of reduction */
    void (*reduce_func)(void *, void *); /* provided reduction function on 2 elements */
    ABT_mutex mutex;                     /* mutex to perform final (among different threads) reduction */
} reduction_args_t;

void reduction_thread(void *arg) {
    reduction_args_t *reduction_args = (reduction_args_t *)arg;
    size_t num_elems = reduction_args->num_elems;
    size_t elem_size = reduction_args->elem_size;
    char *array = (char *)reduction_args->array;
    
    // Initialize local result to default value of a reduction
    void *local_result = (void *)malloc(elem_size);
    memcpy(local_result, reduction_args->default_reduction_value, elem_size);

    for (size_t i = 0; i < num_elems; ++i) {
        reduction_args->reduce_func(local_result, array + i * elem_size);
    }

    ABT_mutex_lock(reduction_args->mutex);
    reduction_args->reduce_func(reduction_args->result, local_result);
    ABT_mutex_unlock(reduction_args->mutex);

    free(local_result);
}

void reduce_common(
    reduction_context_t *reduction_context,
    void *array,
    size_t num_elems,
    size_t elem_size,
    void *default_reduction_value,
    void (*reduce_func)(void *, void *),
    void *result
) {
    int num_threads = reduction_context->num_threads;
    size_t elems_per_thread = num_elems / num_threads;
    reduction_args_t *thread_args = 
        (reduction_args_t *)malloc(sizeof(reduction_args_t) * num_threads);
    ABT_mutex mutex;
    ABT_mutex_create(&mutex);

    memcpy(result, default_reduction_value, elem_size);

    for (int i = 0; i < num_threads; ++i) {
        thread_args[i].array = (char *)array + i * elems_per_thread * elem_size;
        thread_args[i].num_elems = (i == num_threads - 1) ? (num_elems - i * elems_per_thread) : elems_per_thread;
        thread_args[i].elem_size = elem_size;
        thread_args[i].default_reduction_value = default_reduction_value;
        thread_args[i].result = result;
        thread_args[i].reduce_func = reduce_func;
        thread_args[i].mutex = mutex;
    }

    for (int i = 0; i < num_threads; ++i) {
        int pool_id = i % reduction_context->num_pools;
        ABT_thread_create(
            reduction_context->pools[pool_id],
            reduction_thread,
            &thread_args[i],
            ABT_THREAD_ATTR_NULL,
            &(reduction_context->threads[i])
        );

    }

    for (int i = 0; i < num_threads; ++i) {
        ABT_thread_join(reduction_context->threads[i]);
        ABT_thread_free(&reduction_context->threads[i]);
    }

    ABT_mutex_free(&mutex);
    free(thread_args);
}

#endif

// =================== Definitions for reduction funcs ===================

#define BODY_sum(type) *((type *)a) += *((type *)b);
#define BODY_sub(type) *((type *)a) -= *((type *)b);
#define BODY_prod(type) *((type *)a) *= *((type *)b);
#define BODY_and(type) *((type *)a) &= *((type *)b);
#define BODY_or(type) *((type *)a) |= *((type *)b);
#define BODY_xor(type) *((type *)a) ^= *((type *)b);
#define BODY_logical_and(type) *((type *)a) = *((type *)b) && *((type *)a);
#define BODY_logical_or(type) *((type *)a) = *((type *)b) || *((type *)a);
#define BODY_max(type) if (*((type *)a) < *((type *)b)) *((type *)a) = *((type *)b);
#define BODY_min(type) if (*((type *)a) > *((type *)b)) *((type *)a) = *((type *)b);

#define DEF_HOST(func, type, type_str) \
void reduce_##func##_##type_str##_func(void *a, void *b) { BODY_##func(type) }

// Use in case when type and it's string representation are the same (for example, int, float)
#define DEF_HOST_SIMPLE(func, type) DEF_HOST(func, type, type)

DEF_HOST_SIMPLE(sum, char);
DEF_HOST_SIMPLE(sub, char);
DEF_HOST_SIMPLE(prod, char);
DEF_HOST_SIMPLE(and, char);
DEF_HOST_SIMPLE(or, char);
DEF_HOST_SIMPLE(xor, char);
DEF_HOST_SIMPLE(logical_and, char);
DEF_HOST_SIMPLE(logical_or, char);
DEF_HOST_SIMPLE(max, char);
DEF_HOST_SIMPLE(min, char);

DEF_HOST_SIMPLE(sum, int);
DEF_HOST_SIMPLE(sub, int);
DEF_HOST_SIMPLE(prod, int);
DEF_HOST_SIMPLE(and, int);
DEF_HOST_SIMPLE(or, int);
DEF_HOST_SIMPLE(xor, int);
DEF_HOST_SIMPLE(logical_and, int);
DEF_HOST_SIMPLE(logical_or, int);
DEF_HOST_SIMPLE(max, int);
DEF_HOST_SIMPLE(min, int);

DEF_HOST_SIMPLE(sum, long);
DEF_HOST_SIMPLE(sub, long);
DEF_HOST_SIMPLE(prod, long);
DEF_HOST_SIMPLE(and, long);
DEF_HOST_SIMPLE(or, long);
DEF_HOST_SIMPLE(xor, long);
DEF_HOST_SIMPLE(logical_and, long);
DEF_HOST_SIMPLE(logical_or, long);
DEF_HOST_SIMPLE(max, long);
DEF_HOST_SIMPLE(min, long);

DEF_HOST(sum, long long, long_long);
DEF_HOST(sub, long long, long_long);
DEF_HOST(prod, long long, long_long);
DEF_HOST(and, long long, long_long);
DEF_HOST(or, long long, long_long);
DEF_HOST(xor, long long, long_long);
DEF_HOST(logical_and, long long, long_long);
DEF_HOST(logical_or, long long, long_long);
DEF_HOST(max, long long, long_long);
DEF_HOST(min, long long, long_long);

DEF_HOST_SIMPLE(sum, float);
DEF_HOST_SIMPLE(sub, float);
DEF_HOST_SIMPLE(prod, float);
DEF_HOST_SIMPLE(max, float);
DEF_HOST_SIMPLE(min, float);

DEF_HOST_SIMPLE(sum, double);
DEF_HOST_SIMPLE(sub, double);
DEF_HOST_SIMPLE(prod, double);
DEF_HOST_SIMPLE(max, double);
DEF_HOST_SIMPLE(min, double);


#define DEFINE_REDFUNC(func, type, type_str, default_value) DECLARE_REDFUNC(func, type, type_str) { \
    type default_reduction_value = default_value; \
    reduce_common(reduction_context, array, num_elems, sizeof(type), \
                  &default_reduction_value, reduce_##func##_##type_str##_func, result); \
}

// Use in case when type and it's string representation are the same (for example, int, float)
#define DEFINE_REDFUNC_SIMPLE(func, type, default_value) DEFINE_REDFUNC(func, type, type, default_value)

DEFINE_REDFUNC_SIMPLE(sum, char, 0);
DEFINE_REDFUNC_SIMPLE(sub, char, 0);
DEFINE_REDFUNC_SIMPLE(prod, char, 1);
DEFINE_REDFUNC_SIMPLE(and, char, ~(char)0);
DEFINE_REDFUNC_SIMPLE(or, char, 0);
DEFINE_REDFUNC_SIMPLE(xor, char, 0);
DEFINE_REDFUNC_SIMPLE(logical_and, char, 1);
DEFINE_REDFUNC_SIMPLE(logical_or, char, 0);
DEFINE_REDFUNC_SIMPLE(max, char, CHAR_MIN);
DEFINE_REDFUNC_SIMPLE(min, char, CHAR_MAX);

DEFINE_REDFUNC_SIMPLE(sum, int, 0);
DEFINE_REDFUNC_SIMPLE(sub, int, 0);
DEFINE_REDFUNC_SIMPLE(prod, int, 1);
DEFINE_REDFUNC_SIMPLE(and, int, ~(int)0);
DEFINE_REDFUNC_SIMPLE(or, int, 0);
DEFINE_REDFUNC_SIMPLE(xor, int, 0);
DEFINE_REDFUNC_SIMPLE(logical_and, int, 1);
DEFINE_REDFUNC_SIMPLE(logical_or, int, 0);
DEFINE_REDFUNC_SIMPLE(max, int, INT_MIN);
DEFINE_REDFUNC_SIMPLE(min, int, INT_MAX);

DEFINE_REDFUNC_SIMPLE(sum, long, 0);
DEFINE_REDFUNC_SIMPLE(sub, long, 0);
DEFINE_REDFUNC_SIMPLE(prod, long, 1);
DEFINE_REDFUNC_SIMPLE(and, long, ~(long)0);
DEFINE_REDFUNC_SIMPLE(or, long, 0);
DEFINE_REDFUNC_SIMPLE(xor, long, 0);
DEFINE_REDFUNC_SIMPLE(logical_and, long, 1);
DEFINE_REDFUNC_SIMPLE(logical_or, long, 0);
DEFINE_REDFUNC_SIMPLE(max, long, LONG_MIN);
DEFINE_REDFUNC_SIMPLE(min, long, LONG_MAX);

DEFINE_REDFUNC(sum, long long, long_long, 0);
DEFINE_REDFUNC(sub, long long, long_long, 0);
DEFINE_REDFUNC(prod, long long, long_long, 1);
DEFINE_REDFUNC(and, long long, long_long, ~(long long)0);
DEFINE_REDFUNC(or, long long, long_long, 0);
DEFINE_REDFUNC(xor, long long, long_long, 0);
DEFINE_REDFUNC(logical_and, long long, long_long, 0);
DEFINE_REDFUNC(logical_or, long long, long_long, 0);
DEFINE_REDFUNC(max, long long, long_long, LLONG_MIN);
DEFINE_REDFUNC(min, long long, long_long, LLONG_MAX);

DEFINE_REDFUNC_SIMPLE(sum, float, 0);
DEFINE_REDFUNC_SIMPLE(sub, float, 0);
DEFINE_REDFUNC_SIMPLE(prod, float, 1);
DEFINE_REDFUNC_SIMPLE(max, float, FLT_MIN);
DEFINE_REDFUNC_SIMPLE(min, float, FLT_MAX);

DEFINE_REDFUNC_SIMPLE(sum, double, 0);
DEFINE_REDFUNC_SIMPLE(sub, double, 0);
DEFINE_REDFUNC_SIMPLE(prod, double, 1);
DEFINE_REDFUNC_SIMPLE(max, double, DBL_MIN);
DEFINE_REDFUNC_SIMPLE(min, double, DBL_MAX);
// =================== End Definitions for reduction funcs ===============
