#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include "abt_reduction.h"
#include "../argobots_framework/examples/workstealing_scheduler/abt_workstealing_scheduler.h"
#include "../argobots_framework/examples/workstealing_scheduler/abt_workstealing_scheduler_cost_aware.h"

#define Max(a, b) ((a) > (b) ? (a) : (b))
#define L 384
#define ITMAX 100

#define DEFAULT_XSTREAMS 4
#define DEFAULT_THREADS 4

float A[L][L][L];
float B[L][L][L];
float MAXEPS = 0.5f;


static ABT_sched *g_scheds = NULL;
static int g_use_ws_scheduler = 0;
static int g_use_cost_aware_scheduler = 0;

static void configure_scheduler_mode(void) {
    const char *scheduler_mode = getenv("ABT_WS_SCHEDULER");
    if (!scheduler_mode || scheduler_mode[0] == '\0' || strcmp(scheduler_mode, "default") == 0) {
        g_use_ws_scheduler = 0;
        g_use_cost_aware_scheduler = 0;
        return;
    }
    g_use_ws_scheduler = 1;
    g_use_cost_aware_scheduler = (strcmp(scheduler_mode, "new") == 0 || strcmp(scheduler_mode, "cost-aware") == 0);
}

static inline void register_task_estimate_if_needed(int pool_id, double estimate) {
    if (g_use_cost_aware_scheduler) {
        ws_push_task_estimate(pool_id, estimate);
    }
}

typedef struct {
    int start_i;
    int end_i;
    float *eps_local;
} jacobi_args_t;

void update_A_thread(void *arg) {
    jacobi_args_t *jacobi_args = (jacobi_args_t *)arg;
    float local_eps = 0.0f;
    
    for (int i = jacobi_args->start_i; i < jacobi_args->end_i; i++) {
        for (int j = 1; j < L-1; j++) {
            for (int k = 1; k < L-1; k++) {
                float tmp = fabs(B[i][j][k] - A[i][j][k]);
                local_eps = Max(tmp, local_eps);
                A[i][j][k] = B[i][j][k];
            }
        }
    }
    
    *(jacobi_args->eps_local) = local_eps;
}

void update_B_thread(void *arg) {
    jacobi_args_t *jacobi_args = (jacobi_args_t *)arg;
    
    for (int i = jacobi_args->start_i; i < jacobi_args->end_i; i++) {
        for (int j = 1; j < L-1; j++) {
            for (int k = 1; k < L-1; k++) {
                B[i][j][k] = (A[i-1][j][k] + A[i][j-1][k] + 
                             A[i][j][k-1] + A[i][j][k+1] + 
                             A[i][j+1][k] + A[i+1][j][k]) / 6.0f;
            }
        }
    }
}

void initialize_argobots(reduction_context_t *reduction_context, int num_xstreams, int num_threads) {
    /* Initialize Argobots. */
    ABT_init(0, NULL);
    configure_scheduler_mode();

    reduction_context->num_xstreams = num_xstreams;
    reduction_context->xstreams = (ABT_xstream *)malloc(sizeof(ABT_xstream) * num_xstreams);

    int num_pools = num_xstreams;
    reduction_context->num_pools = num_pools;
    reduction_context->pools = (ABT_pool *)malloc(sizeof(ABT_pool) * num_pools);

    reduction_context->num_threads = num_threads;
    reduction_context->threads = (ABT_thread *)malloc(sizeof(ABT_thread) * num_threads);

    if (g_use_ws_scheduler) {
        g_scheds = (ABT_sched *)calloc(num_xstreams, sizeof(ABT_sched));
        for (int i = 0; i < num_xstreams; i++) {
            ABT_pool_create_basic(ABT_POOL_FIFO, ABT_POOL_ACCESS_MPMC, ABT_TRUE,
                                  &(reduction_context->pools[i]));
        }

        if (g_use_cost_aware_scheduler) {
            ABT_create_ws_scheds_cost_aware(num_xstreams, reduction_context->pools, g_scheds);
        } else {
            ABT_create_ws_scheds(num_xstreams, reduction_context->pools, g_scheds);
        }

        ABT_xstream_self(&(reduction_context->xstreams[0]));
        ABT_xstream_set_main_sched(reduction_context->xstreams[0], g_scheds[0]);
        for (int i = 1; i < num_xstreams; i++) {
            ABT_xstream_create(g_scheds[i], &(reduction_context->xstreams[i]));
        }
    } else {
        /* Get a primary execution stream. */
        ABT_xstream_self(&(reduction_context->xstreams[0]));

        /* Create secondary execution streams. */
        for (int i = 1; i < num_xstreams; i++) {
            ABT_xstream_create(ABT_SCHED_NULL, &(reduction_context->xstreams[i]));
        }

        /* Get default pools. */
        for (int i = 0; i < num_xstreams; i++) {
            ABT_xstream_get_main_pools(reduction_context->xstreams[i], 1,
                                       &(reduction_context->pools[i]));
        }
    }
}

void finalize_argobots(reduction_context_t *reduction_context) {
    /* Free ULTs. */
    for (int i = 0; i < reduction_context->num_threads; i++) {
        ABT_thread_free(&reduction_context->threads[i]);
    }    
    
    /* Join and free secondary execution streams. */
    for (int i = 1; i < reduction_context->num_xstreams; i++) {
        ABT_xstream_join(reduction_context->xstreams[i]);
        ABT_xstream_free(&reduction_context->xstreams[i]);
    }
    
    if (g_scheds) {
        free(g_scheds);
        g_scheds = NULL;
    }

    /* Finalize Argobots. */
    ABT_finalize();

     /* Free allocated memory. */
    free(reduction_context->xstreams);
    free(reduction_context->pools);
    free(reduction_context->threads);
}

int main(int argc, char **argv) {
    int num_xstreams = DEFAULT_XSTREAMS;
    int num_threads = DEFAULT_THREADS;
    float eps;
    clock_t start, end;
    struct timespec start_real_time, end_real_time;
    double cpu_time_used;
    
    /* Parse command line arguments if provided */
    if (argc > 1) {
        num_xstreams = atoi(argv[1]);
        if (num_xstreams <= 0) num_xstreams = DEFAULT_XSTREAMS;
        if (argc > 2) {
            num_threads = atoi(argv[2]);
            if (num_threads <= 0) num_threads = DEFAULT_THREADS;
        }
    }
    
    printf("Running Jacobi-3D with xstreams=%d and threads=%d\n", num_xstreams, num_threads);
    
    /* Initialize Argobots */
    reduction_context_t reduction_context;
    initialize_argobots(&reduction_context, num_xstreams, num_threads);
    
    for (int i = 0; i < L; i++) {
        for (int j = 0; j < L; j++) {
            for (int k = 0; k < L; k++) {
                A[i][j][k] = 0;
                if (i == 0 ||  j == 0 ||  k == 0 ||  i == L-1 ||  j == L-1 || k == L-1)
                    B[i][j][k] = 0;
                else
                    B[i][j][k] = 4 + i + j + k;
            }
        }
    }
    
    start = clock();
    clock_gettime(CLOCK_REALTIME, &start_real_time);
    
    jacobi_args_t *thread_args = (jacobi_args_t *)malloc(sizeof(jacobi_args_t) * num_threads);
    float *eps_values = (float *)malloc(sizeof(float) * num_threads);
    int rows_per_thread = (L - 2) / num_threads;
    for (int it = 1; it <= ITMAX; it++) {
        for (int t = 0; t < num_threads; t++) {
            thread_args[t].start_i = 1 + t * rows_per_thread;
            thread_args[t].end_i = (t == num_threads - 1) ? L - 1 : thread_args[t].start_i + rows_per_thread;
            thread_args[t].eps_local = &eps_values[t];
            
            register_task_estimate_if_needed(t % reduction_context.num_pools, (double)(rows_per_thread * L * L));
            ABT_thread_create(
                reduction_context.pools[t % reduction_context.num_pools],
                update_A_thread,
                &thread_args[t],
                ABT_THREAD_ATTR_NULL,
                &reduction_context.threads[t]
            );
        }
        for (int t = 0; t < num_threads; t++) {
            ABT_thread_join(reduction_context.threads[t]);
            ABT_thread_free(&reduction_context.threads[t]);
        }
        
        /* Use Argobots reduction to find maximum epsilon */
        reduce_max_float(&reduction_context, eps_values, num_threads, &eps);
        
        for (int t = 0; t < num_threads; t++) {
            register_task_estimate_if_needed(t % reduction_context.num_pools, (double)(rows_per_thread * L * L));
            ABT_thread_create(
                reduction_context.pools[t % reduction_context.num_pools],
                update_B_thread,
                &thread_args[t],
                ABT_THREAD_ATTR_NULL,
                &reduction_context.threads[t]
            );
        }
        for (int t = 0; t < num_threads; t++) {
            ABT_thread_join(reduction_context.threads[t]);
            ABT_thread_free(&reduction_context.threads[t]);
        }
        
        // printf(" IT = %4i   EPS = %14.7E\n", it, eps);
        if (eps < MAXEPS)
            break;
    }
    
    end = clock();
    clock_gettime(CLOCK_REALTIME, &end_real_time);
    cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
    long long real_time_nanoseconds = (end_real_time.tv_sec - start_real_time.tv_sec) * 1000000000 + (end_real_time.tv_nsec - start_real_time.tv_nsec);
    
    free(thread_args);
    free(eps_values);
    finalize_argobots(&reduction_context);
    
    printf(" Jacobi3D Benchmark Completed.\n");
    printf(" Size              = %4d x %4d x %4d\n", L, L, L);
    printf(" Iterations        =       %12d\n", ITMAX);
    printf(" Time in seconds   =       %12.2lf\n", cpu_time_used);
    printf(" Real time (nanos) =       %12lld\n", real_time_nanoseconds);
    printf(" Operation type    =     floating point\n");
    printf(" Verification      =       %12s\n", 
           (fabs(eps - 5.058044) < 1e-4 ? "SUCCESSFUL" : "UNSUCCESSFUL"));
    printf(" END OF Jacobi3D Benchmark\n");
    
    return 0;
}