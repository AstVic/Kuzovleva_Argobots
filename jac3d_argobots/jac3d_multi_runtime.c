#include <abt.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "../workstealing_scheduler/abt_workstealing_scheduler.h"
#include "../workstealing_scheduler/abt_workstealing_scheduler_cost_aware.h"

#define DEFAULT_XSTREAMS 4
#define DEFAULT_THREADS 4
#define DEFAULT_ITMAX 100
#define DEFAULT_MAXEPS 0.5f

typedef enum {
    JACOBI_PHASE_A = 0,
    JACOBI_PHASE_B = 1,
    JACOBI_PHASE_REDUCTION = 2
} jacobi_phase_t;

typedef struct {
    int size;
    int itmax;
    float maxeps;
    float *a;
    float *b;
    float final_eps;
    double checksum;
    int iterations_done;
    int success;
    int converged;
} jacobi_problem_t;

typedef struct {
    ABT_xstream *xstreams;
    ABT_pool *pools;
    ABT_sched *scheds;
    int num_xstreams;
    int use_ws_scheduler;
    int use_cost_aware_scheduler;
} runtime_context_t;

typedef struct {
    jacobi_problem_t *problem;
    int start_i;
    int end_i;
    jacobi_phase_t phase;
    float *eps_local;
} jacobi_chunk_arg_t;

typedef struct {
    const float *input;
    int start;
    int end;
    float *output;
} reduction_chunk_arg_t;

static void configure_scheduler_mode(int *use_ws_scheduler,
                                     int *use_cost_aware_scheduler)
{
    const char *scheduler_mode = getenv("ABT_WS_SCHEDULER");
    if (!scheduler_mode || scheduler_mode[0] == '\0' ||
        strcmp(scheduler_mode, "default") == 0) {
        *use_ws_scheduler = 0;
        *use_cost_aware_scheduler = 0;
        return;
    }
    *use_ws_scheduler = 1;
    *use_cost_aware_scheduler =
        (strcmp(scheduler_mode, "new") == 0 ||
         strcmp(scheduler_mode, "cost-aware") == 0);
}

static size_t cell_index(int n, int i, int j, int k)
{
    return ((size_t)i * (size_t)n + (size_t)j) * (size_t)n + (size_t)k;
}

static double estimate_chunk_cost(int size, int rows, jacobi_phase_t phase)
{
    double cells = (double)rows * (double)(size - 2) * (double)(size - 2);
    double phase_factor = 1.0;
    if (phase == JACOBI_PHASE_A) {
        phase_factor = 1.2;
    } else if (phase == JACOBI_PHASE_REDUCTION) {
        phase_factor = 0.05;
    }
    return cells * phase_factor;
}

static double estimate_reduction_cost(int num_values)
{
    return (double)num_values;
}

static int task_pool_id(int problem_id, int chunk_id, int num_xstreams)
{
    int active_pools = num_xstreams / 2;
    if (active_pools < 1) {
        active_pools = 1;
    }
    return (problem_id + chunk_id) % active_pools;
}

static void initialize_problem_arrays(jacobi_problem_t *problem)
{
    int n = problem->size;
    for (int i = 0; i < n; i++) {
        for (int j = 0; j < n; j++) {
            for (int k = 0; k < n; k++) {
                size_t idx = cell_index(n, i, j, k);
                problem->a[idx] = 0.0f;
                if (i == 0 || j == 0 || k == 0 ||
                    i == n - 1 || j == n - 1 || k == n - 1) {
                    problem->b[idx] = 0.0f;
                } else {
                    problem->b[idx] = (float)(4 + i + j + k);
                }
            }
        }
    }
}

static double compute_checksum(const float *grid, int n)
{
    double sum = 0.0;
    size_t total = (size_t)n * (size_t)n * (size_t)n;
    for (size_t i = 0; i < total; i++) {
        sum += (double)grid[i];
    }
    return sum;
}

static void jacobi_chunk_run(void *arg)
{
    jacobi_chunk_arg_t *chunk = (jacobi_chunk_arg_t *)arg;
    jacobi_problem_t *problem = chunk->problem;
    int n = problem->size;

    if (chunk->phase == JACOBI_PHASE_A) {
        float local_eps = 0.0f;
        for (int i = chunk->start_i; i < chunk->end_i; i++) {
            for (int j = 1; j < n - 1; j++) {
                for (int k = 1; k < n - 1; k++) {
                    size_t idx = cell_index(n, i, j, k);
                    float tmp = fabsf(problem->b[idx] - problem->a[idx]);
                    if (tmp > local_eps) {
                        local_eps = tmp;
                    }
                    problem->a[idx] = problem->b[idx];
                }
            }
        }
        *chunk->eps_local = local_eps;
    } else {
        for (int i = chunk->start_i; i < chunk->end_i; i++) {
            for (int j = 1; j < n - 1; j++) {
                for (int k = 1; k < n - 1; k++) {
                    size_t idx = cell_index(n, i, j, k);
                    problem->b[idx] =
                        (problem->a[cell_index(n, i - 1, j, k)] +
                         problem->a[cell_index(n, i, j - 1, k)] +
                         problem->a[cell_index(n, i, j, k - 1)] +
                         problem->a[cell_index(n, i, j, k + 1)] +
                         problem->a[cell_index(n, i, j + 1, k)] +
                         problem->a[cell_index(n, i + 1, j, k)]) / 6.0f;
                }
            }
        }
    }
}

static void reduction_chunk_run(void *arg)
{
    reduction_chunk_arg_t *chunk = (reduction_chunk_arg_t *)arg;
    float local_max = 0.0f;

    for (int i = chunk->start; i < chunk->end; i++) {
        if (chunk->input[i] > local_max) {
            local_max = chunk->input[i];
        }
    }
    *chunk->output = local_max;
}

static int init_runtime(runtime_context_t *ctx, int num_xstreams)
{
    ABT_init(0, NULL);
    configure_scheduler_mode(&ctx->use_ws_scheduler,
                             &ctx->use_cost_aware_scheduler);

    ctx->num_xstreams = num_xstreams;
    ctx->xstreams = (ABT_xstream *)calloc((size_t)num_xstreams,
                                          sizeof(ABT_xstream));
    ctx->pools = (ABT_pool *)calloc((size_t)num_xstreams, sizeof(ABT_pool));
    ctx->scheds = NULL;
    if (!ctx->xstreams || !ctx->pools) {
        return -1;
    }

    if (ctx->use_ws_scheduler) {
        ctx->scheds = (ABT_sched *)calloc((size_t)num_xstreams,
                                          sizeof(ABT_sched));
        if (!ctx->scheds) {
            return -1;
        }

        for (int i = 0; i < num_xstreams; i++) {
            ABT_pool_create_basic(ABT_POOL_FIFO, ABT_POOL_ACCESS_MPMC,
                                  ABT_TRUE, &ctx->pools[i]);
        }

        if (ctx->use_cost_aware_scheduler) {
            ABT_create_ws_scheds_cost_aware(num_xstreams, ctx->pools, ctx->scheds);
        } else {
            ABT_create_ws_scheds(num_xstreams, ctx->pools, ctx->scheds);
        }

        ABT_xstream_self(&ctx->xstreams[0]);
        ABT_xstream_set_main_sched(ctx->xstreams[0], ctx->scheds[0]);
        for (int i = 1; i < num_xstreams; i++) {
            ABT_xstream_create(ctx->scheds[i], &ctx->xstreams[i]);
        }
    } else {
        ABT_xstream_self(&ctx->xstreams[0]);
        for (int i = 1; i < num_xstreams; i++) {
            ABT_xstream_create(ABT_SCHED_NULL, &ctx->xstreams[i]);
        }
        for (int i = 0; i < num_xstreams; i++) {
            ABT_xstream_get_main_pools(ctx->xstreams[i], 1, &ctx->pools[i]);
        }
    }

    return 0;
}

static void finalize_runtime(runtime_context_t *ctx)
{
    for (int i = 1; i < ctx->num_xstreams; i++) {
        ABT_xstream_join(ctx->xstreams[i]);
        ABT_xstream_free(&ctx->xstreams[i]);
    }

    free(ctx->scheds);
    free(ctx->xstreams);
    free(ctx->pools);
    ctx->scheds = NULL;
    ctx->xstreams = NULL;
    ctx->pools = NULL;

    ABT_finalize();
}

static void print_usage(const char *prog)
{
    fprintf(stderr, "Usage: %s <xstreams> <threads> <size1> [size2 ...]\n", prog);
    fprintf(stderr, "Example: %s 4 8 384 320 256 192\n", prog);
}

static int count_active_problems(const jacobi_problem_t *problems, int num_problems)
{
    int active = 0;
    for (int i = 0; i < num_problems; i++) {
        if (!problems[i].converged) {
            active++;
        }
    }
    return active;
}

int main(int argc, char **argv)
{
    if (argc < 4) {
        print_usage(argv[0]);
        return 1;
    }

    int num_xstreams = atoi(argv[1]);
    int num_threads = atoi(argv[2]);
    if (num_xstreams <= 0) {
        num_xstreams = DEFAULT_XSTREAMS;
    }
    if (num_threads <= 0) {
        num_threads = DEFAULT_THREADS;
    }

    int num_problems = argc - 3;
    jacobi_problem_t *problems =
        (jacobi_problem_t *)calloc((size_t)num_problems, sizeof(jacobi_problem_t));
    runtime_context_t runtime = {0};
    if (!problems) {
        fprintf(stderr, "Allocation failure for problem metadata.\n");
        return 1;
    }

    for (int i = 0; i < num_problems; i++) {
        int size = atoi(argv[i + 3]);
        size_t total_cells;
        if (size < 4) {
            fprintf(stderr, "Grid size must be >= 4, got %d\n", size);
            free(problems);
            return 1;
        }
        problems[i].size = size;
        problems[i].itmax = DEFAULT_ITMAX;
        problems[i].maxeps = DEFAULT_MAXEPS;
        problems[i].success = 0;
        problems[i].converged = 0;

        total_cells = (size_t)size * (size_t)size * (size_t)size;
        problems[i].a = (float *)malloc(total_cells * sizeof(float));
        problems[i].b = (float *)malloc(total_cells * sizeof(float));
        if (!problems[i].a || !problems[i].b) {
            fprintf(stderr, "Allocation failure for grid size %d\n", size);
            for (int k = 0; k <= i; k++) {
                free(problems[k].a);
                free(problems[k].b);
            }
            free(problems);
            return 1;
        }
        initialize_problem_arrays(&problems[i]);
    }

    if (init_runtime(&runtime, num_xstreams) != 0) {
        for (int i = 0; i < num_problems; i++) {
            free(problems[i].a);
            free(problems[i].b);
        }
        free(problems);
        fprintf(stderr, "Failed to initialize Argobots runtime.\n");
        return 1;
    }

    if (runtime.use_ws_scheduler) {
        if (runtime.use_cost_aware_scheduler) {
            ws_reset_steal_count();
        } else {
            ws_old_reset_steal_count();
        }
    }

    printf("Running %d Jacobi-3D problems in one Argobots runtime\n", num_problems);
    printf("xstreams=%d threads=%d\n", num_xstreams, num_threads);
    printf("problem_sizes:");
    for (int i = 0; i < num_problems; i++) {
        printf(" %d", problems[i].size);
    }
    printf("\n");

    clock_t cpu_start = clock();
    struct timespec wall_start, wall_end;
    clock_gettime(CLOCK_REALTIME, &wall_start);

    for (int it = 1; it <= DEFAULT_ITMAX; it++) {
        int active = count_active_problems(problems, num_problems);
        int total_chunks = 0;
        for (int p = 0; p < num_problems; p++) {
            int interior_rows = problems[p].size - 2;
            if (!problems[p].converged) {
                total_chunks += (num_threads < interior_rows) ? num_threads : interior_rows;
            }
        }
        if (active == 0 || total_chunks == 0) {
            break;
        }

        ABT_thread *threads =
            (ABT_thread *)calloc((size_t)total_chunks, sizeof(ABT_thread));
        jacobi_chunk_arg_t *chunk_args =
            (jacobi_chunk_arg_t *)calloc((size_t)total_chunks, sizeof(jacobi_chunk_arg_t));
        float *eps_values =
            (float *)calloc((size_t)total_chunks, sizeof(float));
        if (!threads || !chunk_args || !eps_values) {
            free(threads);
            free(chunk_args);
            free(eps_values);
            fprintf(stderr, "Allocation failure for chunk metadata.\n");
            break;
        }

        int chunk_index = 0;
        for (int p = 0; p < num_problems; p++) {
            jacobi_problem_t *problem = &problems[p];
            int interior_rows;
            int chunks;
            int base_rows;
            int remainder;
            int current_row;

            if (problem->converged) {
                continue;
            }

            interior_rows = problem->size - 2;
            chunks = (num_threads < interior_rows) ? num_threads : interior_rows;
            base_rows = interior_rows / chunks;
            remainder = interior_rows % chunks;
            current_row = 1;

            for (int c = 0; c < chunks; c++) {
                int rows = base_rows + (c < remainder ? 1 : 0);
                int pool_id = task_pool_id(p, c, runtime.num_xstreams);
                chunk_args[chunk_index].problem = problem;
                chunk_args[chunk_index].start_i = current_row;
                chunk_args[chunk_index].end_i = current_row + rows;
                chunk_args[chunk_index].phase = JACOBI_PHASE_A;
                chunk_args[chunk_index].eps_local = &eps_values[chunk_index];

                if (runtime.use_cost_aware_scheduler) {
                    ws_push_task_estimate(pool_id,
                                          estimate_chunk_cost(problem->size, rows,
                                                              JACOBI_PHASE_A));
                }
                ABT_thread_create(runtime.pools[pool_id], jacobi_chunk_run,
                                  &chunk_args[chunk_index], ABT_THREAD_ATTR_NULL,
                                  &threads[chunk_index]);
                current_row += rows;
                chunk_index++;
            }
        }

        for (int i = 0; i < total_chunks; i++) {
            ABT_thread_join(threads[i]);
            ABT_thread_free(&threads[i]);
        }

        {
            ABT_thread *reduction_threads =
                (ABT_thread *)calloc((size_t)num_problems * (size_t)num_threads,
                                     sizeof(ABT_thread));
            reduction_chunk_arg_t *reduction_args =
                (reduction_chunk_arg_t *)calloc((size_t)num_problems * (size_t)num_threads,
                                               sizeof(reduction_chunk_arg_t));
            float *reduction_outputs =
                (float *)calloc((size_t)num_problems * (size_t)num_threads,
                                sizeof(float));
            if (!reduction_threads || !reduction_args || !reduction_outputs) {
                free(reduction_threads);
                free(reduction_args);
                free(reduction_outputs);
                free(threads);
                free(chunk_args);
                free(eps_values);
                fprintf(stderr, "Allocation failure for reduction metadata.\n");
                break;
            }

            chunk_index = 0;
            int reduction_task_index = 0;
            for (int p = 0; p < num_problems; p++) {
                jacobi_problem_t *problem = &problems[p];
                int interior_rows;
                int chunks;
                int reduction_chunks;
                int base_chunk_len;
                int remainder;
                int current = 0;

                if (problem->converged) {
                    continue;
                }

                interior_rows = problem->size - 2;
                chunks = (num_threads < interior_rows) ? num_threads : interior_rows;
                reduction_chunks = (chunks < runtime.num_xstreams) ? chunks : runtime.num_xstreams;
                if (reduction_chunks < 1) {
                    reduction_chunks = 1;
                }
                base_chunk_len = chunks / reduction_chunks;
                remainder = chunks % reduction_chunks;

                for (int r = 0; r < reduction_chunks; r++) {
                    int len = base_chunk_len + (r < remainder ? 1 : 0);
                    int pool_id = task_pool_id(p, r + chunks, runtime.num_xstreams);
                    reduction_args[reduction_task_index].input = &eps_values[chunk_index];
                    reduction_args[reduction_task_index].start = current;
                    reduction_args[reduction_task_index].end = current + len;
                    reduction_args[reduction_task_index].output =
                        &reduction_outputs[reduction_task_index];

                    if (runtime.use_cost_aware_scheduler) {
                        ws_push_task_estimate(pool_id, estimate_reduction_cost(len));
                    }
                    ABT_thread_create(runtime.pools[pool_id], reduction_chunk_run,
                                      &reduction_args[reduction_task_index],
                                      ABT_THREAD_ATTR_NULL,
                                      &reduction_threads[reduction_task_index]);
                    current += len;
                    reduction_task_index++;
                }
                chunk_index += chunks;
            }

            for (int i = 0; i < reduction_task_index; i++) {
                ABT_thread_join(reduction_threads[i]);
                ABT_thread_free(&reduction_threads[i]);
            }

            reduction_task_index = 0;
            for (int p = 0; p < num_problems; p++) {
                jacobi_problem_t *problem = &problems[p];
                int interior_rows;
                int chunks;
                int reduction_chunks;
                float eps = 0.0f;

                if (problem->converged) {
                    continue;
                }

                interior_rows = problem->size - 2;
                chunks = (num_threads < interior_rows) ? num_threads : interior_rows;
                reduction_chunks = (chunks < runtime.num_xstreams) ? chunks : runtime.num_xstreams;
                if (reduction_chunks < 1) {
                    reduction_chunks = 1;
                }

                for (int r = 0; r < reduction_chunks; r++) {
                    if (reduction_outputs[reduction_task_index + r] > eps) {
                        eps = reduction_outputs[reduction_task_index + r];
                    }
                }
                problem->final_eps = eps;
                problem->iterations_done = it;
                reduction_task_index += reduction_chunks;
            }

            free(reduction_threads);
            free(reduction_args);
            free(reduction_outputs);
        }

        free(threads);
        free(chunk_args);
        free(eps_values);

        threads = (ABT_thread *)calloc((size_t)total_chunks, sizeof(ABT_thread));
        chunk_args = (jacobi_chunk_arg_t *)calloc((size_t)total_chunks, sizeof(jacobi_chunk_arg_t));
        if (!threads || !chunk_args) {
            free(threads);
            free(chunk_args);
            fprintf(stderr, "Allocation failure for chunk metadata.\n");
            break;
        }

        chunk_index = 0;
        for (int p = 0; p < num_problems; p++) {
            jacobi_problem_t *problem = &problems[p];
            int interior_rows;
            int chunks;
            int base_rows;
            int remainder;
            int current_row;

            if (problem->converged) {
                continue;
            }

            interior_rows = problem->size - 2;
            chunks = (num_threads < interior_rows) ? num_threads : interior_rows;
            base_rows = interior_rows / chunks;
            remainder = interior_rows % chunks;
            current_row = 1;

            for (int c = 0; c < chunks; c++) {
                int rows = base_rows + (c < remainder ? 1 : 0);
                int pool_id = task_pool_id(p, c, runtime.num_xstreams);
                chunk_args[chunk_index].problem = problem;
                chunk_args[chunk_index].start_i = current_row;
                chunk_args[chunk_index].end_i = current_row + rows;
                chunk_args[chunk_index].phase = JACOBI_PHASE_B;
                chunk_args[chunk_index].eps_local = NULL;

                if (runtime.use_cost_aware_scheduler) {
                    ws_push_task_estimate(pool_id,
                                          estimate_chunk_cost(problem->size, rows,
                                                              JACOBI_PHASE_B));
                }
                ABT_thread_create(runtime.pools[pool_id], jacobi_chunk_run,
                                  &chunk_args[chunk_index], ABT_THREAD_ATTR_NULL,
                                  &threads[chunk_index]);
                current_row += rows;
                chunk_index++;
            }
        }

        for (int i = 0; i < total_chunks; i++) {
            ABT_thread_join(threads[i]);
            ABT_thread_free(&threads[i]);
        }

        for (int p = 0; p < num_problems; p++) {
            if (!problems[p].converged && problems[p].final_eps < problems[p].maxeps) {
                problems[p].converged = 1;
            }
        }

        free(threads);
        free(chunk_args);
    }

    clock_t cpu_end = clock();
    clock_gettime(CLOCK_REALTIME, &wall_end);

    long long steal_operations = 0;
    long long stolen_tasks = 0;
    if (runtime.use_ws_scheduler) {
        if (runtime.use_cost_aware_scheduler) {
            steal_operations = ws_get_steal_ops_count();
            stolen_tasks = ws_get_stolen_tasks_count();
        } else {
            steal_operations = ws_old_get_steal_ops_count();
            stolen_tasks = ws_old_get_stolen_tasks_count();
        }
    }

    int success = 1;
    for (int i = 0; i < num_problems; i++) {
        problems[i].checksum = compute_checksum(problems[i].b, problems[i].size);
        problems[i].success = isfinite(problems[i].final_eps) &&
                              isfinite(problems[i].checksum);
        printf("problem[%d]_result: size=%d iterations=%d final_eps=%f checksum=%.6f status=%s\n",
               i, problems[i].size, problems[i].iterations_done, problems[i].final_eps,
               problems[i].checksum, problems[i].success ? "OK" : "FAILED");
        if (!problems[i].success) {
            success = 0;
        }
    }

    double cpu_time_used =
        ((double)(cpu_end - cpu_start)) / (double)CLOCKS_PER_SEC;
    long long real_time_nanoseconds =
        (wall_end.tv_sec - wall_start.tv_sec) * 1000000000LL +
        (wall_end.tv_nsec - wall_start.tv_nsec);

    finalize_runtime(&runtime);

    for (int i = 0; i < num_problems; i++) {
        free(problems[i].a);
        free(problems[i].b);
    }
    free(problems);

    printf(" Multi-Jacobi Benchmark Completed.\n");
    printf(" Problem count      =       %12d\n", num_problems);
    printf(" Threads per task   =       %12d\n", num_threads);
    printf(" Time in seconds    =       %12.2lf\n", cpu_time_used);
    printf(" Real time (nanos)  =       %12lld\n", real_time_nanoseconds);
    printf(" Steal operations   =       %12lld\n", steal_operations);
    printf(" Stolen tasks       =       %12lld\n", stolen_tasks);
    printf(" Verification       =       %12s\n",
           success ? "SUCCESSFUL" : "UNSUCCESSFUL");
    printf(" END OF Multi-Jacobi Benchmark\n");

    return success ? 0 : 2;
}
