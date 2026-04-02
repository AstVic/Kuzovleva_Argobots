#include <abt.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "../workstealing_scheduler/abt_workstealing_scheduler.h"
#include "../workstealing_scheduler/abt_workstealing_scheduler_cost_aware.h"

#define DEFAULT_XSTREAMS 4
#define DEFAULT_ITMAX 100
#define DEFAULT_MAXEPS 0.5f
#define REFERENCE_GRID_SIZE 384
#define REFERENCE_BEST_CHUNKS 80

typedef enum {
    JACOBI_PHASE_A = 0,
    JACOBI_PHASE_B = 1,
    JACOBI_PHASE_REDUCTION = 2
} jacobi_phase_t;

/* Общий контекст одного Argobots runtime: execution streams, пулы и
 * выбранный режим планировщика. Все задачи Якоби используют его совместно. */
typedef struct {
    ABT_xstream *xstreams;
    ABT_pool *pools;
    ABT_sched *scheds;
    int num_xstreams;
    int use_ws_scheduler;
    int use_cost_aware_scheduler;
} runtime_context_t;

/* Полное состояние одной независимой задачи Якоби: размеры сетки, массивы,
 * параметры сходимости и ссылка на общий runtime. */
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
    int problem_id;
    int num_chunks;
    runtime_context_t *runtime;
} jacobi_problem_t;

/* Аргументы одного вычислительного чанка. Чанк покрывает диапазон строк
 * по оси i и выполняет фазу A или B для конкретной задачи Якоби. */
typedef struct {
    jacobi_problem_t *problem;
    int start_i;
    int end_i;
    jacobi_phase_t phase;
    float *eps_local;
} jacobi_chunk_arg_t;

/* Аргументы одной подзадачи редукции, которая считает частичный максимум
 * по фрагменту массива локальных eps-значений. */
typedef struct {
    const float *input;
    int start;
    int end;
    float *output;
} reduction_chunk_arg_t;

/* Читает переменную окружения и определяет, какой планировщик должен
 * использоваться в текущем runtime. */
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

/* Преобразует координаты (i, j, k) в линейный индекс плоского массива,
 * в котором хранится кубическая сетка. */
static size_t cell_index(int n, int i, int j, int k)
{
    return ((size_t)i * (size_t)n + (size_t)j) * (size_t)n + (size_t)k;
}

/* Оценивает стоимость вычислительного чанка для cost-aware планировщика. */
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

/* Оценивает стоимость подзадачи редукции по числу обрабатываемых значений. */
static double estimate_reduction_cost(int num_values)
{
    return (double)num_values;
}

/* Выбирает пул для новой подзадачи. Все мелкие задачи от всех Якоби
 * складываются в общие пулы одного runtime. */
static int task_pool_id(int problem_id, int chunk_id, int num_xstreams)
{
    /* Все чанки от всех задач Якоби используют общие пулы runtime.
     * Смещение по problem_id помогает естественно перемешивать задачи. */
    return (problem_id + chunk_id) % num_xstreams;
}

/* Инициализирует массивы одной задачи начальными значениями метода Якоби. */
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

/* Считает контрольную сумму итоговой сетки для печати и быстрой проверки. */
static double compute_checksum(const float *grid, int n)
{
    double sum = 0.0;
    size_t total = (size_t)n * (size_t)n * (size_t)n;
    for (size_t i = 0; i < total; i++) {
        sum += (double)grid[i];
    }
    return sum;
}

/* Выполняет один вычислительный чанк: в фазе A обновляет a и считает
 * локальный eps, в фазе B обновляет b. */
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

/* Выполняет один reduction-чанк и возвращает частичный максимум. */
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

/* Создаёт один общий runtime: xstreams, пулы и нужные scheduler-ы.
 * В этом runtime затем работают все top-level и дочерние задачи. */
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

/* Корректно завершает runtime и освобождает все связанные ресурсы. */
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

/* Вычисляет число чанков для задачи пропорционально её размеру.
 * В качестве эталона используется лучшая конфигурация одиночного Jacobi:
 * 80 чанков для сетки размера 384. */
static int scaled_problem_chunks(int size)
{
    int interior_rows = size - 2;
    int chunks = (size * REFERENCE_BEST_CHUNKS + REFERENCE_GRID_SIZE / 2) /
                 REFERENCE_GRID_SIZE;

    if (chunks < 1) {
        chunks = 1;
    }
    if (chunks > interior_rows) {
        chunks = interior_rows;
    }
    return chunks;
}

/* Печатает формат запуска multi-runtime сценария. */
static void print_usage(const char *prog)
{
    fprintf(stderr, "Usage: %s <xstreams> <size1> [size2 ...]\n", prog);
    fprintf(stderr, "Example: %s 4 384 320 256 192\n", prog);
}

/* Возвращает фактическое число chunk-task для одной задачи: не больше,
 * чем число внутренних строк сетки. */
static int problem_chunk_count(const jacobi_problem_t *problem)
{
    int interior_rows = problem->size - 2;
    return (problem->num_chunks < interior_rows) ? problem->num_chunks : interior_rows;
}

/* Создаёт все чанки одной фазы для конкретной задачи Якоби и раскладывает
 * их по общим пулам runtime. */
static int launch_problem_phase(jacobi_problem_t *problem,
                                jacobi_phase_t phase,
                                ABT_thread *threads,
                                jacobi_chunk_arg_t *chunk_args,
                                float *eps_values)
{
    runtime_context_t *runtime = problem->runtime;
    int chunks = problem_chunk_count(problem);
    int interior_rows = problem->size - 2;
    int base_rows = interior_rows / chunks;
    int remainder = interior_rows % chunks;
    int current_row = 1;

    /* chunks здесь означает число chunk-task, на которые режется одна
     * конкретная задача Якоби. */
    for (int c = 0; c < chunks; c++) {
        int rows = base_rows + (c < remainder ? 1 : 0);
        int pool_id = task_pool_id(problem->problem_id, c, runtime->num_xstreams);
        chunk_args[c].problem = problem;
        chunk_args[c].start_i = current_row;
        chunk_args[c].end_i = current_row + rows;
        chunk_args[c].phase = phase;
        chunk_args[c].eps_local = (phase == JACOBI_PHASE_A) ? &eps_values[c] : NULL;

        if (runtime->use_cost_aware_scheduler) {
            ws_push_task_estimate(pool_id,
                                  estimate_chunk_cost(problem->size, rows, phase));
        }
        ABT_thread_create(runtime->pools[pool_id], jacobi_chunk_run,
                          &chunk_args[c], ABT_THREAD_ATTR_NULL, &threads[c]);
        current_row += rows;
    }

    return chunks;
}

/* Выполняет локальную редукцию одной задачи Якоби: запускает reduction-task,
 * ждёт только их и сохраняет итоговый eps этой задачи. */
static int run_problem_reduction(jacobi_problem_t *problem,
                                 float *eps_values)
{
    runtime_context_t *runtime = problem->runtime;
    int chunks = problem_chunk_count(problem);
    int reduction_chunks = (chunks < runtime->num_xstreams) ? chunks : runtime->num_xstreams;
    int base_chunk_len;
    int remainder;
    int current = 0;
    float eps = 0.0f;
    ABT_thread *reduction_threads;
    reduction_chunk_arg_t *reduction_args;
    float *reduction_outputs;

    if (reduction_chunks < 1) {
        reduction_chunks = 1;
    }

    reduction_threads = (ABT_thread *)calloc((size_t)reduction_chunks, sizeof(ABT_thread));
    reduction_args = (reduction_chunk_arg_t *)calloc((size_t)reduction_chunks,
                                                     sizeof(reduction_chunk_arg_t));
    reduction_outputs = (float *)calloc((size_t)reduction_chunks, sizeof(float));
    if (!reduction_threads || !reduction_args || !reduction_outputs) {
        free(reduction_threads);
        free(reduction_args);
        free(reduction_outputs);
        return -1;
    }

    base_chunk_len = chunks / reduction_chunks;
    remainder = chunks % reduction_chunks;

    for (int r = 0; r < reduction_chunks; r++) {
        int len = base_chunk_len + (r < remainder ? 1 : 0);
        int pool_id = task_pool_id(problem->problem_id, chunks + r, runtime->num_xstreams);
        reduction_args[r].input = eps_values;
        reduction_args[r].start = current;
        reduction_args[r].end = current + len;
        reduction_args[r].output = &reduction_outputs[r];

        if (runtime->use_cost_aware_scheduler) {
            ws_push_task_estimate(pool_id, estimate_reduction_cost(len));
        }
        ABT_thread_create(runtime->pools[pool_id], reduction_chunk_run,
                          &reduction_args[r], ABT_THREAD_ATTR_NULL,
                          &reduction_threads[r]);
        current += len;
    }

    /* Редукция локальна для одной задачи Якоби: ожидание идёт только по её
     * частичным максимумам, без глобального барьера между задачами. */
    for (int r = 0; r < reduction_chunks; r++) {
        ABT_thread_join(reduction_threads[r]);
        ABT_thread_free(&reduction_threads[r]);
        if (reduction_outputs[r] > eps) {
            eps = reduction_outputs[r];
        }
    }

    problem->final_eps = eps;

    free(reduction_threads);
    free(reduction_args);
    free(reduction_outputs);
    return 0;
}

/* Top-level ULT одной задачи Якоби. Он полностью управляет её жизненным
 * циклом: фаза A, локальная редукция, фаза B и переход к следующей итерации. */
static void run_one_jacobi_problem(void *arg)
{
    jacobi_problem_t *problem = (jacobi_problem_t *)arg;
    int chunks = problem_chunk_count(problem);
    ABT_thread *threads =
        (ABT_thread *)calloc((size_t)chunks, sizeof(ABT_thread));
    jacobi_chunk_arg_t *chunk_args =
        (jacobi_chunk_arg_t *)calloc((size_t)chunks, sizeof(jacobi_chunk_arg_t));
    float *eps_values =
        (float *)calloc((size_t)chunks, sizeof(float));

    problem->success = 0;
    problem->converged = 0;
    problem->iterations_done = 0;
    problem->final_eps = 0.0f;

    if (!threads || !chunk_args || !eps_values) {
        free(threads);
        free(chunk_args);
        free(eps_values);
        return;
    }

    for (int it = 1; it <= problem->itmax; it++) {
        int launched_chunks;

        memset(threads, 0, (size_t)chunks * sizeof(ABT_thread));
        memset(chunk_args, 0, (size_t)chunks * sizeof(jacobi_chunk_arg_t));
        memset(eps_values, 0, (size_t)chunks * sizeof(float));

        launched_chunks = launch_problem_phase(problem, JACOBI_PHASE_A,
                                               threads, chunk_args, eps_values);
        for (int c = 0; c < launched_chunks; c++) {
            ABT_thread_join(threads[c]);
            ABT_thread_free(&threads[c]);
        }

        if (run_problem_reduction(problem, eps_values) != 0) {
            free(threads);
            free(chunk_args);
            free(eps_values);
            return;
        }

        memset(threads, 0, (size_t)chunks * sizeof(ABT_thread));
        memset(chunk_args, 0, (size_t)chunks * sizeof(jacobi_chunk_arg_t));
        launch_problem_phase(problem, JACOBI_PHASE_B, threads, chunk_args, eps_values);
        for (int c = 0; c < launched_chunks; c++) {
            ABT_thread_join(threads[c]);
            ABT_thread_free(&threads[c]);
        }

        problem->iterations_done = it;
        if (problem->final_eps < problem->maxeps) {
            problem->converged = 1;
            break;
        }
    }

    problem->checksum = compute_checksum(problem->b, problem->size);
    problem->success = isfinite(problem->final_eps) && isfinite(problem->checksum);

    free(threads);
    free(chunk_args);
    free(eps_values);
}

/* Точка входа сценария multi-runtime. Создаёт несколько независимых задач
 * Якоби в одном runtime и ждёт завершения каждой top-level ULT. */
int main(int argc, char **argv)
{
    if (argc < 3) {
        print_usage(argv[0]);
        return 1;
    }

    int num_xstreams = atoi(argv[1]);
    if (num_xstreams <= 0) {
        num_xstreams = DEFAULT_XSTREAMS;
    }

    int num_problems = argc - 2;
    jacobi_problem_t *problems =
        (jacobi_problem_t *)calloc((size_t)num_problems, sizeof(jacobi_problem_t));
    runtime_context_t runtime = {0};
    if (!problems) {
        fprintf(stderr, "Allocation failure for problem metadata.\n");
        return 1;
    }

    for (int i = 0; i < num_problems; i++) {
        int size = atoi(argv[i + 2]);
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
        problems[i].problem_id = i;
        problems[i].num_chunks = scaled_problem_chunks(size);
        problems[i].runtime = &runtime;

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
    printf("xstreams=%d\n", num_xstreams);
    printf("problem_sizes:");
    for (int i = 0; i < num_problems; i++) {
        printf(" %d", problems[i].size);
    }
    printf("\n");
    printf("problem_chunks:");
    for (int i = 0; i < num_problems; i++) {
        printf(" %d", problems[i].num_chunks);
    }
    printf("\n");

    clock_t cpu_start = clock();
    struct timespec wall_start, wall_end;
    clock_gettime(CLOCK_REALTIME, &wall_start);
    {
        ABT_thread *problem_threads =
            (ABT_thread *)calloc((size_t)num_problems, sizeof(ABT_thread));
        if (!problem_threads) {
            fprintf(stderr, "Allocation failure for top-level Jacobi tasks.\n");
            finalize_runtime(&runtime);
            for (int i = 0; i < num_problems; i++) {
                free(problems[i].a);
                free(problems[i].b);
            }
            free(problems);
            return 1;
        }

        /* Каждый top-level ULT отвечает за одну полную задачу Якоби.
         * Он создаёт чанки и reduction-task только для своей задачи, но все
         * дочерние задачи работают в тех же общих пулах runtime. */
        for (int i = 0; i < num_problems; i++) {
            int pool_id = i % runtime.num_xstreams;
            ABT_thread_create(runtime.pools[pool_id], run_one_jacobi_problem,
                              &problems[i], ABT_THREAD_ATTR_NULL,
                              &problem_threads[i]);
        }

        for (int i = 0; i < num_problems; i++) {
            ABT_thread_join(problem_threads[i]);
            ABT_thread_free(&problem_threads[i]);
        }

        free(problem_threads);
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
    printf(" Time in seconds    =       %12.2lf\n", cpu_time_used);
    printf(" Real time (nanos)  =       %12lld\n", real_time_nanoseconds);
    printf(" Steal operations   =       %12lld\n", steal_operations);
    printf(" Stolen tasks       =       %12lld\n", stolen_tasks);
    printf(" Verification       =       %12s\n",
           success ? "SUCCESSFUL" : "UNSUCCESSFUL");
    printf(" END OF Multi-Jacobi Benchmark\n");

    return success ? 0 : 2;
}
