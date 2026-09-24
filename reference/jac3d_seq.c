/* Jacobi-3D, последовательная версия — эталон для расчёта ускорения.
 *
 * Алгоритм, размер сетки, число итераций и проверка результата совпадают с
 * jac3d_argobots/jac3d.c, поэтому время этой программы можно брать как
 * T_sequential в отношении speedup = T_sequential / T_parallel.
 * Исходник взят из ArgobotsReduction/jac3d_pure_c_fixed; добавлены только
 * замер по CLOCK_MONOTONIC и совпадающий формат вывода. */

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#define Max(a, b) ((a) > (b) ? (a) : (b))

/* Задаются при сборке: -DL=192 -DITMAX=10 */
#ifndef L
#define L 384
#endif
#ifndef ITMAX
#define ITMAX 100
#endif

int i, j, k, it;
float eps;
float MAXEPS = 0.5f;

float A[L][L][L];
float B[L][L][L];

int main(int argc, char **argv)
{
    (void)argc;
    (void)argv;

    for (i = 0; i < L; i++)
        for (j = 0; j < L; j++)
            for (k = 0; k < L; k++) {
                A[i][j][k] = 0;
                if (i == 0 || j == 0 || k == 0 || i == L - 1 || j == L - 1 || k == L - 1)
                    B[i][j][k] = 0;
                else
                    B[i][j][k] = 4 + i + j + k;
            }

    clock_t start = clock();
    struct timespec start_real_time, end_real_time;
    struct timespec start_mono_time, end_mono_time;
    clock_gettime(CLOCK_REALTIME, &start_real_time);
    clock_gettime(CLOCK_MONOTONIC, &start_mono_time);

    for (it = 1; it <= ITMAX; it++) {
        eps = 0;
        for (i = 1; i < L - 1; i++)
            for (j = 1; j < L - 1; j++)
                for (k = 1; k < L - 1; k++) {
                    float tmp = fabs(B[i][j][k] - A[i][j][k]);
                    eps = Max(tmp, eps);
                    A[i][j][k] = B[i][j][k];
                }

        for (i = 1; i < L - 1; i++)
            for (j = 1; j < L - 1; j++)
                for (k = 1; k < L - 1; k++)
                    B[i][j][k] = (A[i - 1][j][k] + A[i][j - 1][k] + A[i][j][k - 1] +
                                  A[i][j][k + 1] + A[i][j + 1][k] + A[i + 1][j][k]) / 6.0f;

        if (eps < MAXEPS)
            break;
    }

    clock_t end = clock();
    clock_gettime(CLOCK_REALTIME, &end_real_time);
    clock_gettime(CLOCK_MONOTONIC, &end_mono_time);

    double cpu_time_used = ((double)(end - start)) / CLOCKS_PER_SEC;
    long long real_time_nanoseconds =
        (end_real_time.tv_sec - start_real_time.tv_sec) * 1000000000LL +
        (end_real_time.tv_nsec - start_real_time.tv_nsec);
    long long mono_time_nanoseconds =
        (end_mono_time.tv_sec - start_mono_time.tv_sec) * 1000000000LL +
        (end_mono_time.tv_nsec - start_mono_time.tv_nsec);

    /* Формат вывода совпадает с jac3d_argobots/jac3d.c, чтобы скрипты
     * разбирали обе программы одинаково. */
    printf(" Jacobi3D Benchmark Completed.\n");
    printf(" Size              = %4d x %4d x %4d\n", L, L, L);
    printf(" Iterations        =       %12d\n", ITMAX);
    printf(" Time in seconds   =       %12.2lf\n", cpu_time_used);
    printf(" Real time (nanos) =       %12lld\n", real_time_nanoseconds);
    printf(" Mono time (nanos) =       %12lld\n", mono_time_nanoseconds);
    printf(" Grid size         =       %12d\n", L);
    printf(" Operation type    =     floating point\n");
    printf(" Steal operations  =       %12d\n", 0);
    printf(" Stolen tasks      =       %12d\n", 0);
    printf(" Verification      =       %12s\n",
           (fabs(eps - 5.058044) < 1e-4 ? "SUCCESSFUL" : "UNSUCCESSFUL"));
    printf(" EPS = %f\n", eps);
    printf(" END OF Jacobi3D Benchmark\n");
    return 0;
}
