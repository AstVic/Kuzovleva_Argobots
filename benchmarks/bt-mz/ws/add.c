#include "header.h"
#include "ws_parallel.h"

/* Исходная функция add, в которой каждый цикл #pragma omp for стал частью ws_part,
 * выполняемой на итерациях [ws_lo, ws_hi). */
void add_part (ou,orhs,nx,nxmax,ny,nz,ws_part,ws_lo,ws_hi)
// beg param
       void *ou;
       void *orhs;
       int nx;
       int nxmax;
       int ny;
       int nz;
       int ws_part;
       long ws_lo;
       long ws_hi;
// end param
{
double (*rhs)[(ny-1)-(0)+1][(nxmax-1)-(0)+1][5] = (double (*)[(ny-1)-(0)+1][(nxmax-1)-(0)+1][5])orhs;
double (*u)[(ny-1)-(0)+1][(nxmax-1)-(0)+1][5] = (double (*)[(ny-1)-(0)+1][(nxmax-1)-(0)+1][5])ou;


//---------------------------------------------------------------------
//---------------------------------------------------------------------

//---------------------------------------------------------------------
//     addition of update to the vector u
//---------------------------------------------------------------------



      int i;
      int j;
      int k;
      int m;

      if (timeron) timer_start (T_ADD);
//$omp do
 
      if (ws_part == 1) do (k , (int)ws_lo, (int)ws_hi - 1,1) {
         do (j , 1, ny-2,1) {
            do (i , 1, nx-2,1) {
               do (m , 1, 5,1) {
                  u[k+0][j+0][i+0][m-1] = u[k+0][j+0][i+0][m-1] + rhs[k+0][j+0][i+0][m-1];
               }
            }
         }
      }
//$omp end do
 
 //#pragma omp end do
      if (timeron) timer_stop (T_ADD);

      return;
}//end

//---------------------------------------------------------------------
//---------------------------------------------------------------------

/* Аргументы add. */
typedef struct {
    void * ou;
    void * orhs;
    int nx;
    int nxmax;
    int ny;
    int nz;
} add_job;

/* Тела параллельных циклов, по одной функции на каждую часть: так история стоимости
 * различает циклы, как если бы каждый был вынесен в свою функцию. */
static void add_body_1(long lo, long hi, void *p)
{
    add_job *j = (add_job *)p;
    add_part(j->ou, j->orhs, j->nx, j->nxmax, j->ny, j->nz, 1, lo, hi);
}

/* add с тем же интерфейсом, что в исходной программе: части выполняются по очереди,
 * каждая - параллельным циклом по своему диапазону (барьер после каждой, как без nowait). */
void add (ou,orhs,nx,nxmax,ny,nz)
// beg param
       void *ou;
       void *orhs;
       int nx;
       int nxmax;
       int ny;
       int nz;
// end param
{
    add_job j;
    j.ou = ou;
    j.orhs = orhs;
    j.nx = nx;
    j.nxmax = nxmax;
    j.ny = ny;
    j.nz = nz;
    ws_parallel_for(1, (nz-2) + 1, add_body_1, &j);
}
