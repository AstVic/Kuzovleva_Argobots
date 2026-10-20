#include "header.h"
#include "ws_parallel.h"

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

typedef struct {
    void * ou;
    void * orhs;
    int nx;
    int nxmax;
    int ny;
    int nz;
    int part;
} add_job;

static void add_body(long lo, long hi, void *p)
{
    add_job *j = (add_job *)p;
    add_part(j->ou, j->orhs, j->nx, j->nxmax, j->ny, j->nz, j->part, lo, hi);
}

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
    j.part = 1;
    ws_parallel_for(1, (nz-2) + 1, add_body, &j);
}
