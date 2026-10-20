#include "header.h"

void add (ou,orhs,nx,nxmax,ny,nz)
// beg param
       void *ou;
       void *orhs;
       int nx;
       int nxmax;
       int ny;
       int nz;
// end param add
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
 
#pragma omp for
      do (k , 1, nz-2,1) {
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

