#include "header.h"

void error_norm (rms,ou,nx,nxmax,ny,nz)
// beg param
       double rms[5];
       void *ou;
       int nx;
       int nxmax;
       int ny;
       int nz;
// end param error_norm
{
double (*u)[(ny-1)-(0)+1][(nxmax-1)-(0)+1][5] = (double (*)[(ny-1)-(0)+1][(nxmax-1)-(0)+1][5])ou;


//---------------------------------------------------------------------
//---------------------------------------------------------------------

//---------------------------------------------------------------------
//     this function computes the norm of the difference between the
//     computed solution and the exact solution
//---------------------------------------------------------------------



      int i;
      int j;
      int k;
      int m;
      double xi;
      double eta;
      double zeta;
      double u_exact[5];
      double add;
      double rms_loc[5];

//$omp master
 
#pragma omp master
{
      do (m , 1, 5,1) {
         rms[m-1] = 0.0e0;
      }
//$omp end master
 
} //#pragma omp end master
//$omp barrier
 
#pragma omp barrier

      do (m,1,5,1) {
         rms_loc[m-1]=0.0e0;
      }
//$omp do
 
#pragma omp for nowait
      do (k , 0, nz-1,1) {
         zeta = (double) (k) *dnzm1;
         do (j , 0, ny-1,1) {
            eta = (double) (j) *dnym1;
            do (i , 0, nx-1,1) {
               xi = (double) (i) *dnxm1;
               exact_solution (xi,eta,zeta, u_exact);

               do (m , 1, 5,1) {
                  add = u[k+0][j+0][i+0][m-1]-u_exact[m-1];
                  rms_loc[m-1] = rms_loc[m-1] + add*add;
               }
            }
         }
      }
//$omp end do nowait
 
 //#pragma omp end do nowait
      do (m,1,5,1) {
//$omp atomic
 
#pragma omp atomic
         rms[m-1]=rms[m-1]+rms_loc[m-1];
      }
//$omp barrier
 
#pragma omp barrier

//$omp master
 
#pragma omp master
{
      do (m , 1, 5,1) {
         rms[m-1] = rms[m-1] /((double) (nz-2)*(double) (ny-2)*(double) (nx-2));
         rms[m-1] = sqrt (rms[m-1]);
      }
//$omp end master
 
} //#pragma omp end master

      return;
}//end


//---------------------------------------------------------------------
//---------------------------------------------------------------------

void rhs_norm (rms,orhs,nx,nxmax,ny,nz)
// beg param
       double rms[5];
       void *orhs;
       int nx;
       int nxmax;
       int ny;
       int nz;
// end param rhs_norm
{
double (*rhs)[(ny-1)-(0)+1][(nxmax-1)-(0)+1][5] = (double (*)[(ny-1)-(0)+1][(nxmax-1)-(0)+1][5])orhs;


//---------------------------------------------------------------------
//---------------------------------------------------------------------



      int i;
      int j;
      int k;
      int m;
      double add;
      double rms_loc[5];

//$omp master
 
#pragma omp master
{
      do (m , 1, 5,1) {
         rms[m-1] = 0.0e0;
      }
//$omp end master
 
} //#pragma omp end master
//$omp barrier
 
#pragma omp barrier

      do (m,1,5,1) {
         rms_loc[m-1]=0.0e0;
      }
//$omp do
 
#pragma omp for nowait
      do (k , 1, nz-2,1) {
         do (j , 1, ny-2,1) {
            do (i , 1, nx-2,1) {
               do (m , 1, 5,1) {
                  add = rhs[k+0][j+0][i+0][m-1];
                  rms_loc[m-1] = rms_loc[m-1] + add*add;
               }
            }
         }
      }
//$omp end do nowait
 
 //#pragma omp end do nowait
      do (m,1,5,1) {
//$omp atomic
 
#pragma omp atomic
        rms[m-1]=rms[m-1]+rms_loc[m-1];
      }
//$omp barrier
 
#pragma omp barrier

//$omp master
 
#pragma omp master
{
      do (m , 1, 5,1) {
        rms[m-1] = rms[m-1] /((double) (nz-2)*(double) (ny-2)*(double) (nx-2));
        rms[m-1] = sqrt (rms[m-1]);
      }
//$omp end master
 
} //#pragma omp end master

      return;
}//end



//---------------------------------------------------------------------
//---------------------------------------------------------------------

