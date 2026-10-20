#include "header.h"

void initialize (ou,nx,nxmax,ny,nz)
// beg param
       void *ou;
       int nx;
       int nxmax;
       int ny;
       int nz;
// end param initialize
{
double (*u)[(ny-1)-(0)+1][(nxmax-1)-(0)+1][5] = (double (*)[(ny-1)-(0)+1][(nxmax-1)-(0)+1][5])ou;


//---------------------------------------------------------------------
//---------------------------------------------------------------------

//---------------------------------------------------------------------
//     This subroutine initializes the field variable u using 
//     tri-linear transfinite interpolation of the boundary values     
//---------------------------------------------------------------------


      int i;
      int j;
      int k;
      int m;
      int ix;
      int iy;
      int iz;
      double xi;
      double eta;
      double zeta;
      double pface[2][3][5];
      double pxi;
      double peta;
      double pzeta;
      double temp[5];

//---------------------------------------------------------------------
//  Later (in compute_rhs) we compute 1/u for every element. A few of 
//  the corner elements are not used, but it convenient (and faster) 
//  to compute the whole thing with a simple loop. Make sure those 
//  values are nonzero by initializing the whole thing here. 
//---------------------------------------------------------------------
//$omp do schedule(static)
 
#pragma omp for schedule(static) nowait
      do (k , 0, nz-1,1) {
         do (j , 0, ny-1,1) {
            do (i , 0, nx-1,1) {
               do (m , 1, 5,1) {
                  u[k+0][j+0][i+0][m-1] = 1.0;
               }
            }
         }
      }
//$omp end do nowait
 
 //#pragma omp end do nowait
//---------------------------------------------------------------------



//---------------------------------------------------------------------
//     first store the "interpolated" values everywhere on the zone    
//---------------------------------------------------------------------

//$omp do schedule(static)
 
#pragma omp for schedule(static) nowait
      do (k , 0, nz-1,1) {
         zeta = (double) (k) *dnzm1;
         do (j , 0, ny-1,1) {
            eta = (double) (j) *dnym1;
            do (i , 0, nx-1,1) {
               xi = (double) (i) *dnxm1;

               do (ix , 1, 2,1) {
                  exact_solution ((double) (ix-1),eta,zeta,&pface[ix-1][0][0]);
               }

               do (iy , 1, 2,1) {
                  exact_solution (xi,(double) (iy-1) ,zeta,&pface[iy-1][1][0]);
               }

               do (iz , 1, 2,1) {
                  exact_solution (xi,eta,(double) (iz-1),&pface[iz-1][2][0]);
               }

               do (m , 1, 5,1) {
                  pxi   = xi * pface[1][0][m-1] +(1.0e0-xi)   * pface[0][0][m-1];
                  peta  = eta * pface[1][1][m-1] +(1.0e0-eta)  * pface[0][1][m-1];
                  pzeta = zeta * pface[1][2][m-1] +(1.0e0-zeta) * pface[0][2][m-1];

                  u[k+0][j+0][i+0][m-1] = pxi + peta + pzeta - pxi*peta - pxi*pzeta - peta*pzeta + pxi*peta*pzeta;

               }
            }
         }
      }
//$omp end do nowait
 
 //#pragma omp end do nowait

//---------------------------------------------------------------------
//     now store the exact values on the boundaries        
//---------------------------------------------------------------------

//---------------------------------------------------------------------
//     west face                                                  
//---------------------------------------------------------------------
      i = 0;
      xi = 0.0e0;
//$omp do schedule(static)
 
#pragma omp for schedule(static) nowait
      do (k , 0, nz-1,1) {
         zeta = (double) (k) *dnzm1;
         do (j , 0, ny-1,1) {
            eta = (double) (j) *dnym1;
            exact_solution (xi,eta,zeta, temp);
            do (m , 1, 5,1) {
               u[k+0][j+0][i+0][m-1] = temp[m-1];
            }
         }
      }
//$omp end do nowait
 
 //#pragma omp end do nowait

//---------------------------------------------------------------------
//     east face                                                      
//---------------------------------------------------------------------

      i = nx-1;
      xi = 1.0e0;
//$omp do schedule(static)
 
#pragma omp for schedule(static) nowait
      do (k , 0, nz-1,1) {
         zeta = (double) (k) *dnzm1;
         do (j , 0, ny-1,1) {
            eta = (double) (j) *dnym1;
            exact_solution (xi,eta,zeta, temp);
            do (m , 1, 5,1) {
               u[k+0][j+0][i+0][m-1] = temp[m-1];
            }
         }
      }
//$omp end do nowait
 
 //#pragma omp end do nowait

//---------------------------------------------------------------------
//     south face                                                 
//---------------------------------------------------------------------
      j = 0;
      eta = 0.0e0;
//$omp do schedule(static)
 
#pragma omp for schedule(static) nowait
      do (k , 0, nz-1,1) {
         zeta = (double) (k) *dnzm1;
         do (i , 0, nx-1,1) {
            xi = (double) (i) *dnxm1;
            exact_solution (xi,eta,zeta, temp);
            do (m , 1, 5,1) {
               u[k+0][j+0][i+0][m-1] = temp[m-1];
            }
         }
      }
//$omp end do nowait
 
 //#pragma omp end do nowait


//---------------------------------------------------------------------
//     north face                                    
//---------------------------------------------------------------------
      j = ny-1;
      eta = 1.0e0;
//$omp do schedule(static)
 
#pragma omp for schedule(static)
      do (k , 0, nz-1,1) {
         zeta = (double) (k) *dnzm1;
         do (i , 0, nx-1,1) {
            xi = (double) (i) *dnxm1;
            exact_solution (xi,eta,zeta, temp);
            do (m , 1, 5,1) {
               u[k+0][j+0][i+0][m-1] = temp[m-1];
            }
         }
      }
//$omp end do
 
 //#pragma omp end do

//---------------------------------------------------------------------
//     bottom face                                       
//---------------------------------------------------------------------
      k = 0;
      zeta = 0.0e0;
//$omp do schedule(static)
 
#pragma omp for schedule(static) nowait
      do (j , 0, ny-1,1) {
         eta = (double) (j) *dnym1;
         do (i ,0, nx-1,1) {
            xi = (double) (i) *dnxm1;
            exact_solution (xi,eta,zeta, temp);
            do (m , 1, 5,1) {
               u[k+0][j+0][i+0][m-1] = temp[m-1];
            }
         }
      }
//$omp end do nowait
 
 //#pragma omp end do nowait

//---------------------------------------------------------------------
//     top face     
//---------------------------------------------------------------------
      k = nz-1;
      zeta = 1.0e0;
//$omp do schedule(static)
 
#pragma omp for schedule(static)
      do (j , 0, ny-1,1) {
         eta = (double) (j) *dnym1;
         do (i ,0, nx-1,1) {
            xi = (double) (i) *dnxm1;
            exact_solution (xi,eta,zeta, temp);
            do (m , 1, 5,1) {
               u[k+0][j+0][i+0][m-1] = temp[m-1];
            }
         }
      }
//$omp end do
 
 //#pragma omp end do

      return;
}//end


//---------------------------------------------------------------------
//---------------------------------------------------------------------

void lhsinit (lhs,size)
// beg param
int size;
       double lhs[(size)-(0)+1][3][5][5];
// end param
{
//      implicit none

//---------------------------------------------------------------------
//---------------------------------------------------------------------

      int i;
      int m;
      int n;

      i = size;
//---------------------------------------------------------------------
//     zero the whole left hand side for starters
//---------------------------------------------------------------------
      do (m , 1, 5,1) {
         do (n , 1, 5,1) {
            lhs[0+0][0][n-1][m-1] = 0.0e0;
            lhs[0+0][1][n-1][m-1] = 0.0e0;
            lhs[0+0][2][n-1][m-1] = 0.0e0;
            lhs[i+0][0][n-1][m-1] = 0.0e0;
            lhs[i+0][1][n-1][m-1] = 0.0e0;
            lhs[i+0][2][n-1][m-1] = 0.0e0;
         }
      }

//---------------------------------------------------------------------
//     next, set all diagonal values to 1. This is overkill, but convenient
//---------------------------------------------------------------------
      do (m , 1, 5,1) {
         lhs[0+0][1][m-1][m-1] = 1.0e0;
         lhs[i+0][1][m-1][m-1] = 1.0e0;
      }

      return;
}//end




//>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>c
//
