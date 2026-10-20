#include "header.h"

void x_solve (orho_i,oqs,osquare,ou,orhs,nx,nxmax,ny,nz)
// beg param
       void *orho_i;
       void *oqs;
       void *osquare;
       void *ou;
       void *orhs;
       int nx;
       int nxmax;
       int ny;
       int nz;
// end param x_solve
{
double (*rhs)[(ny-1)-(0)+1][(nxmax-1)-(0)+1][5] = (double (*)[(ny-1)-(0)+1][(nxmax-1)-(0)+1][5])orhs;
double (*u)[(ny-1)-(0)+1][(nxmax-1)-(0)+1][5] = (double (*)[(ny-1)-(0)+1][(nxmax-1)-(0)+1][5])ou;
double (*square)[(ny-1)-(0)+1][(nxmax-1)-(0)+1] = (double (*)[(ny-1)-(0)+1][(nxmax-1)-(0)+1])osquare;
double (*qs)[(ny-1)-(0)+1][(nxmax-1)-(0)+1] = (double (*)[(ny-1)-(0)+1][(nxmax-1)-(0)+1])oqs;
double (*rho_i)[(ny-1)-(0)+1][(nxmax-1)-(0)+1] = (double (*)[(ny-1)-(0)+1][(nxmax-1)-(0)+1])orho_i;


//---------------------------------------------------------------------
//---------------------------------------------------------------------

//---------------------------------------------------------------------
//     
//     Performs line solves in X direction by first factoring
//     the block-tridiagonal matrix into an upper triangular matrix, 
//     and then performing back substitution to solve for the unknow
//     vectors of each line.  
//     
//     Make sure we treat elements zero to cell_size in the direction
//     of the sweep.
//     
//---------------------------------------------------------------------

#include "work_lhs.h"


      int i;
      int j;
      int k;
      int m;
      int n;
      int isize;

//---------------------------------------------------------------------
//---------------------------------------------------------------------

      if (timeron) timer_start (T_XSOLVE);

//---------------------------------------------------------------------
//---------------------------------------------------------------------

//---------------------------------------------------------------------
//     This function computes the left hand side in the xi-direction
//---------------------------------------------------------------------

      isize = nx-1;

//---------------------------------------------------------------------
//     determine a (labeled f) and n jacobians
//---------------------------------------------------------------------
//$omp do schedule(static)
 
#pragma omp for schedule(static) nowait
      do (k , 1, nz-2,1) {
         do (j , 1, ny-2,1) {
            do (i , 0, isize,1) {

               tmp1 = rho_i[k+0][j+0][i+0];
               tmp2 = tmp1 * tmp1;
               tmp3 = tmp1 * tmp2;
//---------------------------------------------------------------------
//     
//---------------------------------------------------------------------
               fjac[i+0][0][0] = 0.e0;
               fjac[i+0][1][0] = 1.e0;
               fjac[i+0][2][0] = 0.e0;
               fjac[i+0][3][0] = 0.e0;
               fjac[i+0][4][0] = 0.e0;

               fjac[i+0][0][1] = -(u[k+0][j+0][i+0][1] * tmp2 *               u[k+0][j+0][i+0][1])              + c2 * qs[k+0][j+0][i+0];
               fjac[i+0][1][1] =( 2.e0 - c2 )              *( u[k+0][j+0][i+0][1] / u[k+0][j+0][i+0][0] );
               fjac[i+0][2][1] = - c2 *( u[k+0][j+0][i+0][2] * tmp1 );
               fjac[i+0][3][1] = - c2 *( u[k+0][j+0][i+0][3] * tmp1 );
               fjac[i+0][4][1] = c2;

               fjac[i+0][0][2] = -( u[k+0][j+0][i+0][1]*u[k+0][j+0][i+0][2] ) * tmp2;
               fjac[i+0][1][2] = u[k+0][j+0][i+0][2] * tmp1;
               fjac[i+0][2][2] = u[k+0][j+0][i+0][1] * tmp1;
               fjac[i+0][3][2] = 0.e0;
               fjac[i+0][4][2] = 0.e0;

               fjac[i+0][0][3] = -( u[k+0][j+0][i+0][1]*u[k+0][j+0][i+0][3] ) * tmp2;
               fjac[i+0][1][3] = u[k+0][j+0][i+0][3] * tmp1;
               fjac[i+0][2][3] = 0.e0;
               fjac[i+0][3][3] = u[k+0][j+0][i+0][1] * tmp1;
               fjac[i+0][4][3] = 0.e0;

               fjac[i+0][0][4] =( c2 * 2.0e0 * square[k+0][j+0][i+0]              - c1 * u[k+0][j+0][i+0][4] )              *( u[k+0][j+0][i+0][1] * tmp2 );
               fjac[i+0][1][4] = c1 *  u[k+0][j+0][i+0][4] * tmp1               - c2 *( u[k+0][j+0][i+0][1]*u[k+0][j+0][i+0][1] * tmp2 + qs[k+0][j+0][i+0] );
               fjac[i+0][2][4] = - c2 *( u[k+0][j+0][i+0][2]*u[k+0][j+0][i+0][1] )              * tmp2;
               fjac[i+0][3][4] = - c2 *( u[k+0][j+0][i+0][3]*u[k+0][j+0][i+0][1] )              * tmp2;
               fjac[i+0][4][4] = c1 *( u[k+0][j+0][i+0][1] * tmp1 );

               njac[i+0][0][0] = 0.e0;
               njac[i+0][1][0] = 0.e0;
               njac[i+0][2][0] = 0.e0;
               njac[i+0][3][0] = 0.e0;
               njac[i+0][4][0] = 0.e0;

               njac[i+0][0][1] = - con43 * c3c4 * tmp2 * u[k+0][j+0][i+0][1];
               njac[i+0][1][1] =   con43 * c3c4 * tmp1;
               njac[i+0][2][1] =   0.e0;
               njac[i+0][3][1] =   0.e0;
               njac[i+0][4][1] =   0.e0;

               njac[i+0][0][2] = - c3c4 * tmp2 * u[k+0][j+0][i+0][2];
               njac[i+0][1][2] =   0.e0;
               njac[i+0][2][2] =   c3c4 * tmp1;
               njac[i+0][3][2] =   0.e0;
               njac[i+0][4][2] =   0.e0;

               njac[i+0][0][3] = - c3c4 * tmp2 * u[k+0][j+0][i+0][3];
               njac[i+0][1][3] =   0.e0;
               njac[i+0][2][3] =   0.e0;
               njac[i+0][3][3] =   c3c4 * tmp1;
               njac[i+0][4][3] =   0.e0;

               njac[i+0][0][4] = -( con43 * c3c4              - c1345 ) * tmp3 *((u[k+0][j+0][i+0][1])*(u[k+0][j+0][i+0][1]))              -( c3c4 - c1345 ) * tmp3 *((u[k+0][j+0][i+0][2])*(u[k+0][j+0][i+0][2]))              -( c3c4 - c1345 ) * tmp3 *((u[k+0][j+0][i+0][3])*(u[k+0][j+0][i+0][3]))              - c1345 * tmp2 * u[k+0][j+0][i+0][4];

               njac[i+0][1][4] =( con43 * c3c4              - c1345 ) * tmp2 * u[k+0][j+0][i+0][1];
               njac[i+0][2][4] =( c3c4 - c1345 ) * tmp2 * u[k+0][j+0][i+0][2];
               njac[i+0][3][4] =( c3c4 - c1345 ) * tmp2 * u[k+0][j+0][i+0][3];
               njac[i+0][4][4] =( c1345 ) * tmp1;

            }
//---------------------------------------------------------------------
//     now jacobians set, so form left hand side in x direction
//---------------------------------------------------------------------
            lhsinit (lhs,isize);
            do (i , 1, isize-1,1) {

               tmp1 = dt * tx1;
               tmp2 = dt * tx2;

               lhs[i+0][AA-1][0][0] = - tmp2 * fjac[i-1+0][0][0]              - tmp1 * njac[i-1+0][0][0]              - tmp1 * dx1;
               lhs[i+0][AA-1][1][0] = - tmp2 * fjac[i-1+0][1][0]              - tmp1 * njac[i-1+0][1][0];
               lhs[i+0][AA-1][2][0] = - tmp2 * fjac[i-1+0][2][0]              - tmp1 * njac[i-1+0][2][0];
               lhs[i+0][AA-1][3][0] = - tmp2 * fjac[i-1+0][3][0]              - tmp1 * njac[i-1+0][3][0];
               lhs[i+0][AA-1][4][0] = - tmp2 * fjac[i-1+0][4][0]              - tmp1 * njac[i-1+0][4][0];

               lhs[i+0][AA-1][0][1] = - tmp2 * fjac[i-1+0][0][1]              - tmp1 * njac[i-1+0][0][1];
               lhs[i+0][AA-1][1][1] = - tmp2 * fjac[i-1+0][1][1]              - tmp1 * njac[i-1+0][1][1]              - tmp1 * dx2;
               lhs[i+0][AA-1][2][1] = - tmp2 * fjac[i-1+0][2][1]              - tmp1 * njac[i-1+0][2][1];
               lhs[i+0][AA-1][3][1] = - tmp2 * fjac[i-1+0][3][1]              - tmp1 * njac[i-1+0][3][1];
               lhs[i+0][AA-1][4][1] = - tmp2 * fjac[i-1+0][4][1]              - tmp1 * njac[i-1+0][4][1];

               lhs[i+0][AA-1][0][2] = - tmp2 * fjac[i-1+0][0][2]              - tmp1 * njac[i-1+0][0][2];
               lhs[i+0][AA-1][1][2] = - tmp2 * fjac[i-1+0][1][2]              - tmp1 * njac[i-1+0][1][2];
               lhs[i+0][AA-1][2][2] = - tmp2 * fjac[i-1+0][2][2]              - tmp1 * njac[i-1+0][2][2]              - tmp1 * dx3;
               lhs[i+0][AA-1][3][2] = - tmp2 * fjac[i-1+0][3][2]              - tmp1 * njac[i-1+0][3][2];
               lhs[i+0][AA-1][4][2] = - tmp2 * fjac[i-1+0][4][2]              - tmp1 * njac[i-1+0][4][2];

               lhs[i+0][AA-1][0][3] = - tmp2 * fjac[i-1+0][0][3]              - tmp1 * njac[i-1+0][0][3];
               lhs[i+0][AA-1][1][3] = - tmp2 * fjac[i-1+0][1][3]              - tmp1 * njac[i-1+0][1][3];
               lhs[i+0][AA-1][2][3] = - tmp2 * fjac[i-1+0][2][3]              - tmp1 * njac[i-1+0][2][3];
               lhs[i+0][AA-1][3][3] = - tmp2 * fjac[i-1+0][3][3]              - tmp1 * njac[i-1+0][3][3]              - tmp1 * dx4;
               lhs[i+0][AA-1][4][3] = - tmp2 * fjac[i-1+0][4][3]              - tmp1 * njac[i-1+0][4][3];

               lhs[i+0][AA-1][0][4] = - tmp2 * fjac[i-1+0][0][4]              - tmp1 * njac[i-1+0][0][4];
               lhs[i+0][AA-1][1][4] = - tmp2 * fjac[i-1+0][1][4]              - tmp1 * njac[i-1+0][1][4];
               lhs[i+0][AA-1][2][4] = - tmp2 * fjac[i-1+0][2][4]              - tmp1 * njac[i-1+0][2][4];
               lhs[i+0][AA-1][3][4] = - tmp2 * fjac[i-1+0][3][4]              - tmp1 * njac[i-1+0][3][4];
               lhs[i+0][AA-1][4][4] = - tmp2 * fjac[i-1+0][4][4]              - tmp1 * njac[i-1+0][4][4]              - tmp1 * dx5;

               lhs[i+0][BB-1][0][0] = 1.e0 + tmp1 * 2.e0 * njac[i+0][0][0]              + tmp1 * 2.e0 * dx1;
               lhs[i+0][BB-1][1][0] = tmp1 * 2.e0 * njac[i+0][1][0];
               lhs[i+0][BB-1][2][0] = tmp1 * 2.e0 * njac[i+0][2][0];
               lhs[i+0][BB-1][3][0] = tmp1 * 2.e0 * njac[i+0][3][0];
               lhs[i+0][BB-1][4][0] = tmp1 * 2.e0 * njac[i+0][4][0];

               lhs[i+0][BB-1][0][1] = tmp1 * 2.e0 * njac[i+0][0][1];
               lhs[i+0][BB-1][1][1] = 1.e0 + tmp1 * 2.e0 * njac[i+0][1][1]              + tmp1 * 2.e0 * dx2;
               lhs[i+0][BB-1][2][1] = tmp1 * 2.e0 * njac[i+0][2][1];
               lhs[i+0][BB-1][3][1] = tmp1 * 2.e0 * njac[i+0][3][1];
               lhs[i+0][BB-1][4][1] = tmp1 * 2.e0 * njac[i+0][4][1];

               lhs[i+0][BB-1][0][2] = tmp1 * 2.e0 * njac[i+0][0][2];
               lhs[i+0][BB-1][1][2] = tmp1 * 2.e0 * njac[i+0][1][2];
               lhs[i+0][BB-1][2][2] = 1.e0 + tmp1 * 2.e0 * njac[i+0][2][2]              + tmp1 * 2.e0 * dx3;
               lhs[i+0][BB-1][3][2] = tmp1 * 2.e0 * njac[i+0][3][2];
               lhs[i+0][BB-1][4][2] = tmp1 * 2.e0 * njac[i+0][4][2];

               lhs[i+0][BB-1][0][3] = tmp1 * 2.e0 * njac[i+0][0][3];
               lhs[i+0][BB-1][1][3] = tmp1 * 2.e0 * njac[i+0][1][3];
               lhs[i+0][BB-1][2][3] = tmp1 * 2.e0 * njac[i+0][2][3];
               lhs[i+0][BB-1][3][3] = 1.e0 + tmp1 * 2.e0 * njac[i+0][3][3]              + tmp1 * 2.e0 * dx4;
               lhs[i+0][BB-1][4][3] = tmp1 * 2.e0 * njac[i+0][4][3];

               lhs[i+0][BB-1][0][4] = tmp1 * 2.e0 * njac[i+0][0][4];
               lhs[i+0][BB-1][1][4] = tmp1 * 2.e0 * njac[i+0][1][4];
               lhs[i+0][BB-1][2][4] = tmp1 * 2.e0 * njac[i+0][2][4];
               lhs[i+0][BB-1][3][4] = tmp1 * 2.e0 * njac[i+0][3][4];
               lhs[i+0][BB-1][4][4] = 1.e0 + tmp1 * 2.e0 * njac[i+0][4][4]              + tmp1 * 2.e0 * dx5;

               lhs[i+0][CC-1][0][0] =  tmp2 * fjac[i+1+0][0][0]              - tmp1 * njac[i+1+0][0][0]              - tmp1 * dx1;
               lhs[i+0][CC-1][1][0] =  tmp2 * fjac[i+1+0][1][0]              - tmp1 * njac[i+1+0][1][0];
               lhs[i+0][CC-1][2][0] =  tmp2 * fjac[i+1+0][2][0]              - tmp1 * njac[i+1+0][2][0];
               lhs[i+0][CC-1][3][0] =  tmp2 * fjac[i+1+0][3][0]              - tmp1 * njac[i+1+0][3][0];
               lhs[i+0][CC-1][4][0] =  tmp2 * fjac[i+1+0][4][0]              - tmp1 * njac[i+1+0][4][0];

               lhs[i+0][CC-1][0][1] =  tmp2 * fjac[i+1+0][0][1]              - tmp1 * njac[i+1+0][0][1];
               lhs[i+0][CC-1][1][1] =  tmp2 * fjac[i+1+0][1][1]              - tmp1 * njac[i+1+0][1][1]              - tmp1 * dx2;
               lhs[i+0][CC-1][2][1] =  tmp2 * fjac[i+1+0][2][1]              - tmp1 * njac[i+1+0][2][1];
               lhs[i+0][CC-1][3][1] =  tmp2 * fjac[i+1+0][3][1]              - tmp1 * njac[i+1+0][3][1];
               lhs[i+0][CC-1][4][1] =  tmp2 * fjac[i+1+0][4][1]              - tmp1 * njac[i+1+0][4][1];

               lhs[i+0][CC-1][0][2] =  tmp2 * fjac[i+1+0][0][2]              - tmp1 * njac[i+1+0][0][2];
               lhs[i+0][CC-1][1][2] =  tmp2 * fjac[i+1+0][1][2]              - tmp1 * njac[i+1+0][1][2];
               lhs[i+0][CC-1][2][2] =  tmp2 * fjac[i+1+0][2][2]              - tmp1 * njac[i+1+0][2][2]              - tmp1 * dx3;
               lhs[i+0][CC-1][3][2] =  tmp2 * fjac[i+1+0][3][2]              - tmp1 * njac[i+1+0][3][2];
               lhs[i+0][CC-1][4][2] =  tmp2 * fjac[i+1+0][4][2]              - tmp1 * njac[i+1+0][4][2];

               lhs[i+0][CC-1][0][3] =  tmp2 * fjac[i+1+0][0][3]              - tmp1 * njac[i+1+0][0][3];
               lhs[i+0][CC-1][1][3] =  tmp2 * fjac[i+1+0][1][3]              - tmp1 * njac[i+1+0][1][3];
               lhs[i+0][CC-1][2][3] =  tmp2 * fjac[i+1+0][2][3]              - tmp1 * njac[i+1+0][2][3];
               lhs[i+0][CC-1][3][3] =  tmp2 * fjac[i+1+0][3][3]              - tmp1 * njac[i+1+0][3][3]              - tmp1 * dx4;
               lhs[i+0][CC-1][4][3] =  tmp2 * fjac[i+1+0][4][3]              - tmp1 * njac[i+1+0][4][3];

               lhs[i+0][CC-1][0][4] =  tmp2 * fjac[i+1+0][0][4]              - tmp1 * njac[i+1+0][0][4];
               lhs[i+0][CC-1][1][4] =  tmp2 * fjac[i+1+0][1][4]              - tmp1 * njac[i+1+0][1][4];
               lhs[i+0][CC-1][2][4] =  tmp2 * fjac[i+1+0][2][4]              - tmp1 * njac[i+1+0][2][4];
               lhs[i+0][CC-1][3][4] =  tmp2 * fjac[i+1+0][3][4]              - tmp1 * njac[i+1+0][3][4];
               lhs[i+0][CC-1][4][4] =  tmp2 * fjac[i+1+0][4][4]              - tmp1 * njac[i+1+0][4][4]              - tmp1 * dx5;

            }

//---------------------------------------------------------------------
//---------------------------------------------------------------------

//---------------------------------------------------------------------
//     performs gaussian elimination on this cell.
//     
//     assumes that unpacking routines for non-first cells 
//     preload C' and rhs' from previous cell.
//     
//     assumed send happens outside this routine, but that
//     c'(IMAX) and rhs'(IMAX) will be sent to next cell
//---------------------------------------------------------------------

//---------------------------------------------------------------------
//     outer most do loops - sweeping in i direction
//---------------------------------------------------------------------

//---------------------------------------------------------------------
//     multiply c(0,j,k) by b_inverse and copy back to c
//     multiply rhs(0) by b_inverse(0) and copy to rhs
//---------------------------------------------------------------------
            binvcrhs (&lhs[0+0][BB-1][0][0],&lhs[0+0][CC-1][0][0],&rhs[k+0][j+0][0+0][0] );

//---------------------------------------------------------------------
//     begin inner most do loop
//     do all the elements of the cell unless last 
//---------------------------------------------------------------------
            do (i,1,isize-1,1) {

//---------------------------------------------------------------------
//     rhs(i) = rhs(i) - A*rhs(i-1)
//---------------------------------------------------------------------
               matvec_sub (&lhs[i+0][AA-1][0][0],&rhs[k+0][j+0][i-1+0][0],&rhs[k+0][j+0][i+0][0]);

//---------------------------------------------------------------------
//     B(i) = B(i) - C(i-1)*A(i)
//---------------------------------------------------------------------
               matmul_sub (&lhs[i+0][AA-1][0][0],&lhs[i-1+0][CC-1][0][0],&lhs[i+0][BB-1][0][0]);


//---------------------------------------------------------------------
//     multiply c(i,j,k) by b_inverse and copy back to c
//     multiply rhs(1,j,k) by b_inverse(1,j,k) and copy to rhs
//---------------------------------------------------------------------
               binvcrhs (&lhs[i+0][BB-1][0][0],&lhs[i+0][CC-1][0][0],&rhs[k+0][j+0][i+0][0] );

            }

//---------------------------------------------------------------------
//     rhs(isize) = rhs(isize) - A*rhs(isize-1)
//---------------------------------------------------------------------
            matvec_sub (&lhs[isize+0][AA-1][0][0],&rhs[k+0][j+0][isize-1+0][0],&rhs[k+0][j+0][isize+0][0]);

//---------------------------------------------------------------------
//     B(isize) = B(isize) - C(isize-1)*A(isize)
//---------------------------------------------------------------------
            matmul_sub (&lhs[isize+0][AA-1][0][0],&lhs[isize-1+0][CC-1][0][0],&lhs[isize+0][BB-1][0][0]);

//---------------------------------------------------------------------
//     multiply rhs() by b_inverse() and copy to rhs
//---------------------------------------------------------------------
            binvrhs (&lhs[isize+0][BB-1][0][0],&rhs[k+0][j+0][isize+0][0] );


//---------------------------------------------------------------------
//     back solve: if last cell, then generate U(isize)=rhs(isize)
//     else assume U(isize) is loaded in un pack backsub_info
//     so just use it
//     after call u(istart) will be sent to next cell
//---------------------------------------------------------------------

            dom (i,isize-1,0,-1) {
               do (m,1,BLOCK_SIZE,1) {
                  do (n,1,BLOCK_SIZE,1) {
                     rhs[k+0][j+0][i+0][m-1] = rhs[k+0][j+0][i+0][m-1]                     - lhs[i+0][CC-1][n-1][m-1]*rhs[k+0][j+0][i+1+0][n-1];
                  }
               }
            }

         }
      }
//$omp end do nowait
 
 //#pragma omp end do nowait
      if (timeron) timer_stop (T_XSOLVE);

      return;
}//end


//---------------------------------------------------------------------
//---------------------------------------------------------------------

