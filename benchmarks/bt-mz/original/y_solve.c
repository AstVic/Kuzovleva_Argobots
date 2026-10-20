#include "header.h"

void y_solve (orho_i,oqs,osquare,ou,orhs,nx,nxmax,ny,nz)
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
// end param y_solve
{
double (*rhs)[(ny-1)-(0)+1][(nxmax-1)-(0)+1][5] = (double (*)[(ny-1)-(0)+1][(nxmax-1)-(0)+1][5])orhs;
double (*u)[(ny-1)-(0)+1][(nxmax-1)-(0)+1][5] = (double (*)[(ny-1)-(0)+1][(nxmax-1)-(0)+1][5])ou;
double (*square)[(ny-1)-(0)+1][(nxmax-1)-(0)+1] = (double (*)[(ny-1)-(0)+1][(nxmax-1)-(0)+1])osquare;
double (*qs)[(ny-1)-(0)+1][(nxmax-1)-(0)+1] = (double (*)[(ny-1)-(0)+1][(nxmax-1)-(0)+1])oqs;
double (*rho_i)[(ny-1)-(0)+1][(nxmax-1)-(0)+1] = (double (*)[(ny-1)-(0)+1][(nxmax-1)-(0)+1])orho_i;


//---------------------------------------------------------------------
//---------------------------------------------------------------------

//---------------------------------------------------------------------
//     Performs line solves in Y direction by first factoring
//     the block-tridiagonal matrix into an upper triangular matrix, 
//     and then performing back substitution to solve for the unknow
//     vectors of each line.  
//     
//     Make sure we treat elements zero to cell_size in the direction
//     of the sweep.
//---------------------------------------------------------------------

#include "work_lhs.h"


      int i;
      int j;
      int k;
      int m;
      int n;
      int jsize;

//---------------------------------------------------------------------
//---------------------------------------------------------------------

      if (timeron) timer_start (T_YSOLVE);

//---------------------------------------------------------------------
//---------------------------------------------------------------------

//---------------------------------------------------------------------
//     This function computes the left hand side for the three y-factors   
//---------------------------------------------------------------------

      jsize = ny-1;

//---------------------------------------------------------------------
//     Compute the indices for storing the tri-diagonal matrix;
//     determine a (labeled f) and n jacobians for cell c
//---------------------------------------------------------------------
//$omp do schedule(static)
 
#pragma omp for schedule(static)
      do (k , 1, nz-2,1) {
         do (i , 1, nx-2,1) {
            do (j , 0, jsize,1) {

               tmp1 = rho_i[k+0][j+0][i+0];
               tmp2 = tmp1 * tmp1;
               tmp3 = tmp1 * tmp2;

               fjac[j+0][0][0] = 0.e0;
               fjac[j+0][1][0] = 0.e0;
               fjac[j+0][2][0] = 1.e0;
               fjac[j+0][3][0] = 0.e0;
               fjac[j+0][4][0] = 0.e0;

               fjac[j+0][0][1] = -( u[k+0][j+0][i+0][1]*u[k+0][j+0][i+0][2] )              * tmp2;
               fjac[j+0][1][1] = u[k+0][j+0][i+0][2] * tmp1;
               fjac[j+0][2][1] = u[k+0][j+0][i+0][1] * tmp1;
               fjac[j+0][3][1] = 0.e0;
               fjac[j+0][4][1] = 0.e0;

               fjac[j+0][0][2] = -( u[k+0][j+0][i+0][2]*u[k+0][j+0][i+0][2]*tmp2)              + c2 * qs[k+0][j+0][i+0];
               fjac[j+0][1][2] = - c2 *  u[k+0][j+0][i+0][1] * tmp1;
               fjac[j+0][2][2] =( 2.e0 - c2 )              *  u[k+0][j+0][i+0][2] * tmp1;
               fjac[j+0][3][2] = - c2 * u[k+0][j+0][i+0][3] * tmp1;
               fjac[j+0][4][2] = c2;

               fjac[j+0][0][3] = -( u[k+0][j+0][i+0][2]*u[k+0][j+0][i+0][3] )              * tmp2;
               fjac[j+0][1][3] = 0.e0;
               fjac[j+0][2][3] = u[k+0][j+0][i+0][3] * tmp1;
               fjac[j+0][3][3] = u[k+0][j+0][i+0][2] * tmp1;
               fjac[j+0][4][3] = 0.e0;

               fjac[j+0][0][4] =( c2 * 2.0e0 * square[k+0][j+0][i+0]              - c1 * u[k+0][j+0][i+0][4] )              * u[k+0][j+0][i+0][2] * tmp2;
               fjac[j+0][1][4] = - c2 * u[k+0][j+0][i+0][1]*u[k+0][j+0][i+0][2]               * tmp2;
               fjac[j+0][2][4] = c1 * u[k+0][j+0][i+0][4] * tmp1               - c2 *( qs[k+0][j+0][i+0]              + u[k+0][j+0][i+0][2]*u[k+0][j+0][i+0][2] * tmp2 );
               fjac[j+0][3][4] = - c2 *( u[k+0][j+0][i+0][2]*u[k+0][j+0][i+0][3] )              * tmp2;
               fjac[j+0][4][4] = c1 * u[k+0][j+0][i+0][2] * tmp1;

               njac[j+0][0][0] = 0.e0;
               njac[j+0][1][0] = 0.e0;
               njac[j+0][2][0] = 0.e0;
               njac[j+0][3][0] = 0.e0;
               njac[j+0][4][0] = 0.e0;

               njac[j+0][0][1] = - c3c4 * tmp2 * u[k+0][j+0][i+0][1];
               njac[j+0][1][1] =   c3c4 * tmp1;
               njac[j+0][2][1] =   0.e0;
               njac[j+0][3][1] =   0.e0;
               njac[j+0][4][1] =   0.e0;

               njac[j+0][0][2] = - con43 * c3c4 * tmp2 * u[k+0][j+0][i+0][2];
               njac[j+0][1][2] =   0.e0;
               njac[j+0][2][2] =   con43 * c3c4 * tmp1;
               njac[j+0][3][2] =   0.e0;
               njac[j+0][4][2] =   0.e0;

               njac[j+0][0][3] = - c3c4 * tmp2 * u[k+0][j+0][i+0][3];
               njac[j+0][1][3] =   0.e0;
               njac[j+0][2][3] =   0.e0;
               njac[j+0][3][3] =   c3c4 * tmp1;
               njac[j+0][4][3] =   0.e0;

               njac[j+0][0][4] = -(  c3c4              - c1345 ) * tmp3 *((u[k+0][j+0][i+0][1])*(u[k+0][j+0][i+0][1]))              -( con43 * c3c4              - c1345 ) * tmp3 *((u[k+0][j+0][i+0][2])*(u[k+0][j+0][i+0][2]))              -( c3c4 - c1345 ) * tmp3 *((u[k+0][j+0][i+0][3])*(u[k+0][j+0][i+0][3]))              - c1345 * tmp2 * u[k+0][j+0][i+0][4];

               njac[j+0][1][4] =(  c3c4 - c1345 ) * tmp2 * u[k+0][j+0][i+0][1];
               njac[j+0][2][4] =( con43 * c3c4              - c1345 ) * tmp2 * u[k+0][j+0][i+0][2];
               njac[j+0][3][4] =( c3c4 - c1345 ) * tmp2 * u[k+0][j+0][i+0][3];
               njac[j+0][4][4] =( c1345 ) * tmp1;

            }

//---------------------------------------------------------------------
//     now joacobians set, so form left hand side in y direction
//---------------------------------------------------------------------
            lhsinit (lhs,jsize);
            do (j , 1, jsize-1,1) {

               tmp1 = dt * ty1;
               tmp2 = dt * ty2;

               lhs[j+0][AA-1][0][0] = - tmp2 * fjac[j-1+0][0][0]              - tmp1 * njac[j-1+0][0][0]              - tmp1 * dy1;
               lhs[j+0][AA-1][1][0] = - tmp2 * fjac[j-1+0][1][0]              - tmp1 * njac[j-1+0][1][0];
               lhs[j+0][AA-1][2][0] = - tmp2 * fjac[j-1+0][2][0]              - tmp1 * njac[j-1+0][2][0];
               lhs[j+0][AA-1][3][0] = - tmp2 * fjac[j-1+0][3][0]              - tmp1 * njac[j-1+0][3][0];
               lhs[j+0][AA-1][4][0] = - tmp2 * fjac[j-1+0][4][0]              - tmp1 * njac[j-1+0][4][0];

               lhs[j+0][AA-1][0][1] = - tmp2 * fjac[j-1+0][0][1]              - tmp1 * njac[j-1+0][0][1];
               lhs[j+0][AA-1][1][1] = - tmp2 * fjac[j-1+0][1][1]              - tmp1 * njac[j-1+0][1][1]              - tmp1 * dy2;
               lhs[j+0][AA-1][2][1] = - tmp2 * fjac[j-1+0][2][1]              - tmp1 * njac[j-1+0][2][1];
               lhs[j+0][AA-1][3][1] = - tmp2 * fjac[j-1+0][3][1]              - tmp1 * njac[j-1+0][3][1];
               lhs[j+0][AA-1][4][1] = - tmp2 * fjac[j-1+0][4][1]              - tmp1 * njac[j-1+0][4][1];

               lhs[j+0][AA-1][0][2] = - tmp2 * fjac[j-1+0][0][2]              - tmp1 * njac[j-1+0][0][2];
               lhs[j+0][AA-1][1][2] = - tmp2 * fjac[j-1+0][1][2]              - tmp1 * njac[j-1+0][1][2];
               lhs[j+0][AA-1][2][2] = - tmp2 * fjac[j-1+0][2][2]              - tmp1 * njac[j-1+0][2][2]              - tmp1 * dy3;
               lhs[j+0][AA-1][3][2] = - tmp2 * fjac[j-1+0][3][2]              - tmp1 * njac[j-1+0][3][2];
               lhs[j+0][AA-1][4][2] = - tmp2 * fjac[j-1+0][4][2]              - tmp1 * njac[j-1+0][4][2];

               lhs[j+0][AA-1][0][3] = - tmp2 * fjac[j-1+0][0][3]              - tmp1 * njac[j-1+0][0][3];
               lhs[j+0][AA-1][1][3] = - tmp2 * fjac[j-1+0][1][3]              - tmp1 * njac[j-1+0][1][3];
               lhs[j+0][AA-1][2][3] = - tmp2 * fjac[j-1+0][2][3]              - tmp1 * njac[j-1+0][2][3];
               lhs[j+0][AA-1][3][3] = - tmp2 * fjac[j-1+0][3][3]              - tmp1 * njac[j-1+0][3][3]              - tmp1 * dy4;
               lhs[j+0][AA-1][4][3] = - tmp2 * fjac[j-1+0][4][3]              - tmp1 * njac[j-1+0][4][3];

               lhs[j+0][AA-1][0][4] = - tmp2 * fjac[j-1+0][0][4]              - tmp1 * njac[j-1+0][0][4];
               lhs[j+0][AA-1][1][4] = - tmp2 * fjac[j-1+0][1][4]              - tmp1 * njac[j-1+0][1][4];
               lhs[j+0][AA-1][2][4] = - tmp2 * fjac[j-1+0][2][4]              - tmp1 * njac[j-1+0][2][4];
               lhs[j+0][AA-1][3][4] = - tmp2 * fjac[j-1+0][3][4]              - tmp1 * njac[j-1+0][3][4];
               lhs[j+0][AA-1][4][4] = - tmp2 * fjac[j-1+0][4][4]              - tmp1 * njac[j-1+0][4][4]              - tmp1 * dy5;

               lhs[j+0][BB-1][0][0] = 1.e0 + tmp1 * 2.e0 * njac[j+0][0][0]              + tmp1 * 2.e0 * dy1;
               lhs[j+0][BB-1][1][0] = tmp1 * 2.e0 * njac[j+0][1][0];
               lhs[j+0][BB-1][2][0] = tmp1 * 2.e0 * njac[j+0][2][0];
               lhs[j+0][BB-1][3][0] = tmp1 * 2.e0 * njac[j+0][3][0];
               lhs[j+0][BB-1][4][0] = tmp1 * 2.e0 * njac[j+0][4][0];

               lhs[j+0][BB-1][0][1] = tmp1 * 2.e0 * njac[j+0][0][1];
               lhs[j+0][BB-1][1][1] = 1.e0 + tmp1 * 2.e0 * njac[j+0][1][1]              + tmp1 * 2.e0 * dy2;
               lhs[j+0][BB-1][2][1] = tmp1 * 2.e0 * njac[j+0][2][1];
               lhs[j+0][BB-1][3][1] = tmp1 * 2.e0 * njac[j+0][3][1];
               lhs[j+0][BB-1][4][1] = tmp1 * 2.e0 * njac[j+0][4][1];

               lhs[j+0][BB-1][0][2] = tmp1 * 2.e0 * njac[j+0][0][2];
               lhs[j+0][BB-1][1][2] = tmp1 * 2.e0 * njac[j+0][1][2];
               lhs[j+0][BB-1][2][2] = 1.e0 + tmp1 * 2.e0 * njac[j+0][2][2]              + tmp1 * 2.e0 * dy3;
               lhs[j+0][BB-1][3][2] = tmp1 * 2.e0 * njac[j+0][3][2];
               lhs[j+0][BB-1][4][2] = tmp1 * 2.e0 * njac[j+0][4][2];

               lhs[j+0][BB-1][0][3] = tmp1 * 2.e0 * njac[j+0][0][3];
               lhs[j+0][BB-1][1][3] = tmp1 * 2.e0 * njac[j+0][1][3];
               lhs[j+0][BB-1][2][3] = tmp1 * 2.e0 * njac[j+0][2][3];
               lhs[j+0][BB-1][3][3] = 1.e0 + tmp1 * 2.e0 * njac[j+0][3][3]              + tmp1 * 2.e0 * dy4;
               lhs[j+0][BB-1][4][3] = tmp1 * 2.e0 * njac[j+0][4][3];

               lhs[j+0][BB-1][0][4] = tmp1 * 2.e0 * njac[j+0][0][4];
               lhs[j+0][BB-1][1][4] = tmp1 * 2.e0 * njac[j+0][1][4];
               lhs[j+0][BB-1][2][4] = tmp1 * 2.e0 * njac[j+0][2][4];
               lhs[j+0][BB-1][3][4] = tmp1 * 2.e0 * njac[j+0][3][4];
               lhs[j+0][BB-1][4][4] = 1.e0 + tmp1 * 2.e0 * njac[j+0][4][4]               + tmp1 * 2.e0 * dy5;

               lhs[j+0][CC-1][0][0] =  tmp2 * fjac[j+1+0][0][0]              - tmp1 * njac[j+1+0][0][0]              - tmp1 * dy1;
               lhs[j+0][CC-1][1][0] =  tmp2 * fjac[j+1+0][1][0]              - tmp1 * njac[j+1+0][1][0];
               lhs[j+0][CC-1][2][0] =  tmp2 * fjac[j+1+0][2][0]              - tmp1 * njac[j+1+0][2][0];
               lhs[j+0][CC-1][3][0] =  tmp2 * fjac[j+1+0][3][0]              - tmp1 * njac[j+1+0][3][0];
               lhs[j+0][CC-1][4][0] =  tmp2 * fjac[j+1+0][4][0]              - tmp1 * njac[j+1+0][4][0];

               lhs[j+0][CC-1][0][1] =  tmp2 * fjac[j+1+0][0][1]              - tmp1 * njac[j+1+0][0][1];
               lhs[j+0][CC-1][1][1] =  tmp2 * fjac[j+1+0][1][1]              - tmp1 * njac[j+1+0][1][1]              - tmp1 * dy2;
               lhs[j+0][CC-1][2][1] =  tmp2 * fjac[j+1+0][2][1]              - tmp1 * njac[j+1+0][2][1];
               lhs[j+0][CC-1][3][1] =  tmp2 * fjac[j+1+0][3][1]              - tmp1 * njac[j+1+0][3][1];
               lhs[j+0][CC-1][4][1] =  tmp2 * fjac[j+1+0][4][1]              - tmp1 * njac[j+1+0][4][1];

               lhs[j+0][CC-1][0][2] =  tmp2 * fjac[j+1+0][0][2]              - tmp1 * njac[j+1+0][0][2];
               lhs[j+0][CC-1][1][2] =  tmp2 * fjac[j+1+0][1][2]              - tmp1 * njac[j+1+0][1][2];
               lhs[j+0][CC-1][2][2] =  tmp2 * fjac[j+1+0][2][2]              - tmp1 * njac[j+1+0][2][2]              - tmp1 * dy3;
               lhs[j+0][CC-1][3][2] =  tmp2 * fjac[j+1+0][3][2]              - tmp1 * njac[j+1+0][3][2];
               lhs[j+0][CC-1][4][2] =  tmp2 * fjac[j+1+0][4][2]              - tmp1 * njac[j+1+0][4][2];

               lhs[j+0][CC-1][0][3] =  tmp2 * fjac[j+1+0][0][3]              - tmp1 * njac[j+1+0][0][3];
               lhs[j+0][CC-1][1][3] =  tmp2 * fjac[j+1+0][1][3]              - tmp1 * njac[j+1+0][1][3];
               lhs[j+0][CC-1][2][3] =  tmp2 * fjac[j+1+0][2][3]              - tmp1 * njac[j+1+0][2][3];
               lhs[j+0][CC-1][3][3] =  tmp2 * fjac[j+1+0][3][3]              - tmp1 * njac[j+1+0][3][3]              - tmp1 * dy4;
               lhs[j+0][CC-1][4][3] =  tmp2 * fjac[j+1+0][4][3]              - tmp1 * njac[j+1+0][4][3];

               lhs[j+0][CC-1][0][4] =  tmp2 * fjac[j+1+0][0][4]              - tmp1 * njac[j+1+0][0][4];
               lhs[j+0][CC-1][1][4] =  tmp2 * fjac[j+1+0][1][4]              - tmp1 * njac[j+1+0][1][4];
               lhs[j+0][CC-1][2][4] =  tmp2 * fjac[j+1+0][2][4]              - tmp1 * njac[j+1+0][2][4];
               lhs[j+0][CC-1][3][4] =  tmp2 * fjac[j+1+0][3][4]              - tmp1 * njac[j+1+0][3][4];
               lhs[j+0][CC-1][4][4] =  tmp2 * fjac[j+1+0][4][4]              - tmp1 * njac[j+1+0][4][4]              - tmp1 * dy5;

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
//     c'(JMAX) and rhs'(JMAX) will be sent to next cell
//---------------------------------------------------------------------

//---------------------------------------------------------------------
//     multiply c(i,0,k) by b_inverse and copy back to c
//     multiply rhs(0) by b_inverse(0) and copy to rhs
//---------------------------------------------------------------------
            binvcrhs (&lhs[0+0][BB-1][0][0],&lhs[0+0][CC-1][0][0],&rhs[k+0][0+0][i+0][0] );

//---------------------------------------------------------------------
//     begin inner most do loop
//     do all the elements of the cell unless last 
//---------------------------------------------------------------------
            do (j,1,jsize-1,1) {

//---------------------------------------------------------------------
//     subtract A*lhs_vector(j-1) from lhs_vector(j)
//     
//     rhs(j) = rhs(j) - A*rhs(j-1)
//---------------------------------------------------------------------
               matvec_sub (&lhs[j+0][AA-1][0][0],&rhs[k+0][j-1+0][i+0][0],&rhs[k+0][j+0][i+0][0]);

//---------------------------------------------------------------------
//     B(j) = B(j) - C(j-1)*A(j)
//---------------------------------------------------------------------
               matmul_sub (&lhs[j+0][AA-1][0][0],&lhs[j-1+0][CC-1][0][0],&lhs[j+0][BB-1][0][0]);

//---------------------------------------------------------------------
//     multiply c(i,j,k) by b_inverse and copy back to c
//     multiply rhs(i,1,k) by b_inverse(i,1,k) and copy to rhs
//---------------------------------------------------------------------
               binvcrhs (&lhs[j+0][BB-1][0][0],&lhs[j+0][CC-1][0][0],&rhs[k+0][j+0][i+0][0] );

            }


//---------------------------------------------------------------------
//     rhs(jsize) = rhs(jsize) - A*rhs(jsize-1)
//---------------------------------------------------------------------
            matvec_sub (&lhs[jsize+0][AA-1][0][0],&rhs[k+0][jsize-1+0][i+0][0],&rhs[k+0][jsize+0][i+0][0]);

//---------------------------------------------------------------------
//     B(jsize) = B(jsize) - C(jsize-1)*A(jsize)
//     call matmul_sub(aa,i,jsize,k,c,
//     $              cc,i,jsize-1,k,c,bb,i,jsize,k)
//---------------------------------------------------------------------
            matmul_sub (&lhs[jsize+0][AA-1][0][0],&lhs[jsize-1+0][CC-1][0][0],&lhs[jsize+0][BB-1][0][0]);

//---------------------------------------------------------------------
//     multiply rhs(jsize) by b_inverse(jsize) and copy to rhs
//---------------------------------------------------------------------
            binvrhs (&lhs[jsize+0][BB-1][0][0],&rhs[k+0][jsize+0][i+0][0] );


//---------------------------------------------------------------------
//     back solve: if last cell, then generate U(jsize)=rhs(jsize)
//     else assume U(jsize) is loaded in un pack backsub_info
//     so just use it
//     after call u(jstart) will be sent to next cell
//---------------------------------------------------------------------

            dom (j,jsize-1,0,-1) {
               do (m,1,BLOCK_SIZE,1) {
                  do (n,1,BLOCK_SIZE,1) {
                     rhs[k+0][j+0][i+0][m-1] = rhs[k+0][j+0][i+0][m-1]                     - lhs[j+0][CC-1][n-1][m-1]*rhs[k+0][j+1+0][i+0][n-1];
                  }
               }
            }

         }
      }
//$omp end do
 
 //#pragma omp end do
      if (timeron) timer_stop (T_YSOLVE);

      return;
}//end



