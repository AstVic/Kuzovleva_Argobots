#include "header.h"

void z_solve (orho_i,oqs,osquare,ou,orhs,nx,nxmax,ny,nz)
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
// end param z_solve
{
double (*rhs)[(ny-1)-(0)+1][(nxmax-1)-(0)+1][5] = (double (*)[(ny-1)-(0)+1][(nxmax-1)-(0)+1][5])orhs;
double (*u)[(ny-1)-(0)+1][(nxmax-1)-(0)+1][5] = (double (*)[(ny-1)-(0)+1][(nxmax-1)-(0)+1][5])ou;
double (*square)[(ny-1)-(0)+1][(nxmax-1)-(0)+1] = (double (*)[(ny-1)-(0)+1][(nxmax-1)-(0)+1])osquare;
double (*qs)[(ny-1)-(0)+1][(nxmax-1)-(0)+1] = (double (*)[(ny-1)-(0)+1][(nxmax-1)-(0)+1])oqs;
double (*rho_i)[(ny-1)-(0)+1][(nxmax-1)-(0)+1] = (double (*)[(ny-1)-(0)+1][(nxmax-1)-(0)+1])orho_i;


//---------------------------------------------------------------------
//---------------------------------------------------------------------

//---------------------------------------------------------------------
//     Performs line solves in Z direction by first factoring
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
      int ksize;

//---------------------------------------------------------------------
//---------------------------------------------------------------------

      if (timeron) timer_start (T_ZSOLVE);

//---------------------------------------------------------------------
//---------------------------------------------------------------------

//---------------------------------------------------------------------
//     This function computes the left hand side for the three z-factors   
//---------------------------------------------------------------------

      ksize = nz-1;

//---------------------------------------------------------------------
//     Compute the indices for storing the block-diagonal matrix;
//     determine c (labeled f) and s jacobians
//---------------------------------------------------------------------
//$omp do schedule(static)
 
#pragma omp for schedule(static)
      do (j , 1, ny-2,1) {
         do (i , 1, nx-2,1) {
            do (k , 0, ksize,1) {

               tmp1 = 1.e0 / u[k+0][j+0][i+0][0];
               tmp2 = tmp1 * tmp1;
               tmp3 = tmp1 * tmp2;

               fjac[k+0][0][0] = 0.e0;
               fjac[k+0][1][0] = 0.e0;
               fjac[k+0][2][0] = 0.e0;
               fjac[k+0][3][0] = 1.e0;
               fjac[k+0][4][0] = 0.e0;

               fjac[k+0][0][1] = -( u[k+0][j+0][i+0][1]*u[k+0][j+0][i+0][3] )               * tmp2;
               fjac[k+0][1][1] = u[k+0][j+0][i+0][3] * tmp1;
               fjac[k+0][2][1] = 0.e0;
               fjac[k+0][3][1] = u[k+0][j+0][i+0][1] * tmp1;
               fjac[k+0][4][1] = 0.e0;

               fjac[k+0][0][2] = -( u[k+0][j+0][i+0][2]*u[k+0][j+0][i+0][3] )              * tmp2;
               fjac[k+0][1][2] = 0.e0;
               fjac[k+0][2][2] = u[k+0][j+0][i+0][3] * tmp1;
               fjac[k+0][3][2] = u[k+0][j+0][i+0][2] * tmp1;
               fjac[k+0][4][2] = 0.e0;

               fjac[k+0][0][3] = -(u[k+0][j+0][i+0][3]*u[k+0][j+0][i+0][3] * tmp2 )               + c2 * qs[k+0][j+0][i+0];
               fjac[k+0][1][3] = - c2 *  u[k+0][j+0][i+0][1] * tmp1;
               fjac[k+0][2][3] = - c2 *  u[k+0][j+0][i+0][2] * tmp1;
               fjac[k+0][3][3] =( 2.e0 - c2 )              *  u[k+0][j+0][i+0][3] * tmp1;
               fjac[k+0][4][3] = c2;

               fjac[k+0][0][4] =( c2 * 2.0e0 * square[k+0][j+0][i+0]               - c1 * u[k+0][j+0][i+0][4] )              * u[k+0][j+0][i+0][3] * tmp2;
               fjac[k+0][1][4] = - c2 *( u[k+0][j+0][i+0][1]*u[k+0][j+0][i+0][3] )              * tmp2;
               fjac[k+0][2][4] = - c2 *( u[k+0][j+0][i+0][2]*u[k+0][j+0][i+0][3] )              * tmp2;
               fjac[k+0][3][4] = c1 *( u[k+0][j+0][i+0][4] * tmp1 )              - c2 *( qs[k+0][j+0][i+0]              + u[k+0][j+0][i+0][3]*u[k+0][j+0][i+0][3] * tmp2 );
               fjac[k+0][4][4] = c1 * u[k+0][j+0][i+0][3] * tmp1;

               njac[k+0][0][0] = 0.e0;
               njac[k+0][1][0] = 0.e0;
               njac[k+0][2][0] = 0.e0;
               njac[k+0][3][0] = 0.e0;
               njac[k+0][4][0] = 0.e0;

               njac[k+0][0][1] = - c3c4 * tmp2 * u[k+0][j+0][i+0][1];
               njac[k+0][1][1] =   c3c4 * tmp1;
               njac[k+0][2][1] =   0.e0;
               njac[k+0][3][1] =   0.e0;
               njac[k+0][4][1] =   0.e0;

               njac[k+0][0][2] = - c3c4 * tmp2 * u[k+0][j+0][i+0][2];
               njac[k+0][1][2] =   0.e0;
               njac[k+0][2][2] =   c3c4 * tmp1;
               njac[k+0][3][2] =   0.e0;
               njac[k+0][4][2] =   0.e0;

               njac[k+0][0][3] = - con43 * c3c4 * tmp2 * u[k+0][j+0][i+0][3];
               njac[k+0][1][3] =   0.e0;
               njac[k+0][2][3] =   0.e0;
               njac[k+0][3][3] =   con43 * c3 * c4 * tmp1;
               njac[k+0][4][3] =   0.e0;

               njac[k+0][0][4] = -(  c3c4              - c1345 ) * tmp3 *((u[k+0][j+0][i+0][1])*(u[k+0][j+0][i+0][1]))              -( c3c4 - c1345 ) * tmp3 *((u[k+0][j+0][i+0][2])*(u[k+0][j+0][i+0][2]))              -( con43 * c3c4              - c1345 ) * tmp3 *((u[k+0][j+0][i+0][3])*(u[k+0][j+0][i+0][3]))              - c1345 * tmp2 * u[k+0][j+0][i+0][4];

               njac[k+0][1][4] =(  c3c4 - c1345 ) * tmp2 * u[k+0][j+0][i+0][1];
               njac[k+0][2][4] =(  c3c4 - c1345 ) * tmp2 * u[k+0][j+0][i+0][2];
               njac[k+0][3][4] =( con43 * c3c4              - c1345 ) * tmp2 * u[k+0][j+0][i+0][3];
               njac[k+0][4][4] =( c1345 )* tmp1;


            }

//---------------------------------------------------------------------
//     now jacobians set, so form left hand side in z direction
//---------------------------------------------------------------------
            lhsinit (lhs,ksize);
            do (k , 1, ksize-1,1) {

               tmp1 = dt * tz1;
               tmp2 = dt * tz2;

               lhs[k+0][AA-1][0][0] = - tmp2 * fjac[k-1+0][0][0]              - tmp1 * njac[k-1+0][0][0]              - tmp1 * dz1;
               lhs[k+0][AA-1][1][0] = - tmp2 * fjac[k-1+0][1][0]              - tmp1 * njac[k-1+0][1][0];
               lhs[k+0][AA-1][2][0] = - tmp2 * fjac[k-1+0][2][0]              - tmp1 * njac[k-1+0][2][0];
               lhs[k+0][AA-1][3][0] = - tmp2 * fjac[k-1+0][3][0]              - tmp1 * njac[k-1+0][3][0];
               lhs[k+0][AA-1][4][0] = - tmp2 * fjac[k-1+0][4][0]              - tmp1 * njac[k-1+0][4][0];

               lhs[k+0][AA-1][0][1] = - tmp2 * fjac[k-1+0][0][1]              - tmp1 * njac[k-1+0][0][1];
               lhs[k+0][AA-1][1][1] = - tmp2 * fjac[k-1+0][1][1]              - tmp1 * njac[k-1+0][1][1]              - tmp1 * dz2;
               lhs[k+0][AA-1][2][1] = - tmp2 * fjac[k-1+0][2][1]              - tmp1 * njac[k-1+0][2][1];
               lhs[k+0][AA-1][3][1] = - tmp2 * fjac[k-1+0][3][1]              - tmp1 * njac[k-1+0][3][1];
               lhs[k+0][AA-1][4][1] = - tmp2 * fjac[k-1+0][4][1]              - tmp1 * njac[k-1+0][4][1];

               lhs[k+0][AA-1][0][2] = - tmp2 * fjac[k-1+0][0][2]              - tmp1 * njac[k-1+0][0][2];
               lhs[k+0][AA-1][1][2] = - tmp2 * fjac[k-1+0][1][2]              - tmp1 * njac[k-1+0][1][2];
               lhs[k+0][AA-1][2][2] = - tmp2 * fjac[k-1+0][2][2]              - tmp1 * njac[k-1+0][2][2]              - tmp1 * dz3;
               lhs[k+0][AA-1][3][2] = - tmp2 * fjac[k-1+0][3][2]              - tmp1 * njac[k-1+0][3][2];
               lhs[k+0][AA-1][4][2] = - tmp2 * fjac[k-1+0][4][2]              - tmp1 * njac[k-1+0][4][2];

               lhs[k+0][AA-1][0][3] = - tmp2 * fjac[k-1+0][0][3]              - tmp1 * njac[k-1+0][0][3];
               lhs[k+0][AA-1][1][3] = - tmp2 * fjac[k-1+0][1][3]              - tmp1 * njac[k-1+0][1][3];
               lhs[k+0][AA-1][2][3] = - tmp2 * fjac[k-1+0][2][3]              - tmp1 * njac[k-1+0][2][3];
               lhs[k+0][AA-1][3][3] = - tmp2 * fjac[k-1+0][3][3]              - tmp1 * njac[k-1+0][3][3]              - tmp1 * dz4;
               lhs[k+0][AA-1][4][3] = - tmp2 * fjac[k-1+0][4][3]              - tmp1 * njac[k-1+0][4][3];

               lhs[k+0][AA-1][0][4] = - tmp2 * fjac[k-1+0][0][4]              - tmp1 * njac[k-1+0][0][4];
               lhs[k+0][AA-1][1][4] = - tmp2 * fjac[k-1+0][1][4]              - tmp1 * njac[k-1+0][1][4];
               lhs[k+0][AA-1][2][4] = - tmp2 * fjac[k-1+0][2][4]              - tmp1 * njac[k-1+0][2][4];
               lhs[k+0][AA-1][3][4] = - tmp2 * fjac[k-1+0][3][4]              - tmp1 * njac[k-1+0][3][4];
               lhs[k+0][AA-1][4][4] = - tmp2 * fjac[k-1+0][4][4]              - tmp1 * njac[k-1+0][4][4]              - tmp1 * dz5;

               lhs[k+0][BB-1][0][0] = 1.e0 + tmp1 * 2.e0 * njac[k+0][0][0]              + tmp1 * 2.e0 * dz1;
               lhs[k+0][BB-1][1][0] = tmp1 * 2.e0 * njac[k+0][1][0];
               lhs[k+0][BB-1][2][0] = tmp1 * 2.e0 * njac[k+0][2][0];
               lhs[k+0][BB-1][3][0] = tmp1 * 2.e0 * njac[k+0][3][0];
               lhs[k+0][BB-1][4][0] = tmp1 * 2.e0 * njac[k+0][4][0];

               lhs[k+0][BB-1][0][1] = tmp1 * 2.e0 * njac[k+0][0][1];
               lhs[k+0][BB-1][1][1] = 1.e0 + tmp1 * 2.e0 * njac[k+0][1][1]              + tmp1 * 2.e0 * dz2;
               lhs[k+0][BB-1][2][1] = tmp1 * 2.e0 * njac[k+0][2][1];
               lhs[k+0][BB-1][3][1] = tmp1 * 2.e0 * njac[k+0][3][1];
               lhs[k+0][BB-1][4][1] = tmp1 * 2.e0 * njac[k+0][4][1];

               lhs[k+0][BB-1][0][2] = tmp1 * 2.e0 * njac[k+0][0][2];
               lhs[k+0][BB-1][1][2] = tmp1 * 2.e0 * njac[k+0][1][2];
               lhs[k+0][BB-1][2][2] = 1.e0 + tmp1 * 2.e0 * njac[k+0][2][2]              + tmp1 * 2.e0 * dz3;
               lhs[k+0][BB-1][3][2] = tmp1 * 2.e0 * njac[k+0][3][2];
               lhs[k+0][BB-1][4][2] = tmp1 * 2.e0 * njac[k+0][4][2];

               lhs[k+0][BB-1][0][3] = tmp1 * 2.e0 * njac[k+0][0][3];
               lhs[k+0][BB-1][1][3] = tmp1 * 2.e0 * njac[k+0][1][3];
               lhs[k+0][BB-1][2][3] = tmp1 * 2.e0 * njac[k+0][2][3];
               lhs[k+0][BB-1][3][3] = 1.e0 + tmp1 * 2.e0 * njac[k+0][3][3]              + tmp1 * 2.e0 * dz4;
               lhs[k+0][BB-1][4][3] = tmp1 * 2.e0 * njac[k+0][4][3];

               lhs[k+0][BB-1][0][4] = tmp1 * 2.e0 * njac[k+0][0][4];
               lhs[k+0][BB-1][1][4] = tmp1 * 2.e0 * njac[k+0][1][4];
               lhs[k+0][BB-1][2][4] = tmp1 * 2.e0 * njac[k+0][2][4];
               lhs[k+0][BB-1][3][4] = tmp1 * 2.e0 * njac[k+0][3][4];
               lhs[k+0][BB-1][4][4] = 1.e0 + tmp1 * 2.e0 * njac[k+0][4][4]               + tmp1 * 2.e0 * dz5;

               lhs[k+0][CC-1][0][0] =  tmp2 * fjac[k+1+0][0][0]              - tmp1 * njac[k+1+0][0][0]              - tmp1 * dz1;
               lhs[k+0][CC-1][1][0] =  tmp2 * fjac[k+1+0][1][0]              - tmp1 * njac[k+1+0][1][0];
               lhs[k+0][CC-1][2][0] =  tmp2 * fjac[k+1+0][2][0]              - tmp1 * njac[k+1+0][2][0];
               lhs[k+0][CC-1][3][0] =  tmp2 * fjac[k+1+0][3][0]              - tmp1 * njac[k+1+0][3][0];
               lhs[k+0][CC-1][4][0] =  tmp2 * fjac[k+1+0][4][0]              - tmp1 * njac[k+1+0][4][0];

               lhs[k+0][CC-1][0][1] =  tmp2 * fjac[k+1+0][0][1]              - tmp1 * njac[k+1+0][0][1];
               lhs[k+0][CC-1][1][1] =  tmp2 * fjac[k+1+0][1][1]              - tmp1 * njac[k+1+0][1][1]              - tmp1 * dz2;
               lhs[k+0][CC-1][2][1] =  tmp2 * fjac[k+1+0][2][1]              - tmp1 * njac[k+1+0][2][1];
               lhs[k+0][CC-1][3][1] =  tmp2 * fjac[k+1+0][3][1]              - tmp1 * njac[k+1+0][3][1];
               lhs[k+0][CC-1][4][1] =  tmp2 * fjac[k+1+0][4][1]              - tmp1 * njac[k+1+0][4][1];

               lhs[k+0][CC-1][0][2] =  tmp2 * fjac[k+1+0][0][2]              - tmp1 * njac[k+1+0][0][2];
               lhs[k+0][CC-1][1][2] =  tmp2 * fjac[k+1+0][1][2]              - tmp1 * njac[k+1+0][1][2];
               lhs[k+0][CC-1][2][2] =  tmp2 * fjac[k+1+0][2][2]              - tmp1 * njac[k+1+0][2][2]              - tmp1 * dz3;
               lhs[k+0][CC-1][3][2] =  tmp2 * fjac[k+1+0][3][2]              - tmp1 * njac[k+1+0][3][2];
               lhs[k+0][CC-1][4][2] =  tmp2 * fjac[k+1+0][4][2]              - tmp1 * njac[k+1+0][4][2];

               lhs[k+0][CC-1][0][3] =  tmp2 * fjac[k+1+0][0][3]              - tmp1 * njac[k+1+0][0][3];
               lhs[k+0][CC-1][1][3] =  tmp2 * fjac[k+1+0][1][3]              - tmp1 * njac[k+1+0][1][3];
               lhs[k+0][CC-1][2][3] =  tmp2 * fjac[k+1+0][2][3]              - tmp1 * njac[k+1+0][2][3];
               lhs[k+0][CC-1][3][3] =  tmp2 * fjac[k+1+0][3][3]              - tmp1 * njac[k+1+0][3][3]              - tmp1 * dz4;
               lhs[k+0][CC-1][4][3] =  tmp2 * fjac[k+1+0][4][3]              - tmp1 * njac[k+1+0][4][3];

               lhs[k+0][CC-1][0][4] =  tmp2 * fjac[k+1+0][0][4]              - tmp1 * njac[k+1+0][0][4];
               lhs[k+0][CC-1][1][4] =  tmp2 * fjac[k+1+0][1][4]              - tmp1 * njac[k+1+0][1][4];
               lhs[k+0][CC-1][2][4] =  tmp2 * fjac[k+1+0][2][4]              - tmp1 * njac[k+1+0][2][4];
               lhs[k+0][CC-1][3][4] =  tmp2 * fjac[k+1+0][3][4]              - tmp1 * njac[k+1+0][3][4];
               lhs[k+0][CC-1][4][4] =  tmp2 * fjac[k+1+0][4][4]              - tmp1 * njac[k+1+0][4][4]              - tmp1 * dz5;

            }

            do (k , 0, ksize,1) {
               rtmp[k+0][0] = rhs[k+0][j+0][i+0][0];
               rtmp[k+0][1] = rhs[k+0][j+0][i+0][1];
               rtmp[k+0][2] = rhs[k+0][j+0][i+0][2];
               rtmp[k+0][3] = rhs[k+0][j+0][i+0][3];
               rtmp[k+0][4] = rhs[k+0][j+0][i+0][4];
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
//     c'(KMAX) and rhs'(KMAX) will be sent to next cell.
//---------------------------------------------------------------------

//---------------------------------------------------------------------
//     outer most do loops - sweeping in i direction
//---------------------------------------------------------------------

//---------------------------------------------------------------------
//     multiply c(i,j,0) by b_inverse and copy back to c
//     multiply rhs(0) by b_inverse(0) and copy to rhs
//---------------------------------------------------------------------
            binvcrhs (&lhs[0+0][BB-1][0][0],&lhs[0+0][CC-1][0][0],&rtmp[0+0][0] );


//---------------------------------------------------------------------
//     begin inner most do loop
//     do all the elements of the cell unless last 
//---------------------------------------------------------------------
            do (k,1,ksize-1,1) {

//---------------------------------------------------------------------
//     subtract A*lhs_vector(k-1) from lhs_vector(k)
//     
//     rhs(k) = rhs(k) - A*rhs(k-1)
//---------------------------------------------------------------------
               matvec_sub (&lhs[k+0][AA-1][0][0],&rtmp[k-1+0][0],&rtmp[k+0][0]);

//---------------------------------------------------------------------
//     B(k) = B(k) - C(k-1)*A(k)
//     call matmul_sub(aa,i,j,k,c,cc,i,j,k-1,c,bb,i,j,k)
//---------------------------------------------------------------------
               matmul_sub (&lhs[k+0][AA-1][0][0],&lhs[k-1+0][CC-1][0][0],&lhs[k+0][BB-1][0][0]);

//---------------------------------------------------------------------
//     multiply c(i,j,k) by b_inverse and copy back to c
//     multiply rhs(i,j,1) by b_inverse(i,j,1) and copy to rhs
//---------------------------------------------------------------------
               binvcrhs (&lhs[k+0][BB-1][0][0],&lhs[k+0][CC-1][0][0],&rtmp[k+0][0] );

            }

//---------------------------------------------------------------------
//     Now finish up special cases for last cell
//---------------------------------------------------------------------

//---------------------------------------------------------------------
//     rhs(ksize) = rhs(ksize) - A*rhs(ksize-1)
//---------------------------------------------------------------------
            matvec_sub (&lhs[ksize+0][AA-1][0][0],&rtmp[ksize-1+0][0],&rtmp[ksize+0][0]);

//---------------------------------------------------------------------
//     B(ksize) = B(ksize) - C(ksize-1)*A(ksize)
//     call matmul_sub(aa,i,j,ksize,c,
//     $              cc,i,j,ksize-1,c,bb,i,j,ksize)
//---------------------------------------------------------------------
            matmul_sub (&lhs[ksize+0][AA-1][0][0],&lhs[ksize-1+0][CC-1][0][0],&lhs[ksize+0][BB-1][0][0]);

//---------------------------------------------------------------------
//     multiply rhs(ksize) by b_inverse(ksize) and copy to rhs
//---------------------------------------------------------------------
            binvrhs (&lhs[ksize+0][BB-1][0][0],&rtmp[ksize+0][0] );


//---------------------------------------------------------------------
//---------------------------------------------------------------------

//---------------------------------------------------------------------
//     back solve: if last cell, then generate U(ksize)=rhs(ksize)
//     else assume U(ksize) is loaded in un pack backsub_info
//     so just use it
//     after call u(kstart) will be sent to next cell
//---------------------------------------------------------------------

            dom (k,ksize-1,0,-1) {
               do (m,1,BLOCK_SIZE,1) {
                  do (n,1,BLOCK_SIZE,1) {
                     rtmp[k+0][m-1] = rtmp[k+0][m-1]                     - lhs[k+0][CC-1][n-1][m-1]*rtmp[k+1+0][n-1];
                  }
                  rhs[k+0][j+0][i+0][m-1] = rtmp[k+0][m-1];
               }
            }

         }
      }
//$omp end do
 
 //#pragma omp end do
      if (timeron) timer_stop (T_ZSOLVE);

      return;
}//end
