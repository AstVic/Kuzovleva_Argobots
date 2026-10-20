#include "header.h"

void exact_rhs (oforcing,nx,nxmax,ny,nz)
// beg param
       void *oforcing;
       int nx;
       int nxmax;
       int ny;
       int nz;
// end param exact_rhs
{
double (*forcing)[(ny-1)-(0)+1][(nxmax-1)-(0)+1][5] = (double (*)[(ny-1)-(0)+1][(nxmax-1)-(0)+1][5])oforcing;


//---------------------------------------------------------------------
//---------------------------------------------------------------------

//---------------------------------------------------------------------
//     compute the right hand side based on exact solution
//---------------------------------------------------------------------


      double dtemp[5];
      double xi;
      double eta;
      double zeta;
      double dtpp;
      int m;
      int i;
      int j;
      int k;
      int ip1;
      int im1;
      int jp1;
      int jm1;
      int km1;
      int kp1;

//---------------------------------------------------------------------
//     initialize                                  
//---------------------------------------------------------------------
//$omp do schedule(static)
 
#pragma omp for schedule(static)
      do (k, 0, nz-1,1) {
         do (j , 0, ny-1,1) {
            do (i , 0, nx-1,1) {
               do (m , 1, 5,1) {
                  forcing[k+0][j+0][i+0][m-1] = 0.0e0;
               }
            }
         }
      }
//$omp end do
 
 //#pragma omp end do

//---------------------------------------------------------------------
//     xi-direction flux differences                      
//---------------------------------------------------------------------
//$omp do schedule(static)
 
#pragma omp for schedule(static) nowait
      do (k , 1, nz-2,1) {
         zeta = (double) (k) *dnzm1;
         do (j , 1, ny-2,1) {
            eta = (double) (j) *dnym1;

            do (i,0, nx-1,1) {
               xi = (double) (i) *dnxm1;

               exact_solution (xi,eta,zeta, dtemp);
               do (m , 1, 5,1) {
                  ue[m-1][i+0] = dtemp[m-1];
               }

               dtpp = 1.0e0 / dtemp[0];

               do (m , 2, 5,1) {
                  buf[m-1][i+0] = dtpp * dtemp[m-1];
               }

               cuf[i+0]   = buf[1][i+0] * buf[1][i+0];
               buf[0][i+0] = cuf[i+0] + buf[2][i+0] * buf[2][i+0] + buf[3][i+0] * buf[3][i+0];
               q[i+0] = 0.5e0*(buf[1][i+0]*ue[1][i+0] + buf[2][i+0]*ue[2][i+0] + buf[3][i+0]*ue[3][i+0]);

            }

            do (i , 1, nx-2,1) {
               im1 = i-1;
               ip1 = i+1;

               forcing[k+0][j+0][i+0][0] = forcing[k+0][j+0][i+0][0] - tx2*( ue[1][ip1+0]-ue[1][im1+0] )+ dx1tx1*(ue[0][ip1+0]-2.0e0*ue[0][i+0]+ue[0][im1+0]);

               forcing[k+0][j+0][i+0][1] = forcing[k+0][j+0][i+0][1] - tx2 *((ue[1][ip1+0]*buf[1][ip1+0]+c2*(ue[4][ip1+0]-q[ip1+0]))-(ue[1][im1+0]*buf[1][im1+0]+c2*(ue[4][im1+0]-q[im1+0])))+ xxcon1*(buf[1][ip1+0]-2.0e0*buf[1][i+0]+buf[1][im1+0])+ dx2tx1*( ue[1][ip1+0]-2.0e0* ue[1][i+0]+ue[1][im1+0]);

               forcing[k+0][j+0][i+0][2] = forcing[k+0][j+0][i+0][2] - tx2 *(                 ue[2][ip1+0]*buf[1][ip1+0]-ue[2][im1+0]*buf[1][im1+0])+ xxcon2*(buf[2][ip1+0]-2.0e0*buf[2][i+0]+buf[2][im1+0])+ dx3tx1*( ue[2][ip1+0]-2.0e0*ue[2][i+0] +ue[2][im1+0]);

               forcing[k+0][j+0][i+0][3] = forcing[k+0][j+0][i+0][3] - tx2*(                 ue[3][ip1+0]*buf[1][ip1+0]-ue[3][im1+0]*buf[1][im1+0])+ xxcon2*(buf[3][ip1+0]-2.0e0*buf[3][i+0]+buf[3][im1+0])+ dx4tx1*( ue[3][ip1+0]-2.0e0* ue[3][i+0]+ ue[3][im1+0]);

               forcing[k+0][j+0][i+0][4] = forcing[k+0][j+0][i+0][4] - tx2*(                 buf[1][ip1+0]*(c1*ue[4][ip1+0]-c2*q[ip1+0])- buf[1][im1+0]*(c1*ue[4][im1+0]-c2*q[im1+0]))+ 0.5e0*xxcon3*(buf[0][ip1+0]-2.0e0*buf[0][i+0]+ buf[0][im1+0])+ xxcon4*(cuf[ip1+0]-2.0e0*cuf[i+0]+cuf[im1+0])+ xxcon5*(buf[4][ip1+0]-2.0e0*buf[4][i+0]+buf[4][im1+0])+ dx5tx1*( ue[4][ip1+0]-2.0e0* ue[4][i+0]+ ue[4][im1+0]);
            }

//---------------------------------------------------------------------
//     Fourth-order dissipation                         
//---------------------------------------------------------------------

            do (m , 1, 5,1) {
               i = 1;
               forcing[k+0][j+0][i+0][m-1] = forcing[k+0][j+0][i+0][m-1] - dssp *(5.0e0*ue[m-1][i+0] - 4.0e0*ue[m-1][i+1+0] +ue[m-1][i+2+0]);
               i = 2;
               forcing[k+0][j+0][i+0][m-1] = forcing[k+0][j+0][i+0][m-1] - dssp *(-4.0e0*ue[m-1][i-1+0] + 6.0e0*ue[m-1][i+0] - 4.0e0*ue[m-1][i+1+0] + ue[m-1][i+2+0]);
            }

            do (m , 1, 5,1) {
               do (i , 3, nx-4,1) {
                  forcing[k+0][j+0][i+0][m-1] = forcing[k+0][j+0][i+0][m-1] - dssp*(ue[m-1][i-2+0] - 4.0e0*ue[m-1][i-1+0] + 6.0e0*ue[m-1][i+0] - 4.0e0*ue[m-1][i+1+0] + ue[m-1][i+2+0]);
               }
            }

            do (m , 1, 5,1) {
               i = nx-3;
               forcing[k+0][j+0][i+0][m-1] = forcing[k+0][j+0][i+0][m-1] - dssp *(ue[m-1][i-2+0] - 4.0e0*ue[m-1][i-1+0] + 6.0e0*ue[m-1][i+0] - 4.0e0*ue[m-1][i+1+0]);
               i = nx-2;
               forcing[k+0][j+0][i+0][m-1] = forcing[k+0][j+0][i+0][m-1] - dssp *(ue[m-1][i-2+0] - 4.0e0*ue[m-1][i-1+0] + 5.0e0*ue[m-1][i+0]);
            }

         }
      }
//$omp end do nowait
 
 //#pragma omp end do nowait

//---------------------------------------------------------------------
//     eta-direction flux differences             
//---------------------------------------------------------------------
//$omp do schedule(static)
 
#pragma omp for schedule(static)
      do (k , 1, nz-2,1) {
         zeta = (double) (k) *dnzm1;
         do (i,1, nx-2,1) {
            xi = (double) (i) *dnxm1;

            do (j,0, ny-1,1) {
               eta = (double) (j) *dnym1;

               exact_solution (xi,eta,zeta, dtemp);
               do (m , 1, 5,1) {
                  ue[m-1][j+0] = dtemp[m-1];
               }

               dtpp = 1.0e0/dtemp[0];

               do (m , 2, 5,1) {
                  buf[m-1][j+0] = dtpp * dtemp[m-1];
               }

               cuf[j+0]   = buf[2][j+0] * buf[2][j+0];
               buf[0][j+0] = cuf[j+0] + buf[1][j+0] * buf[1][j+0] + buf[3][j+0] * buf[3][j+0];
               q[j+0] = 0.5e0*(buf[1][j+0]*ue[1][j+0] + buf[2][j+0]*ue[2][j+0] + buf[3][j+0]*ue[3][j+0]);
            }

            do (j , 1, ny-2,1) {
               jm1 = j-1;
               jp1 = j+1;

               forcing[k+0][j+0][i+0][0] = forcing[k+0][j+0][i+0][0] - ty2*( ue[2][jp1+0]-ue[2][jm1+0] )+ dy1ty1*(ue[0][jp1+0]-2.0e0*ue[0][j+0]+ue[0][jm1+0]);

               forcing[k+0][j+0][i+0][1] = forcing[k+0][j+0][i+0][1] - ty2*(                 ue[1][jp1+0]*buf[2][jp1+0]-ue[1][jm1+0]*buf[2][jm1+0])+ yycon2*(buf[1][jp1+0]-2.0e0*buf[1][j+0]+buf[1][jm1+0])+ dy2ty1*( ue[1][jp1+0]-2.0* ue[1][j+0]+ ue[1][jm1+0]);

               forcing[k+0][j+0][i+0][2] = forcing[k+0][j+0][i+0][2] - ty2*((ue[2][jp1+0]*buf[2][jp1+0]+c2*(ue[4][jp1+0]-q[jp1+0]))-(ue[2][jm1+0]*buf[2][jm1+0]+c2*(ue[4][jm1+0]-q[jm1+0])))+ yycon1*(buf[2][jp1+0]-2.0e0*buf[2][j+0]+buf[2][jm1+0])+ dy3ty1*( ue[2][jp1+0]-2.0e0*ue[2][j+0] +ue[2][jm1+0]);

               forcing[k+0][j+0][i+0][3] = forcing[k+0][j+0][i+0][3] - ty2*(                 ue[3][jp1+0]*buf[2][jp1+0]-ue[3][jm1+0]*buf[2][jm1+0])+ yycon2*(buf[3][jp1+0]-2.0e0*buf[3][j+0]+buf[3][jm1+0])+ dy4ty1*( ue[3][jp1+0]-2.0e0*ue[3][j+0]+ ue[3][jm1+0]);

               forcing[k+0][j+0][i+0][4] = forcing[k+0][j+0][i+0][4] - ty2*(                 buf[2][jp1+0]*(c1*ue[4][jp1+0]-c2*q[jp1+0])- buf[2][jm1+0]*(c1*ue[4][jm1+0]-c2*q[jm1+0]))+ 0.5e0*yycon3*(buf[0][jp1+0]-2.0e0*buf[0][j+0]+ buf[0][jm1+0])+ yycon4*(cuf[jp1+0]-2.0e0*cuf[j+0]+cuf[jm1+0])+ yycon5*(buf[4][jp1+0]-2.0e0*buf[4][j+0]+buf[4][jm1+0])+ dy5ty1*(ue[4][jp1+0]-2.0e0*ue[4][j+0]+ue[4][jm1+0]);
            }

//---------------------------------------------------------------------
//     Fourth-order dissipation                      
//---------------------------------------------------------------------
            do (m , 1, 5,1) {
               j = 1;
               forcing[k+0][j+0][i+0][m-1] = forcing[k+0][j+0][i+0][m-1] - dssp *(5.0e0*ue[m-1][j+0] - 4.0e0*ue[m-1][j+1+0] +ue[m-1][j+2+0]);
               j = 2;
               forcing[k+0][j+0][i+0][m-1] = forcing[k+0][j+0][i+0][m-1] - dssp *(-4.0e0*ue[m-1][j-1+0] + 6.0e0*ue[m-1][j+0] - 4.0e0*ue[m-1][j+1+0] + ue[m-1][j+2+0]);
            }

            do (m , 1, 5,1) {
               do (j , 3, ny-4,1) {
                  forcing[k+0][j+0][i+0][m-1] = forcing[k+0][j+0][i+0][m-1] - dssp*(ue[m-1][j-2+0] - 4.0e0*ue[m-1][j-1+0] + 6.0e0*ue[m-1][j+0] - 4.0e0*ue[m-1][j+1+0] + ue[m-1][j+2+0]);
               }
            }

            do (m , 1, 5,1) {
               j = ny-3;
               forcing[k+0][j+0][i+0][m-1] = forcing[k+0][j+0][i+0][m-1] - dssp *(ue[m-1][j-2+0] - 4.0e0*ue[m-1][j-1+0] + 6.0e0*ue[m-1][j+0] - 4.0e0*ue[m-1][j+1+0]);
               j = ny-2;
               forcing[k+0][j+0][i+0][m-1] = forcing[k+0][j+0][i+0][m-1] - dssp *(ue[m-1][j-2+0] - 4.0e0*ue[m-1][j-1+0] + 5.0e0*ue[m-1][j+0]);

            }

         }
      }
//$omp end do
 
 //#pragma omp end do

//---------------------------------------------------------------------
//     zeta-direction flux differences                      
//---------------------------------------------------------------------
//$omp do schedule(static)
 
#pragma omp for schedule(static)
      do (j,1, ny-2,1) {
         eta = (double) (j) *dnym1;
         do (i , 1, nx-2,1) {
            xi = (double) (i) *dnxm1;

            do (k,0, nz-1,1) {
               zeta = (double) (k) *dnzm1;

               exact_solution (xi,eta,zeta, dtemp);
               do (m , 1, 5,1) {
                  ue[m-1][k+0] = dtemp[m-1];
               }

               dtpp = 1.0e0/dtemp[0];

               do (m , 2, 5,1) {
                  buf[m-1][k+0] = dtpp * dtemp[m-1];
               }

               cuf[k+0]   = buf[3][k+0] * buf[3][k+0];
               buf[0][k+0] = cuf[k+0] + buf[1][k+0] * buf[1][k+0] + buf[2][k+0] * buf[2][k+0];
               q[k+0] = 0.5e0*(buf[1][k+0]*ue[1][k+0] + buf[2][k+0]*ue[2][k+0] + buf[3][k+0]*ue[3][k+0]);
            }

            do (k,1, nz-2,1) {
               km1 = k-1;
               kp1 = k+1;

               forcing[k+0][j+0][i+0][0] = forcing[k+0][j+0][i+0][0] - tz2*( ue[3][kp1+0]-ue[3][km1+0] )+ dz1tz1*(ue[0][kp1+0]-2.0e0*ue[0][k+0]+ue[0][km1+0]);

               forcing[k+0][j+0][i+0][1] = forcing[k+0][j+0][i+0][1] - tz2 *(                 ue[1][kp1+0]*buf[3][kp1+0]-ue[1][km1+0]*buf[3][km1+0])+ zzcon2*(buf[1][kp1+0]-2.0e0*buf[1][k+0]+buf[1][km1+0])+ dz2tz1*( ue[1][kp1+0]-2.0e0* ue[1][k+0]+ ue[1][km1+0]);

               forcing[k+0][j+0][i+0][2] = forcing[k+0][j+0][i+0][2] - tz2 *(                 ue[2][kp1+0]*buf[3][kp1+0]-ue[2][km1+0]*buf[3][km1+0])+ zzcon2*(buf[2][kp1+0]-2.0e0*buf[2][k+0]+buf[2][km1+0])+ dz3tz1*(ue[2][kp1+0]-2.0e0*ue[2][k+0]+ue[2][km1+0]);

               forcing[k+0][j+0][i+0][3] = forcing[k+0][j+0][i+0][3] - tz2 *((ue[3][kp1+0]*buf[3][kp1+0]+c2*(ue[4][kp1+0]-q[kp1+0]))-(ue[3][km1+0]*buf[3][km1+0]+c2*(ue[4][km1+0]-q[km1+0])))+ zzcon1*(buf[3][kp1+0]-2.0e0*buf[3][k+0]+buf[3][km1+0])+ dz4tz1*( ue[3][kp1+0]-2.0e0*ue[3][k+0] +ue[3][km1+0]);

               forcing[k+0][j+0][i+0][4] = forcing[k+0][j+0][i+0][4] - tz2 *(                 buf[3][kp1+0]*(c1*ue[4][kp1+0]-c2*q[kp1+0])- buf[3][km1+0]*(c1*ue[4][km1+0]-c2*q[km1+0]))+ 0.5e0*zzcon3*(buf[0][kp1+0]-2.0e0*buf[0][k+0]                 +buf[0][km1+0])+ zzcon4*(cuf[kp1+0]-2.0e0*cuf[k+0]+cuf[km1+0])+ zzcon5*(buf[4][kp1+0]-2.0e0*buf[4][k+0]+buf[4][km1+0])+ dz5tz1*( ue[4][kp1+0]-2.0e0*ue[4][k+0]+ ue[4][km1+0]);
            }

//---------------------------------------------------------------------
//     Fourth-order dissipation                        
//---------------------------------------------------------------------
            do (m , 1, 5,1) {
               k = 1;
               forcing[k+0][j+0][i+0][m-1] = forcing[k+0][j+0][i+0][m-1] - dssp *(5.0e0*ue[m-1][k+0] - 4.0e0*ue[m-1][k+1+0] +ue[m-1][k+2+0]);
               k = 2;
               forcing[k+0][j+0][i+0][m-1] = forcing[k+0][j+0][i+0][m-1] - dssp *(-4.0e0*ue[m-1][k-1+0] + 6.0e0*ue[m-1][k+0] - 4.0e0*ue[m-1][k+1+0] + ue[m-1][k+2+0]);
            }

            do (m , 1, 5,1) {
               do (k , 3, nz-4,1) {
                  forcing[k+0][j+0][i+0][m-1] = forcing[k+0][j+0][i+0][m-1] - dssp*(ue[m-1][k-2+0] - 4.0e0*ue[m-1][k-1+0] + 6.0e0*ue[m-1][k+0] - 4.0e0*ue[m-1][k+1+0] + ue[m-1][k+2+0]);
               }
            }

            do (m , 1, 5,1) {
               k = nz-3;
               forcing[k+0][j+0][i+0][m-1] = forcing[k+0][j+0][i+0][m-1] - dssp *(ue[m-1][k-2+0] - 4.0e0*ue[m-1][k-1+0] + 6.0e0*ue[m-1][k+0] - 4.0e0*ue[m-1][k+1+0]);
               k = nz-2;
               forcing[k+0][j+0][i+0][m-1] = forcing[k+0][j+0][i+0][m-1] - dssp *(ue[m-1][k-2+0] - 4.0e0*ue[m-1][k-1+0] + 5.0e0*ue[m-1][k+0]);
            }

         }
      }
//$omp end do
 
 //#pragma omp end do

//---------------------------------------------------------------------
//     now change the sign of the forcing function, 
//---------------------------------------------------------------------
//$omp do schedule(static)
 
#pragma omp for schedule(static)
      do (k , 1, nz-2,1) {
         do (j , 1, ny-2,1) {
            do (i , 1, nx-2,1) {
               do (m , 1, 5,1) {
                  forcing[k+0][j+0][i+0][m-1] = -1.e0 * forcing[k+0][j+0][i+0][m-1];
               }
            }
         }
      }
//$omp end do
 
 //#pragma omp end do


      return;
}//end

//---------------------------------------------------------------------
//---------------------------------------------------------------------

