#include "header.h"

void compute_rhs (orho_i,ous,ovs,ows,oqs,osquare,orhs,oforcing,ou,nx,nxmax,ny,nz)
// beg param
       void *orho_i;
       void *ous;
       void *ovs;
       void *ows;
       void *oqs;
       void *osquare;
       void *orhs;
       void *oforcing;
       void *ou;
       int nx;
       int nxmax;
       int ny;
       int nz;
// end param compute_rhs
{
double (*u)[(ny-1)-(0)+1][(nxmax-1)-(0)+1][5] = (double (*)[(ny-1)-(0)+1][(nxmax-1)-(0)+1][5])ou;
double (*forcing)[(ny-1)-(0)+1][(nxmax-1)-(0)+1][5] = (double (*)[(ny-1)-(0)+1][(nxmax-1)-(0)+1][5])oforcing;
double (*rhs)[(ny-1)-(0)+1][(nxmax-1)-(0)+1][5] = (double (*)[(ny-1)-(0)+1][(nxmax-1)-(0)+1][5])orhs;
double (*square)[(ny-1)-(0)+1][(nxmax-1)-(0)+1] = (double (*)[(ny-1)-(0)+1][(nxmax-1)-(0)+1])osquare;
double (*qs)[(ny-1)-(0)+1][(nxmax-1)-(0)+1] = (double (*)[(ny-1)-(0)+1][(nxmax-1)-(0)+1])oqs;
double (*ws)[(ny-1)-(0)+1][(nxmax-1)-(0)+1] = (double (*)[(ny-1)-(0)+1][(nxmax-1)-(0)+1])ows;
double (*vs)[(ny-1)-(0)+1][(nxmax-1)-(0)+1] = (double (*)[(ny-1)-(0)+1][(nxmax-1)-(0)+1])ovs;
double (*us)[(ny-1)-(0)+1][(nxmax-1)-(0)+1] = (double (*)[(ny-1)-(0)+1][(nxmax-1)-(0)+1])ous;
double (*rho_i)[(ny-1)-(0)+1][(nxmax-1)-(0)+1] = (double (*)[(ny-1)-(0)+1][(nxmax-1)-(0)+1])orho_i;


//---------------------------------------------------------------------
//---------------------------------------------------------------------



      int i;
      int j;
      int k;
      int m;
      double rho_inv;
      double uijk;
      double up1;
      double um1;
      double vijk;
      double vp1;
      double vm1;
      double wijk;
      double wp1;
      double wm1;

      if (timeron) timer_start (T_RHS);
//---------------------------------------------------------------------
//     compute the reciprocal of density and the kinetic energy, 
//---------------------------------------------------------------------
//$omp do schedule(static)
 
#pragma omp for schedule(static) nowait
      do (k , 0, nz-1,1) {
         do (j , 0, ny-1,1) {
            do (i , 0, nx-1,1) {
               rho_inv = 1.0e0/u[k+0][j+0][i+0][0];
               rho_i[k+0][j+0][i+0] = rho_inv;
               us[k+0][j+0][i+0] = u[k+0][j+0][i+0][1] * rho_inv;
               vs[k+0][j+0][i+0] = u[k+0][j+0][i+0][2] * rho_inv;
               ws[k+0][j+0][i+0] = u[k+0][j+0][i+0][3] * rho_inv;
               square[k+0][j+0][i+0]     = 0.5e0*(                 u[k+0][j+0][i+0][1]*u[k+0][j+0][i+0][1] + u[k+0][j+0][i+0][2]*u[k+0][j+0][i+0][2] + u[k+0][j+0][i+0][3]*u[k+0][j+0][i+0][3] ) * rho_inv;
               qs[k+0][j+0][i+0] = square[k+0][j+0][i+0] * rho_inv;
            }
         }
      }
//$omp end do nowait
 
 //#pragma omp end do nowait

//---------------------------------------------------------------------
// copy the exact forcing term to the right hand side;  because 
// this forcing term is known, we can store it on the whole zone
// including the boundary                   
//---------------------------------------------------------------------

//$omp do schedule(static)
 
#pragma omp for schedule(static)
      do (k , 0, nz-1,1) {
         do (j , 0, ny-1,1) {
            do (i , 0, nx-1,1) {
               do (m , 1, 5,1) {
                  rhs[k+0][j+0][i+0][m-1] = forcing[k+0][j+0][i+0][m-1];
               }
            }
         }
      }
//$omp end do
 
 //#pragma omp end do

      if (timeron) timer_start (T_RHSX);
//---------------------------------------------------------------------
//     compute xi-direction fluxes 
//---------------------------------------------------------------------
//$omp do schedule(static)
 
#pragma omp for schedule(static) nowait
      do (k , 1, nz-2,1) {
         do (j , 1, ny-2,1) {
            do (i , 1, nx-2,1) {
               uijk = us[k+0][j+0][i+0];
               up1  = us[k+0][j+0][i+1+0];
               um1  = us[k+0][j+0][i-1+0];

               rhs[k+0][j+0][i+0][0] = rhs[k+0][j+0][i+0][0] + dx1tx1 *(u[k+0][j+0][i+1+0][0] - 2.0e0*u[k+0][j+0][i+0][0] + u[k+0][j+0][i-1+0][0]) - tx2 *(u[k+0][j+0][i+1+0][1] - u[k+0][j+0][i-1+0][1]);

               rhs[k+0][j+0][i+0][1] = rhs[k+0][j+0][i+0][1] + dx2tx1 *(u[k+0][j+0][i+1+0][1] - 2.0e0*u[k+0][j+0][i+0][1] + u[k+0][j+0][i-1+0][1]) + xxcon2*con43 *(up1 - 2.0e0*uijk + um1) - tx2 *(u[k+0][j+0][i+1+0][1]*up1 - u[k+0][j+0][i-1+0][1]*um1 +(u[k+0][j+0][i+1+0][4]- square[k+0][j+0][i+1+0]- u[k+0][j+0][i-1+0][4]+ square[k+0][j+0][i-1+0])*                 c2);

               rhs[k+0][j+0][i+0][2] = rhs[k+0][j+0][i+0][2] + dx3tx1 *(u[k+0][j+0][i+1+0][2] - 2.0e0*u[k+0][j+0][i+0][2] + u[k+0][j+0][i-1+0][2]) + xxcon2 *(vs[k+0][j+0][i+1+0] - 2.0e0*vs[k+0][j+0][i+0] + vs[k+0][j+0][i-1+0]) - tx2 *(u[k+0][j+0][i+1+0][2]*up1 - u[k+0][j+0][i-1+0][2]*um1);

               rhs[k+0][j+0][i+0][3] = rhs[k+0][j+0][i+0][3] + dx4tx1 *(u[k+0][j+0][i+1+0][3] - 2.0e0*u[k+0][j+0][i+0][3] + u[k+0][j+0][i-1+0][3]) + xxcon2 *(ws[k+0][j+0][i+1+0] - 2.0e0*ws[k+0][j+0][i+0] + ws[k+0][j+0][i-1+0]) - tx2 *(u[k+0][j+0][i+1+0][3]*up1 - u[k+0][j+0][i-1+0][3]*um1);

               rhs[k+0][j+0][i+0][4] = rhs[k+0][j+0][i+0][4] + dx5tx1 *(u[k+0][j+0][i+1+0][4] - 2.0e0*u[k+0][j+0][i+0][4] + u[k+0][j+0][i-1+0][4]) + xxcon3 *(qs[k+0][j+0][i+1+0] - 2.0e0*qs[k+0][j+0][i+0] + qs[k+0][j+0][i-1+0]) + xxcon4 *(up1*up1 - 2.0e0*uijk*uijk + um1*um1) + xxcon5 *(u[k+0][j+0][i+1+0][4]*rho_i[k+0][j+0][i+1+0] - 2.0e0*u[k+0][j+0][i+0][4]*rho_i[k+0][j+0][i+0] + u[k+0][j+0][i-1+0][4]*rho_i[k+0][j+0][i-1+0]) - tx2 *((c1*u[k+0][j+0][i+1+0][4] - c2*square[k+0][j+0][i+1+0])*up1 -(c1*u[k+0][j+0][i-1+0][4] - c2*square[k+0][j+0][i-1+0])*um1 );
            }
         }

//---------------------------------------------------------------------
//     add fourth order xi-direction dissipation               
//---------------------------------------------------------------------
         do (j , 1, ny-2,1) {
            i = 1;
            do (m , 1, 5,1) {
               rhs[k+0][j+0][i+0][m-1] = rhs[k+0][j+0][i+0][m-1]- dssp *( 5.0e0*u[k+0][j+0][i+0][m-1] - 4.0e0*u[k+0][j+0][i+1+0][m-1] + u[k+0][j+0][i+2+0][m-1]);
            }

            i = 2;
            do (m , 1, 5,1) {
               rhs[k+0][j+0][i+0][m-1] = rhs[k+0][j+0][i+0][m-1] - dssp *(-4.0e0*u[k+0][j+0][i-1+0][m-1] + 6.0e0*u[k+0][j+0][i+0][m-1] - 4.0e0*u[k+0][j+0][i+1+0][m-1] + u[k+0][j+0][i+2+0][m-1]);
            }
         }

         do (j , 1, ny-2,1) {
            do (i , 3,nx-4,1) {
               do (m , 1, 5,1) {
                  rhs[k+0][j+0][i+0][m-1] = rhs[k+0][j+0][i+0][m-1] - dssp *(  u[k+0][j+0][i-2+0][m-1] - 4.0e0*u[k+0][j+0][i-1+0][m-1] + 6.0*u[k+0][j+0][i+0][m-1] - 4.0e0*u[k+0][j+0][i+1+0][m-1] + u[k+0][j+0][i+2+0][m-1] );
               }
            }
         }

         do (j , 1, ny-2,1) {
            i = nx-3;
            do (m , 1, 5,1) {
               rhs[k+0][j+0][i+0][m-1] = rhs[k+0][j+0][i+0][m-1] - dssp *( u[k+0][j+0][i-2+0][m-1] - 4.0e0*u[k+0][j+0][i-1+0][m-1] + 6.0e0*u[k+0][j+0][i+0][m-1] - 4.0e0*u[k+0][j+0][i+1+0][m-1] );
            }

            i = nx-2;
            do (m , 1, 5,1) {
               rhs[k+0][j+0][i+0][m-1] = rhs[k+0][j+0][i+0][m-1] - dssp *( u[k+0][j+0][i-2+0][m-1] - 4.e0*u[k+0][j+0][i-1+0][m-1] + 5.e0*u[k+0][j+0][i+0][m-1] );
            }
         }
      }
//$omp end do nowait
 
 //#pragma omp end do nowait
      if (timeron) timer_stop (T_RHSX);

      if (timeron) timer_start (T_RHSY);
//---------------------------------------------------------------------
//     compute eta-direction fluxes 
//---------------------------------------------------------------------
//$omp do schedule(static)
 
#pragma omp for schedule(static) nowait
      do (k , 1, nz-2,1) {
         do (j , 1, ny-2,1) {
            do (i , 1, nx-2,1) {
               vijk = vs[k+0][j+0][i+0];
               vp1  = vs[k+0][j+1+0][i+0];
               vm1  = vs[k+0][j-1+0][i+0];
               rhs[k+0][j+0][i+0][0] = rhs[k+0][j+0][i+0][0] + dy1ty1 *(u[k+0][j+1+0][i+0][0] - 2.0e0*u[k+0][j+0][i+0][0] + u[k+0][j-1+0][i+0][0]) - ty2 *(u[k+0][j+1+0][i+0][2] - u[k+0][j-1+0][i+0][2]);
               rhs[k+0][j+0][i+0][1] = rhs[k+0][j+0][i+0][1] + dy2ty1 *(u[k+0][j+1+0][i+0][1] - 2.0e0*u[k+0][j+0][i+0][1] + u[k+0][j-1+0][i+0][1]) + yycon2 *(us[k+0][j+1+0][i+0] - 2.0e0*us[k+0][j+0][i+0] + us[k+0][j-1+0][i+0]) - ty2 *(u[k+0][j+1+0][i+0][1]*vp1 - u[k+0][j-1+0][i+0][1]*vm1);
               rhs[k+0][j+0][i+0][2] = rhs[k+0][j+0][i+0][2] + dy3ty1 *(u[k+0][j+1+0][i+0][2] - 2.0e0*u[k+0][j+0][i+0][2] + u[k+0][j-1+0][i+0][2]) + yycon2*con43 *(vp1 - 2.0e0*vijk + vm1) - ty2 *(u[k+0][j+1+0][i+0][2]*vp1 - u[k+0][j-1+0][i+0][2]*vm1 +(u[k+0][j+1+0][i+0][4] - square[k+0][j+1+0][i+0] - u[k+0][j-1+0][i+0][4] + square[k+0][j-1+0][i+0])                 *c2);
               rhs[k+0][j+0][i+0][3] = rhs[k+0][j+0][i+0][3] + dy4ty1 *(u[k+0][j+1+0][i+0][3] - 2.0e0*u[k+0][j+0][i+0][3] + u[k+0][j-1+0][i+0][3]) + yycon2 *(ws[k+0][j+1+0][i+0] - 2.0e0*ws[k+0][j+0][i+0] + ws[k+0][j-1+0][i+0]) - ty2 *(u[k+0][j+1+0][i+0][3]*vp1 - u[k+0][j-1+0][i+0][3]*vm1);
               rhs[k+0][j+0][i+0][4] = rhs[k+0][j+0][i+0][4] + dy5ty1 *(u[k+0][j+1+0][i+0][4] - 2.0e0*u[k+0][j+0][i+0][4] + u[k+0][j-1+0][i+0][4]) + yycon3 *(qs[k+0][j+1+0][i+0] - 2.0e0*qs[k+0][j+0][i+0] + qs[k+0][j-1+0][i+0]) + yycon4 *(vp1*vp1       - 2.0e0*vijk*vijk + vm1*vm1) + yycon5 *(u[k+0][j+1+0][i+0][4]*rho_i[k+0][j+1+0][i+0] - 2.0e0*u[k+0][j+0][i+0][4]*rho_i[k+0][j+0][i+0] + u[k+0][j-1+0][i+0][4]*rho_i[k+0][j-1+0][i+0]) - ty2 *((c1*u[k+0][j+1+0][i+0][4] - c2*square[k+0][j+1+0][i+0]) * vp1 -(c1*u[k+0][j-1+0][i+0][4] - c2*square[k+0][j-1+0][i+0]) * vm1);
            }
         }

//---------------------------------------------------------------------
//     add fourth order eta-direction dissipation         
//---------------------------------------------------------------------
         j = 1;
         do (i , 1, nx-2,1) {
            do (m , 1, 5,1) {
               rhs[k+0][j+0][i+0][m-1] = rhs[k+0][j+0][i+0][m-1]- dssp *( 5.0e0*u[k+0][j+0][i+0][m-1] - 4.0e0*u[k+0][j+1+0][i+0][m-1] + u[k+0][j+2+0][i+0][m-1]);
            }
         }

         j = 2;
         do (i , 1, nx-2,1) {
            do (m , 1, 5,1) {
               rhs[k+0][j+0][i+0][m-1] = rhs[k+0][j+0][i+0][m-1] - dssp *(-4.0e0*u[k+0][j-1+0][i+0][m-1] + 6.0e0*u[k+0][j+0][i+0][m-1] - 4.0e0*u[k+0][j+1+0][i+0][m-1] + u[k+0][j+2+0][i+0][m-1]);
            }
         }

         do (j , 3, ny-4,1) {
            do (i , 1,nx-2,1) {
               do (m , 1, 5,1) {
                  rhs[k+0][j+0][i+0][m-1] = rhs[k+0][j+0][i+0][m-1] - dssp *(  u[k+0][j-2+0][i+0][m-1] - 4.0e0*u[k+0][j-1+0][i+0][m-1] + 6.0*u[k+0][j+0][i+0][m-1] - 4.0e0*u[k+0][j+1+0][i+0][m-1] + u[k+0][j+2+0][i+0][m-1] );
               }
            }
         }

         j = ny-3;
         do (i , 1, nx-2,1) {
            do (m , 1, 5,1) {
               rhs[k+0][j+0][i+0][m-1] = rhs[k+0][j+0][i+0][m-1] - dssp *( u[k+0][j-2+0][i+0][m-1] - 4.0e0*u[k+0][j-1+0][i+0][m-1] + 6.0e0*u[k+0][j+0][i+0][m-1] - 4.0e0*u[k+0][j+1+0][i+0][m-1] );
            }
         }

         j = ny-2;
         do (i , 1, nx-2,1) {
            do (m , 1, 5,1) {
               rhs[k+0][j+0][i+0][m-1] = rhs[k+0][j+0][i+0][m-1] - dssp *( u[k+0][j-2+0][i+0][m-1] - 4.e0*u[k+0][j-1+0][i+0][m-1] + 5.e0*u[k+0][j+0][i+0][m-1] );
            }
         }
      }
//$omp end do nowait
 
 //#pragma omp end do nowait
      if (timeron) timer_stop (T_RHSY);

      if (timeron) timer_start (T_RHSZ);
//---------------------------------------------------------------------
//     compute zeta-direction fluxes 
//---------------------------------------------------------------------
//$omp do schedule(static)
 
#pragma omp for schedule(static)
      do (k , 1, nz-2,1) {
         do (j , 1, ny-2,1) {
            do (i , 1, nx-2,1) {
               wijk = ws[k+0][j+0][i+0];
               wp1  = ws[k+1+0][j+0][i+0];
               wm1  = ws[k-1+0][j+0][i+0];

               rhs[k+0][j+0][i+0][0] = rhs[k+0][j+0][i+0][0] + dz1tz1 *(u[k+1+0][j+0][i+0][0] - 2.0e0*u[k+0][j+0][i+0][0] + u[k-1+0][j+0][i+0][0]) - tz2 *(u[k+1+0][j+0][i+0][3] - u[k-1+0][j+0][i+0][3]);
               rhs[k+0][j+0][i+0][1] = rhs[k+0][j+0][i+0][1] + dz2tz1 *(u[k+1+0][j+0][i+0][1] - 2.0e0*u[k+0][j+0][i+0][1] + u[k-1+0][j+0][i+0][1]) + zzcon2 *(us[k+1+0][j+0][i+0] - 2.0e0*us[k+0][j+0][i+0] + us[k-1+0][j+0][i+0]) - tz2 *(u[k+1+0][j+0][i+0][1]*wp1 - u[k-1+0][j+0][i+0][1]*wm1);
               rhs[k+0][j+0][i+0][2] = rhs[k+0][j+0][i+0][2] + dz3tz1 *(u[k+1+0][j+0][i+0][2] - 2.0e0*u[k+0][j+0][i+0][2] + u[k-1+0][j+0][i+0][2]) + zzcon2 *(vs[k+1+0][j+0][i+0] - 2.0e0*vs[k+0][j+0][i+0] + vs[k-1+0][j+0][i+0]) - tz2 *(u[k+1+0][j+0][i+0][2]*wp1 - u[k-1+0][j+0][i+0][2]*wm1);
               rhs[k+0][j+0][i+0][3] = rhs[k+0][j+0][i+0][3] + dz4tz1 *(u[k+1+0][j+0][i+0][3] - 2.0e0*u[k+0][j+0][i+0][3] + u[k-1+0][j+0][i+0][3]) + zzcon2*con43 *(wp1 - 2.0e0*wijk + wm1) - tz2 *(u[k+1+0][j+0][i+0][3]*wp1 - u[k-1+0][j+0][i+0][3]*wm1 +(u[k+1+0][j+0][i+0][4] - square[k+1+0][j+0][i+0] - u[k-1+0][j+0][i+0][4] + square[k-1+0][j+0][i+0])                 *c2);
               rhs[k+0][j+0][i+0][4] = rhs[k+0][j+0][i+0][4] + dz5tz1 *(u[k+1+0][j+0][i+0][4] - 2.0e0*u[k+0][j+0][i+0][4] + u[k-1+0][j+0][i+0][4]) + zzcon3 *(qs[k+1+0][j+0][i+0] - 2.0e0*qs[k+0][j+0][i+0] + qs[k-1+0][j+0][i+0]) + zzcon4 *(wp1*wp1 - 2.0e0*wijk*wijk + wm1*wm1) + zzcon5 *(u[k+1+0][j+0][i+0][4]*rho_i[k+1+0][j+0][i+0] - 2.0e0*u[k+0][j+0][i+0][4]*rho_i[k+0][j+0][i+0] + u[k-1+0][j+0][i+0][4]*rho_i[k-1+0][j+0][i+0]) - tz2 *((c1*u[k+1+0][j+0][i+0][4] - c2*square[k+1+0][j+0][i+0])*wp1 -(c1*u[k-1+0][j+0][i+0][4] - c2*square[k-1+0][j+0][i+0])*wm1);
            }
         }
      }
//$omp end do
 
 //#pragma omp end do

//---------------------------------------------------------------------
//     add fourth order zeta-direction dissipation                
//---------------------------------------------------------------------
      k = 1;
//$omp do schedule(static)
 
#pragma omp for schedule(static) nowait
      do (j , 1, ny-2,1) {
         do (i , 1, nx-2,1) {
            do (m , 1, 5,1) {
               rhs[k+0][j+0][i+0][m-1] = rhs[k+0][j+0][i+0][m-1]- dssp *( 5.0e0*u[k+0][j+0][i+0][m-1] - 4.0e0*u[k+1+0][j+0][i+0][m-1] + u[k+2+0][j+0][i+0][m-1]);
            }
         }
      }
//$omp end do nowait
 
 //#pragma omp end do nowait

      k = 2;
//$omp do schedule(static)
 
#pragma omp for schedule(static) nowait
      do (j , 1, ny-2,1) {
         do (i , 1, nx-2,1) {
            do (m , 1, 5,1) {
               rhs[k+0][j+0][i+0][m-1] = rhs[k+0][j+0][i+0][m-1] - dssp *(-4.0e0*u[k-1+0][j+0][i+0][m-1] + 6.0e0*u[k+0][j+0][i+0][m-1] - 4.0e0*u[k+1+0][j+0][i+0][m-1] + u[k+2+0][j+0][i+0][m-1]);
            }
         }
      }
//$omp end do nowait
 
 //#pragma omp end do nowait

//$omp do schedule(static)
 
#pragma omp for schedule(static) nowait
      do (k , 3, nz-4,1) {
         do (j , 1, ny-2,1) {
            do (i , 1,nx-2,1) {
               do (m , 1, 5,1) {
                  rhs[k+0][j+0][i+0][m-1] = rhs[k+0][j+0][i+0][m-1] - dssp *(  u[k-2+0][j+0][i+0][m-1] - 4.0e0*u[k-1+0][j+0][i+0][m-1] + 6.0*u[k+0][j+0][i+0][m-1] - 4.0e0*u[k+1+0][j+0][i+0][m-1] + u[k+2+0][j+0][i+0][m-1] );
               }
            }
         }
      }
//$omp end do nowait
 
 //#pragma omp end do nowait

      k = nz-3;
//$omp do schedule(static)
 
#pragma omp for schedule(static) nowait
      do (j , 1, ny-2,1) {
         do (i , 1, nx-2,1) {
            do (m , 1, 5,1) {
               rhs[k+0][j+0][i+0][m-1] = rhs[k+0][j+0][i+0][m-1] - dssp *( u[k-2+0][j+0][i+0][m-1] - 4.0e0*u[k-1+0][j+0][i+0][m-1] + 6.0e0*u[k+0][j+0][i+0][m-1] - 4.0e0*u[k+1+0][j+0][i+0][m-1] );
            }
         }
      }
//$omp end do nowait
 
 //#pragma omp end do nowait

      k = nz-2;
//$omp do schedule(static)
 
#pragma omp for schedule(static)
      do (j , 1, ny-2,1) {
         do (i , 1, nx-2,1) {
            do (m , 1, 5,1) {
               rhs[k+0][j+0][i+0][m-1] = rhs[k+0][j+0][i+0][m-1] - dssp *( u[k-2+0][j+0][i+0][m-1] - 4.e0*u[k-1+0][j+0][i+0][m-1] + 5.e0*u[k+0][j+0][i+0][m-1] );
            }
         }
      }
//$omp end do
 
 //#pragma omp end do
      if (timeron) timer_stop (T_RHSZ);

//$omp do schedule(static)
 
#pragma omp for schedule(static) nowait
      do (k , 1, nz-2,1) {
         do (j , 1, ny-2,1) {
            do (i , 1, nx-2,1) {
               do (m , 1, 5,1) {
                  rhs[k+0][j+0][i+0][m-1] = rhs[k+0][j+0][i+0][m-1] * dt;
               }
            }
         }
      }
//$omp end do nowait
 
 //#pragma omp end do nowait
      if (timeron) timer_stop (T_RHS);

      return;
}//end





//---------------------------------------------------------------------
//---------------------------------------------------------------------

