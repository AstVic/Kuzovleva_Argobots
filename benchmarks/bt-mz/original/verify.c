#include "header.h"

      double xce[5];
      double xcr[5];

void verify (no_time_steps,verified,num_zones,rho_i,us,vs,ws,qs,square,rhs,forcing,u,nx,nxmax,ny,nz,proc_zone_id,proc_num_zones)
// beg param
       int no_time_steps;
       logical (*verified);
       int num_zones;
       double rho_i[];
       double us[];
       double vs[];
       double ws[];
       double qs[];
       double square[];
       double rhs[];
       double forcing[];
       double u[];
       int nx[];
       int nxmax[];
       int ny[];
       int nz[];
       int proc_zone_id[];
       int proc_num_zones;
// end param
{

//---------------------------------------------------------------------
//---------------------------------------------------------------------

//---------------------------------------------------------------------
//  verification routine                         
//---------------------------------------------------------------------

#include "omp_stuff.h"

      int zone;

      double xcrref[5];
      double xceref[5];
      double xcrdif[5];
      double xcedif[5];
      double epsilon;
//      double xce[5];
//      double xcr[5];
      double dtref;
      double xce_sub[5];
      double xcr_sub[5];
      int m;
      int niterref;
      int iz;
      int nthreads;
//        save xce, xcr;

//---------------------------------------------------------------------
//   tolerance level
//---------------------------------------------------------------------
        epsilon = 1.0e-08;

//$omp master
 
#pragma omp master
{
        do (m , 1, 5,1) {
          xcr[m-1] = 0.e0;
          xce[m-1] = 0.e0;
        }
//$omp end master
 
} //#pragma omp end master
//$omp barrier
 
#pragma omp barrier

//---------------------------------------------------------------------
//   compute the error norm and the residual norm, and exit if not printing
//---------------------------------------------------------------------
        nthreads = proc_num_threads[myid];

//$omp parallel private(iz,m,zone)
//$omp&  num_threads(nthreads)
 
#pragma omp parallel private(iz,m,zone)  num_threads(nthreads)
{
        do (iz , 1, proc_num_zones,1) {
          zone = proc_zone_id[iz-1];
          error_norm (xce_sub,&u[start5[zone-1]-1],nx[zone-1],nxmax[zone-1],ny[zone-1],nz[zone-1]);
          compute_rhs (&rho_i[start1[zone-1]-1],&us[start1[zone-1]-1],&vs[start1[zone-1]-1],&ws[start1[zone-1]-1],&qs[start1[zone-1]-1],&square[start1[zone-1]-1],&rhs[start5[zone-1]-1],&forcing[start5[zone-1]-1],&u[start5[zone-1]-1],nx[zone-1],nxmax[zone-1],ny[zone-1],nz[zone-1]);

          rhs_norm (xcr_sub,&rhs[start5[zone-1]-1],nx[zone-1],nxmax[zone-1],ny[zone-1],nz[zone-1]);

//$omp master
 
#pragma omp master
{
          do (m , 1, 5,1) {
//$omp atomic
 
#pragma omp atomic
            xcr[m-1] = xcr[m-1] + xcr_sub[m-1] / dt;
//$omp atomic
 
#pragma omp atomic
            xce[m-1] = xce[m-1] + xce_sub[m-1];
          }
//$omp end master
 
} //#pragma omp end master
        }
//$omp end parallel
 
} //#pragma omp end parallel

//$omp barrier
 
#pragma omp barrier

//$omp master
 
#pragma omp master
{
        (*verified) = true;

        do (m , 1,5,1) {
           xcrref[m-1] = 1.0;
           xceref[m-1] = 1.0;
        }

//---------------------------------------------------------------------
//    reference data for class S
//---------------------------------------------------------------------
        if ( CLASS == 'S' ) {
           dtref = 1.0e-2;
           niterref = 60;

//---------------------------------------------------------------------
//  Reference values of RMS-norms of residual.
//---------------------------------------------------------------------
           xcrref[0] = 0.1047687395830e+04;
           xcrref[1] = 0.9419911314792e+02;
           xcrref[2] = 0.2124737403068e+03;
           xcrref[3] = 0.1422173591794e+03;
           xcrref[4] = 0.1135441572375e+04;

//---------------------------------------------------------------------
//  Reference values of RMS-norms of solution error.
//---------------------------------------------------------------------
           xceref[0] = 0.1775416062982e+03;
           xceref[1] = 0.1875540250835e+02;
           xceref[2] = 0.3863334844506e+02;
           xceref[3] = 0.2634713890362e+02;
           xceref[4] = 0.1965566269675e+03;

//---------------------------------------------------------------------
//    reference data for class W
//---------------------------------------------------------------------
        } else if ( CLASS == 'W' ) {
           dtref = 0.8e-3;
           niterref = 200;

//---------------------------------------------------------------------
//  Reference values of RMS-norms of residual.
//---------------------------------------------------------------------
           xcrref[0] = 0.5562611195402e+05;
           xcrref[1] = 0.5151404119932e+04;
           xcrref[2] = 0.1080453907954e+05;
           xcrref[3] = 0.6576058591929e+04;
           xcrref[4] = 0.4528609293561e+05;

//---------------------------------------------------------------------
//  Reference values of RMS-norms of solution error.
//---------------------------------------------------------------------
           xceref[0] = 0.7185154786403e+04;
           xceref[1] = 0.7040472738068e+03;
           xceref[2] = 0.1437035074443e+04;
           xceref[3] = 0.8570666307849e+03;
           xceref[4] = 0.5991235147368e+04;

//---------------------------------------------------------------------
//    reference data for class A
//---------------------------------------------------------------------
        } else if ( CLASS == 'A' ) {
           dtref = 0.8e-3;
           niterref = 200;

//---------------------------------------------------------------------
//  Reference values of RMS-norms of residual.
//---------------------------------------------------------------------
           xcrref[0] = 0.5536703889522e+05;
           xcrref[1] = 0.5077835038405e+04;
           xcrref[2] = 0.1067391361067e+05;
           xcrref[3] = 0.6441179694972e+04;
           xcrref[4] = 0.4371926324069e+05;

//---------------------------------------------------------------------
//  Reference values of RMS-norms of solution error.
//---------------------------------------------------------------------
           xceref[0] = 0.6716797714343e+04;
           xceref[1] = 0.6512687902160e+03;
           xceref[2] = 0.1332930740128e+04;
           xceref[3] = 0.7848302089180e+03;
           xceref[4] = 0.5429053878818e+04;

//---------------------------------------------------------------------
//    reference data for class B
//---------------------------------------------------------------------
        } else if ( CLASS == 'B' ) {
           dtref = 3.0e-4;
           niterref = 200;

//---------------------------------------------------------------------
//  Reference values of RMS-norms of residual.
//---------------------------------------------------------------------
           xcrref[0] = 0.4461388343844e+06;
           xcrref[1] = 0.3799759138035e+05;
           xcrref[2] = 0.8383296623970e+05;
           xcrref[3] = 0.5301970201273e+05;
           xcrref[4] = 0.3618106851311e+06;

//---------------------------------------------------------------------
//  Reference values of RMS-norms of solution error.
//---------------------------------------------------------------------
           xceref[0] = 0.4496733567600e+05;
           xceref[1] = 0.3892068540524e+04;
           xceref[2] = 0.8763825844217e+04;
           xceref[3] = 0.5599040091792e+04;
           xceref[4] = 0.4082652045598e+05;

//---------------------------------------------------------------------
//    reference data class C
//---------------------------------------------------------------------
        } else if ( CLASS == 'C' ) {
           dtref = 1.0e-4;
           niterref = 200;

//---------------------------------------------------------------------
//  Reference values of RMS-norms of residual.
//---------------------------------------------------------------------
           xcrref[0] = 0.3457703287806e+07;
           xcrref[1] = 0.3213621375929e+06;
           xcrref[2] = 0.7002579656870e+06;
           xcrref[3] = 0.4517459627471e+06;
           xcrref[4] = 0.2818715870791e+07;

//---------------------------------------------------------------------
//  Reference values of RMS-norms of solution error.
//---------------------------------------------------------------------
           xceref[0] = 0.2059106993570e+06;
           xceref[1] = 0.1680761129461e+05;
           xceref[2] = 0.4080731640795e+05;
           xceref[3] = 0.2836541076778e+05;
           xceref[4] = 0.2136807610771e+06;

//---------------------------------------------------------------------
//    reference data class D
//---------------------------------------------------------------------
        } else if ( CLASS == 'D' ) {
           dtref = 2.0e-5;
           niterref = 250;

//---------------------------------------------------------------------
//  Reference values of RMS-norms of residual.
//---------------------------------------------------------------------
           xcrref[0] = 0.4250417034981e+08;
           xcrref[1] = 0.4293882192175e+07;
           xcrref[2] = 0.9121841878270e+07;
           xcrref[3] = 0.6201357771439e+07;
           xcrref[4] = 0.3474801891304e+08;

//---------------------------------------------------------------------
//  Reference values of RMS-norms of solution error.
//---------------------------------------------------------------------
           xceref[0] = 0.9462418484583e+06;
           xceref[1] = 0.7884728947105e+05;
           xceref[2] = 0.1902874461259e+06;
           xceref[3] = 0.1361858029909e+06;
           xceref[4] = 0.9816489456253e+06;

//---------------------------------------------------------------------
//    reference data class E
//---------------------------------------------------------------------
        } else if ( CLASS == 'E' ) {
           dtref = 4.0e-6;
           niterref = 250;

//---------------------------------------------------------------------
//  Reference values of RMS-norms of residual.
//---------------------------------------------------------------------
           xcrref[0] = 0.5744815962469e+09;
           xcrref[1] = 0.6088696479719e+08;
           xcrref[2] = 0.1276325224438e+09;
           xcrref[3] = 0.8947040105616e+08;
           xcrref[4] = 0.4726115284807e+09;

//---------------------------------------------------------------------
//  Reference values of RMS-norms of solution error.
//---------------------------------------------------------------------
           xceref[0] = 0.4114447054461e+07;
           xceref[1] = 0.3570776728190e+06;
           xceref[2] = 0.8465106191458e+06;
           xceref[3] = 0.6147182273817e+06;
           xceref[4] = 0.4238908025163e+07;

//---------------------------------------------------------------------
//    reference data class F
//---------------------------------------------------------------------
        } else if ( CLASS == 'F' ) {
           dtref = 1.0e-6;
           niterref = 250;

//---------------------------------------------------------------------
//  Reference values of RMS-norms of residual.
//---------------------------------------------------------------------
           xcrref[0] = 0.6524078317845e+10;
           xcrref[1] = 0.7020439279514e+09;
           xcrref[2] = 0.1467588422194e+10;
           xcrref[3] = 0.1042973064137e+10;
           xcrref[4] = 0.5411102201141e+10;

//---------------------------------------------------------------------
//  Reference values of RMS-norms of solution error.
//---------------------------------------------------------------------
           xceref[0] = 0.1708795375347e+08;
           xceref[1] = 0.1514359936802e+07;
           xceref[2] = 0.3552878359250e+07;
           xceref[3] = 0.2594549582184e+07;
           xceref[4] = 0.1749809607845e+08;

           if (no_time_steps == 25) {

           niterref = 25;
           xcrref[0] = 0.3565049484400e+11;
           xcrref[1] = 0.3752029586145e+10;
           xcrref[2] = 0.7805935552197e+10;
           xcrref[3] = 0.5685995438056e+10;
           xcrref[4] = 0.2908811276266e+11;

           xceref[0] = 0.1805995755490e+08;
           xceref[1] = 0.1632306899424e+07;
           xceref[2] = 0.3778610439036e+07;
           xceref[3] = 0.2749319818549e+07;
           xceref[4] = 0.1814401049296e+08;

           }
        } else {
           dtref = 0.0e0;
           niterref = 0;
           (*verified) = false;
        }

//---------------------------------------------------------------------
//    Compute the difference of solution values and the known reference values.
//---------------------------------------------------------------------
        do (m , 1, 5,1) {

           xcrdif[m-1] = fabs ((xcr[m-1]-xcrref[m-1])/xcrref[m-1]);
           xcedif[m-1] = fabs ((xce[m-1]-xceref[m-1])/xceref[m-1]);

        }

//---------------------------------------------------------------------
//    Output the comparison of computed results to known cases.
//---------------------------------------------------------------------

        printf(" Verification being performed for CLASS  %c\n", CLASS);
        printf(" accuracy setting for epsilon =  %20.13E\n", epsilon);
        if (fabs (dt-dtref) > epsilon) {
           (*verified) = false;
           printf(" DT does not match the reference value of  %15.8E\n", dtref);
        } else if (no_time_steps != niterref) {
           (*verified) = false;
           printf(" NITER does not match the reference value of  %5d\n", niterref);
        }

        printf(" Comparison of RMS-norms of residual\n");

        do (m , 1, 5,1) {
           if (xcrdif[m-1] <= epsilon) {
              printf("           %2d %20.13E %20.13E %20.13E\n", m,xcr[m-1],xcrref[m-1],xcrdif[m-1]);
           } else {
              (*verified) = false;
              printf(" FAILURE:  %2d %20.13E %20.13E %20.13E\n", m,xcr[m-1],xcrref[m-1],xcrdif[m-1]);
           }
        }

        printf(" Comparison of RMS-norms of solution error\n");


        do (m , 1, 5,1) {
           if (xcedif[m-1] <= epsilon) {
              printf("           %2d %20.13E %20.13E %20.13E\n", m,xce[m-1],xceref[m-1],xcedif[m-1]);
           } else {
              (*verified) = false;
              printf(" FAILURE:  %2d %20.13E %20.13E %20.13E\n", m,xce[m-1],xceref[m-1],xcedif[m-1]);
           }
        }


        if ((*verified)) {
           printf(" Verification Successful\n");
        } else {
           printf(" Verification failed\n");
        }
//$omp end master
 
} //#pragma omp end master

        return;


}//end


//---------------------------------------------------------------------
//---------------------------------------------------------------------

