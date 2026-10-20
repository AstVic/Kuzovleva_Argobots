#define do(v,l,h,s) for(v=(l); v<=(h); v+=s)
#define dom(v,l,h,s) for(v=(l); v>=(h); v+=s)
#define mod(x,y)((x)%(y))

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <time.h>

#include "type.h"

double timer_read(int);
#include "print_results.h"


//-------------------------------------------------------------------------!
//                                                                         !
//        N  A  S     P A R A L L E L     B E N C H M A R K S  3.3         !
//                                                                         !
//          O p e n M P    M U L T I - Z O N E    V E R S I O N            !
//                                                                         !
//                            B T - M Z - O M P                            !
//                                                                         !
//-------------------------------------------------------------------------!
//                                                                         !
//    This benchmark is an OpenMP version of the NPB BT code.              !
//    Refer to NAS Technical Reports 95-020 and 99-011 for details.        !
//                                                                         !
//    Permission to use, copy, distribute and modify this software         !
//    for any purpose with or without fee is hereby granted.  We           !
//    request, however, that all derived work reference the NAS            !
//    Parallel Benchmarks 3.3. This software is provided "as is"           !
//    without express or implied warranty.                                 !
//                                                                         !
//    Information on NPB 3.3, including the technical report, the          !
//    original specifications, source code, results and information        !
//    on how to submit new results, is available at:                       !
//                                                                         !
//           http://www.nas.nasa.gov/Software/NPB/                         !
//                                                                         !
//    Send comments or suggestions to  npb@nas.nasa.gov                    !
//                                                                         !
//          NAS Parallel Benchmarks Group                                  !
//          NASA Ames Research Center                                      !
//          Mail Stop: T27A-1                                              !
//          Moffett Field, CA   94035-1000                                 !
//                                                                         !
//          E-mail:  npb@nas.nasa.gov                                      !
//          Fax:     (650) 604-3957                                        !
//                                                                         !
//-------------------------------------------------------------------------!

//---------------------------------------------------------------------
//
// Authors: R. Van der Wijngaart
//          T. Harris
//          M. Yarrow
//          H. Jin
//
//---------------------------------------------------------------------

//---------------------------------------------------------------------
//       program BT
#define NUM_ZONES X_ZONES*Y_ZONES

#include "commons.h"


#ifdef _OPENMP
#include <omp.h>
#endif


#include "header.h"

int main (int argc, char **argv)
{
// end param

//---------------------------------------------------------------------

#include "omp_stuff.h"


      int nx[NUM_ZONES];
      int nxmax[NUM_ZONES];
      int ny[NUM_ZONES];
      int nz[NUM_ZONES];
      int proc_zone_id[NUM_ZONES];
      int proc_num_zones;

//---------------------------------------------------------------------
//   Define all field arrays as one-dimenional arrays, to be reshaped
//---------------------------------------------------------------------
extern double u[PROC_MAX_SIZE5];
extern double us[PROC_MAX_SIZE ];
extern double vs[PROC_MAX_SIZE ];
extern double ws[PROC_MAX_SIZE ];
extern double qs[PROC_MAX_SIZE ];
extern double rho_i[PROC_MAX_SIZE ];
extern double square[PROC_MAX_SIZE ];
extern double rhs[PROC_MAX_SIZE5];
extern double forcing[PROC_MAX_SIZE5];
extern double qbc[PROC_MAX_BCSIZE];


      int i;
      int niter;
      int step;
//      int fstatus;
      int zone;
      int iz;
      int itimer;
      int tot_threads;
      int nthreads;
      double navg;
      double mflops;
      double nsur;
      double n3;

//       extern         timer_read;
      double tmax;
//      double timer_read;
      double t;
      double processor_time;
      double trecs[T_LAST];
      logical verified;
      char* t_names[T_LAST+1];


//---------------------------------------------------------------------
//      Reads input file (if it exists) else takes
//      defaults from parameters
//---------------------------------------------------------------------

       printf("\n\n NAS Parallel Benchmarks (NPB3.3-MZ-OMP) - BT-MZ OpenMP Benchmark\n\n");
       fstatus=fopen("inputbt-mz.data","r");

       timeron = false;
       if (fstatus != NULL) {
         printf(" %s\n", "Reading from input file inputbt-mz.data");
         fscanf(fstatus," %d", &niter);
         while (fgetc(fstatus) !='\n');

         fscanf(fstatus," %lf", &dt);
         while (fgetc(fstatus) !='\n');

         fscanf(fstatus," %d", &itimer);
         while (fgetc(fstatus) !='\n');

         fclose(fstatus);

         if (niter == 0)  niter = NITER_DEFAULT;
         if (dt == 0.e0)  dt    = DT_DEFAULT;
         if (itimer > 0) timeron = true;

       } else {
         niter = NITER_DEFAULT;
         dt    = DT_DEFAULT;
       }

       printf(" Number of zones:  %3d x  %3d\n", X_ZONES, Y_ZONES);
       printf(" Iterations:  %3d dt:  %10.6F\n\n", niter, dt);

       if (timeron) {
         t_names[T_TOTAL] = "total";
         t_names[T_RHSX] = "rhsx";
         t_names[T_RHSY] = "rhsy";
         t_names[T_RHSZ] = "rhsz";
         t_names[T_RHS] = "rhs";
         t_names[T_XSOLVE] = "xsolve";
         t_names[T_YSOLVE] = "ysolve";
         t_names[T_ZSOLVE] = "zsolve";
         t_names[T_RDIS1] = "qbc_copy";
         t_names[T_RDIS2] = "qbc_comm";
         t_names[T_ADD] = "add";
       }

       env_setup (&tot_threads);

       zone_setup (nx, nxmax, ny, nz);

       omp_setup (NUM_ZONES, nx, ny, nz,tot_threads);
       zone_starts (NUM_ZONES, nx, nxmax, ny, nz);

       set_constants ();


//$omp parallel private(iz,i,zone,step,t,trecs,nthreads,
//$omp&  proc_num_zones,proc_zone_id)
//$omp&  if(nested!=2)
 
#pragma omp parallel private(iz,i,zone,step,t,trecs,nthreads, proc_num_zones,proc_zone_id)  if(nested!=2)
{

       omp_init (NUM_ZONES, proc_zone_id,&proc_num_zones);

       nthreads = proc_num_threads[myid];

//$omp parallel private(iz,zone) num_threads(nthreads)
 
#pragma omp parallel private(iz,zone) num_threads(nthreads)
{
       do (iz , 1, proc_num_zones,1) {
         zone = proc_zone_id[iz-1];

         initialize (&u[start5[zone-1]-1],nx[zone-1],nxmax[zone-1],ny[zone-1],nz[zone-1]);
         exact_rhs (&forcing[start5[zone-1]-1],nx[zone-1],nxmax[zone-1],ny[zone-1],nz[zone-1]);

       }
//$omp end parallel
 
} //#pragma omp end parallel

       do (i , 1, T_LAST,1) {
          timer_clear (i);
       }

//---------------------------------------------------------------------
//      do one time step to touch all code, and reinitialize
//---------------------------------------------------------------------

       exch_qbc (u, qbc, nx, nxmax, ny, nz, proc_zone_id,proc_num_zones);

//$omp parallel private(iz,zone) num_threads(nthreads)
 
#pragma omp parallel private(iz,zone) num_threads(nthreads)
{
       do (iz , 1, proc_num_zones,1) {
         zone = proc_zone_id[iz-1];
         adi (&rho_i[start1[zone-1]-1],&us[start1[zone-1]-1],&vs[start1[zone-1]-1],&ws[start1[zone-1]-1],&qs[start1[zone-1]-1],&square[start1[zone-1]-1],&rhs[start5[zone-1]-1],&forcing[start5[zone-1]-1],&u[start5[zone-1]-1],nx[zone-1],nxmax[zone-1],ny[zone-1],nz[zone-1]);
       }

       do (iz , 1, proc_num_zones,1) {
         zone = proc_zone_id[iz-1];
         initialize (&u[start5[zone-1]-1],nx[zone-1],nxmax[zone-1],ny[zone-1],nz[zone-1]);
       }
//$omp end parallel
 
} //#pragma omp end parallel

       do (i , 1, T_LAST,1) {
          timer_clear (i);
       }
//$omp barrier
 
#pragma omp barrier
       timer_start (1);
       clock_t start = clock();

//---------------------------------------------------------------------
//      start the benchmark time step loop
//---------------------------------------------------------------------

       do (step , 1, niter,1) {

         if (mod (step, 20) == 0 || step == 1) {
//$omp master
 
#pragma omp master
{
            printf(" Time step  %4d\n", step);
//$omp end master
 
} //#pragma omp end master
         }

         exch_qbc (u, qbc, nx, nxmax, ny, nz, proc_zone_id,proc_num_zones);

//$omp parallel private(iz,zone) num_threads(nthreads)
 
#pragma omp parallel private(iz,zone) num_threads(nthreads)
{
         do (iz , 1, proc_num_zones,1) {
           zone = proc_zone_id[iz-1];
           adi (&rho_i[start1[zone-1]-1],&us[start1[zone-1]-1],&vs[start1[zone-1]-1],&ws[start1[zone-1]-1],&qs[start1[zone-1]-1],&square[start1[zone-1]-1],&rhs[start5[zone-1]-1],&forcing[start5[zone-1]-1],&u[start5[zone-1]-1],nx[zone-1],nxmax[zone-1],ny[zone-1],nz[zone-1]);
         }
//$omp end parallel
 
} //#pragma omp end parallel

       }

//$omp barrier
 
#pragma omp barrier
       timer_stop (1);
       t = timer_read (1);
       clock_t end = clock();
       processor_time = ((double)(end - start)) / CLOCKS_PER_SEC;

//---------------------------------------------------------------------
//      perform verification and print results
//---------------------------------------------------------------------

       verify (niter,&verified,NUM_ZONES, rho_i, us, vs, ws, qs, square, rhs, forcing, u, nx, nxmax, ny, nz, proc_zone_id,proc_num_zones);

//$omp master
 
#pragma omp master
{
       tmax = t;
       mflops = 0.0e0;
       if ( tmax != 0. ) {
         do (zone , 1, NUM_ZONES,1) {
           n3 = (double) (nx[zone-1])*ny[zone-1]*nz[zone-1];
           navg =(nx[zone-1] + ny[zone-1] + nz[zone-1])/3.0;
           nsur =(nx[zone-1]*ny[zone-1] + nx[zone-1]*nz[zone-1] + ny[zone-1]*nz[zone-1])/3.0;
           mflops = mflops + 1.0e-6*(float) (niter) *(3478.8e0 * n3 - 17655.7e0 * nsur + 28023.7e0 * navg)      / tmax;
         }
       }

       c_print_results ("BT-MZ", CLASS, GX_SIZE, GY_SIZE, GZ_SIZE, niter, tmax, processor_time, mflops, num_othreads, tot_threads, " floating point", verified, NPBVERSION,COMPILETIME, CS1, CS2, CS3, CS4, CS5, CS6, "(none)");
//$omp end master
 
} //#pragma omp end master
//$omp barrier
 
#pragma omp barrier

//---------------------------------------------------------------------
//      More timers
//---------------------------------------------------------------------
       if (!timeron) goto lll999;

       do (i,1, T_LAST,1) {
          trecs[i-1] = timer_read (i);
       }
       if (tmax == 0.0) tmax = 1.0;

//$omp critical (ptime)
 
#pragma omp critical (ptime)
{
       printf(" Myid = %5d num_threads = %4d\n SECTION Time (secs)\n", myid, nthreads);
       do (i,1, T_LAST,1) {
          printf("   %8s: %9.3f ( %6.2f%)\n", t_names[i], trecs[i-1], trecs[i-1]*100./tmax);
          if (i==T_RHS) {
             t = trecs[T_RHSX-1] + trecs[T_RHSY-1] + trecs[T_RHSZ-1];
             printf("    --> total  %8s: %9.3f ( %6.2f%)\n", "sub-rhs", t, t*100./tmax);
             t = trecs[T_RHS-1] - t;
             printf("    --> total  %8s: %9.3f ( %6.2f%)\n", "rest-rhs", t, t*100./tmax);
          } else if (i==T_RDIS2) {
             t = trecs[T_RDIS1-1] + trecs[T_RDIS2-1];
             printf("    --> total  %8s: %9.3f ( %6.2f%)\n", "exch_qbc", t, t*100./tmax);
          }
       }
       printf("\n");
//$omp end critical (ptime)
 
} //#pragma omp end critical (ptime)

lll999:;

//$omp end parallel
 
} //#pragma omp end parallel

return 0;
}//end


//---------------------------------------------------------------------
//---------------------------------------------------------------------

