#include "header.h"

void exch_qbc (u,qbc,nx,nxmax,ny,nz,proc_zone_id,proc_num_zones)
// beg param
       double u[];
       double qbc[];
       int nx[];
       int nxmax[];
       int ny[];
       int nz[];
       int proc_zone_id[];
       int proc_num_zones;
// end param
{

#include "omp_stuff.h"


      int nnx;
      int nnxmax;
      int nny;
      int nnz;
      int zone_no;
      int iz;
      int nthreads;
      int izone_west;
      int izone_east;
      int jzone_south;
      int jzone_north;


       if (timeron) timer_start (T_RDIS2);
//$omp barrier
 
#pragma omp barrier
       if (timeron) timer_stop (T_RDIS2);
       nthreads = proc_num_threads[myid];

//      copy data to qbc buffer
       if (timeron) timer_start (T_RDIS1);
//$omp parallel private(iz,zone_no,nnx,nnxmax,nny,nnz)
//$omp&  num_threads(nthreads)
 
#pragma omp parallel private(iz,zone_no,nnx,nnxmax,nny,nnz)  num_threads(nthreads)
{
       do (iz , 1, proc_num_zones,1) {
           zone_no = proc_zone_id[iz-1];
           nnx    = nx[zone_no-1];
           nnxmax = nxmax[zone_no-1];
           nny    = ny[zone_no-1];
           nnz    = nz[zone_no-1];

           copy_x_face (&u[start5[zone_no-1]-1],&qbc[qstart_west[zone_no-1]-1],nnx,nnxmax,nny,nnz,1, "out");

           copy_x_face (&u[start5[zone_no-1]-1],&qbc[qstart_east[zone_no-1]-1],nnx,nnxmax,nny,nnz,nnx-2, "out");


           copy_y_face (&u[start5[zone_no-1]-1],&qbc[qstart_north[zone_no-1]-1],nnx,nnxmax,nny,nnz,nny-2, "out");

           copy_y_face (&u[start5[zone_no-1]-1],&qbc[qstart_south[zone_no-1]-1],nnx,nnxmax,nny,nnz,1, "out");

       }
//$omp end parallel
 
} //#pragma omp end parallel
       if (timeron) timer_stop (T_RDIS1);

       if (timeron) timer_start (T_RDIS2);
//$omp barrier
 
#pragma omp barrier
       if (timeron) timer_stop (T_RDIS2);

//      copy data from qbc buffer
       if (timeron) timer_start (T_RDIS1);
//$omp parallel private(iz,zone_no,nnx,nnxmax,nny,nnz,
//$omp&  izone_west,izone_east,jzone_south,jzone_north)
//$omp&  num_threads(nthreads)
 
#pragma omp parallel private(iz,zone_no,nnx,nnxmax,nny,nnz, izone_west,izone_east,jzone_south,jzone_north)  num_threads(nthreads)
{
       do (iz , 1, proc_num_zones,1) {
           zone_no = proc_zone_id[iz-1];
           nnx    = nx[zone_no-1];
           nnxmax = nxmax[zone_no-1];
           nny    = ny[zone_no-1];
           nnz    = nz[zone_no-1];

           izone_west  = iz_west[zone_no-1];
           izone_east  = iz_east[zone_no-1];
           jzone_south = iz_south[zone_no-1];
           jzone_north = iz_north[zone_no-1];

           copy_x_face (&u[start5[zone_no-1]-1],&qbc[qstart_east[izone_west-1]-1],nnx,nnxmax,nny,nnz,0, "in");

           copy_x_face (&u[start5[zone_no-1]-1],&qbc[qstart_west[izone_east-1]-1],nnx,nnxmax,nny,nnz,nnx-1, "in");

           copy_y_face (&u[start5[zone_no-1]-1],&qbc[qstart_north[jzone_south-1]-1],nnx,nnxmax,nny,nnz,0, "in");

           copy_y_face (&u[start5[zone_no-1]-1],&qbc[qstart_south[jzone_north-1]-1],nnx,nnxmax,nny,nnz,nny-1, "in");

       }
//$omp end parallel
 
} //#pragma omp end parallel
       if (timeron) timer_stop (T_RDIS1);

       return;
}//end


void copy_y_face (ou,oqbc,nx,nxmax,ny,nz,jloc,dir)
// beg param
       void *ou;
       void *oqbc;
       int nx;
       int nxmax;
       int ny;
       int nz;
       int jloc;
       char* dir;
// end param copy_y_face
{
double (*qbc)[nx-2][5] = (double (*)[nx-2][5])oqbc;
double (*u)[(ny-1)-(0)+1][(nxmax-1)-(0)+1][5] = (double (*)[(ny-1)-(0)+1][(nxmax-1)-(0)+1][5])ou;


//       implicit         none

      int i;
      int j;
      int k;
      int m;

       j = jloc;
       if (!strcmp(dir,"in")) {
//$omp do
 
#pragma omp for
         do (k , 1, nz-2,1) {
           do (i , 1, nx-2,1) {
             do (m , 1, 5,1) {
               u[k+0][j+0][i+0][m-1] = qbc[k-1][i-1][m-1];
             }
           }
         }
//$omp end do
 
 //#pragma omp end do
       } else if (!strcmp(dir,"out")) {
//$omp do
 
#pragma omp for
         do (k , 1, nz-2,1) {
           do (i , 1, nx-2,1) {
             do (m , 1, 5,1) {
               qbc[k-1][i-1][m-1] = u[k+0][j+0][i+0][m-1];
             }
           }
         }
//$omp end do
 
 //#pragma omp end do
       } else {
         printf(" %s %s\n", "Erroneous data designation: ", dir);
exit(1);
       }

       return;
}//end


void copy_x_face (ou,oqbc,nx,nxmax,ny,nz,iloc,dir)
// beg param
       void *ou;
       void *oqbc;
       int nx;
       int nxmax;
       int ny;
       int nz;
       int iloc;
       char* dir;
// end param copy_x_face
{
double (*qbc)[ny-2][5] = (double (*)[ny-2][5])oqbc;
double (*u)[(ny-1)-(0)+1][(nxmax-1)-(0)+1][5] = (double (*)[(ny-1)-(0)+1][(nxmax-1)-(0)+1][5])ou;


//       implicit         none

      int i;
      int j;
      int k;
      int m;

       i = iloc;
       if (!strcmp(dir,"in")) {
//$omp do
 
#pragma omp for
         do (k , 1, nz-2,1) {
           do (j , 1, ny-2,1) {
             do (m , 1, 5,1) {
               u[k+0][j+0][i+0][m-1] = qbc[k-1][j-1][m-1];
             }
           }
         }
//$omp end do
 
 //#pragma omp end do
       } else if (!strcmp(dir,"out")) {
//$omp do
 
#pragma omp for
         do (k , 1, nz-2,1) {
           do (j , 1, ny-2,1) {
             do (m , 1, 5,1) {
               qbc[k-1][j-1][m-1] = u[k+0][j+0][i+0][m-1];
             }
           }
         }
//$omp end do
 
 //#pragma omp end do
       } else {
         printf(" %s %s\n", "Erroneous data designation: ", dir);
exit(1);
       }

       return;
}//end


//---------------------------------------------------------------------
//---------------------------------------------------------------------

