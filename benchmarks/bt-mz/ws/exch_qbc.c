#include "header.h"
#include "ws_parallel.h"

void copy_x_face (void *ou, void *oqbc, int nx, int nxmax, int ny, int nz, int iloc, char* dir);
void copy_y_face (void *ou, void *oqbc, int nx, int nxmax, int ny, int nz, int jloc, char* dir);

/* Аргументы обмена границами: массивы решения, буфер граней и описание зон. */
typedef struct {
    double *u;
    double *qbc;
    int *nx;
    int *nxmax;
    int *ny;
    int *nz;
    int *proc_zone_id;
} exch_job;

/* Копирование граней зон [lo, hi) списка в буфер qbc. */
static void exch_out_body (long lo, long hi, void *p)
{
    exch_job *j = (exch_job *)p;
    for (long iz = lo; iz < hi; iz++) {
        int zone_no = j->proc_zone_id[iz];
        int nnx = j->nx[zone_no-1];
        int nnxmax = j->nxmax[zone_no-1];
        int nny = j->ny[zone_no-1];
        int nnz = j->nz[zone_no-1];
        copy_x_face (&j->u[start5[zone_no-1]-1],&j->qbc[qstart_west[zone_no-1]-1],nnx,nnxmax,nny,nnz,1, "out");
        copy_x_face (&j->u[start5[zone_no-1]-1],&j->qbc[qstart_east[zone_no-1]-1],nnx,nnxmax,nny,nnz,nnx-2, "out");
        copy_y_face (&j->u[start5[zone_no-1]-1],&j->qbc[qstart_north[zone_no-1]-1],nnx,nnxmax,nny,nnz,nny-2, "out");
        copy_y_face (&j->u[start5[zone_no-1]-1],&j->qbc[qstart_south[zone_no-1]-1],nnx,nnxmax,nny,nnz,1, "out");
    }
}

/* Копирование граней соседних зон из буфера qbc в зоны [lo, hi) списка. */
static void exch_in_body (long lo, long hi, void *p)
{
    exch_job *j = (exch_job *)p;
    for (long iz = lo; iz < hi; iz++) {
        int zone_no = j->proc_zone_id[iz];
        int nnx = j->nx[zone_no-1];
        int nnxmax = j->nxmax[zone_no-1];
        int nny = j->ny[zone_no-1];
        int nnz = j->nz[zone_no-1];
        int izone_west = iz_west[zone_no-1];
        int izone_east = iz_east[zone_no-1];
        int jzone_south = iz_south[zone_no-1];
        int jzone_north = iz_north[zone_no-1];
        copy_x_face (&j->u[start5[zone_no-1]-1],&j->qbc[qstart_east[izone_west-1]-1],nnx,nnxmax,nny,nnz,0, "in");
        copy_x_face (&j->u[start5[zone_no-1]-1],&j->qbc[qstart_west[izone_east-1]-1],nnx,nnxmax,nny,nnz,nnx-1, "in");
        copy_y_face (&j->u[start5[zone_no-1]-1],&j->qbc[qstart_north[jzone_south-1]-1],nnx,nnxmax,nny,nnz,0, "in");
        copy_y_face (&j->u[start5[zone_no-1]-1],&j->qbc[qstart_south[jzone_north-1]-1],nnx,nnxmax,nny,nnz,nny-1, "in");
    }
}

/* Исходная функция exch_qbc, в которой оба цикла по зонам выполняются параллельно:
 * сначала все зоны пишут свои грани в буфер, затем читают грани соседей. */
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
       exch_job job = {u, qbc, nx, nxmax, ny, nz, proc_zone_id};

       if (timeron) timer_start (T_RDIS1);
       ws_parallel_for (0, proc_num_zones, exch_out_body, &job);
       ws_parallel_for (0, proc_num_zones, exch_in_body, &job);
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

