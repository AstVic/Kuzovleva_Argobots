#include "header.h"

void zone_setup (nx,nxmax,ny,nz)
// beg param
       int nx[];
       int nxmax[];
       int ny[];
       int nz[];
// end param
{

#include "omp_stuff.h"


      int i;
      int j;
      int zone_no;
      int id_west;
      int id_east;
      int jd_south;
      int jd_north;
      double x_r;
      double y_r;
      double x_smallest;
      double y_smallest;

       if (fabs (RATIO-1.e0) > 1.e-10) {

//        compute zone stretching only if the prescribed zone size ratio 
//        is substantially larger than unity       

         x_r   = exp (log(RATIO)/(X_ZONES-1));
         y_r   = exp (log(RATIO)/(Y_ZONES-1));
         x_smallest = (double) (GX_SIZE)*(x_r-1.e0)/(pow(x_r,X_ZONES)-1.e0);
         y_smallest = (double) (GY_SIZE)*(y_r-1.e0)/(pow(y_r,Y_ZONES)-1.e0);

//        compute tops of intervals, using a slightly tricked rounding
//        to make sure that the intervals are increasing monotonically
//        in size

         do (i , 1, X_ZONES,1) {
            x_end[i-1] = x_smallest*(pow(x_r,i)-1.e0)/(x_r-1.e0)+0.45e0;
         }

         do (j , 1, Y_ZONES,1) {
            y_end[j-1] = y_smallest*(pow(y_r,j)-1.e0)/(y_r-1.e0)+0.45e0;
         }

       } else {

//        compute essentially equal sized zone dimensions

         do (i , 1, X_ZONES,1) {
           x_end[i-1]   =(i*GX_SIZE)/X_ZONES;
         }

         do (j , 1, Y_ZONES,1) {
           y_end[j-1]   =(j*GY_SIZE)/Y_ZONES;
         }

       }

       x_start[0] = 1;
       do (i , 1, X_ZONES,1) {
          if (i != X_ZONES) x_start[i] = x_end[i-1] + 1;
          x_size[i-1]  = x_end[i-1] - x_start[i-1] + 1;
       }

       y_start[0] = 1;
       do (j , 1, Y_ZONES,1) {
          if (j != Y_ZONES) y_start[j] = y_end[j-1] + 1;
          y_size[j-1] = y_end[j-1] - y_start[j-1] + 1;
       }

       if (npb_verbose > 1) printf("\n Zone sizes:\n");

       do (j , 1, Y_ZONES,1) {
         do (i , 1, X_ZONES,1) {
           zone_no =(i-1)+(j-1)*X_ZONES+1;
           nx[zone_no-1] = x_size[i-1];
           nxmax[zone_no-1] = nx[zone_no-1] + 1 - mod (nx[zone_no-1],2);
           ny[zone_no-1] = y_size[j-1];
           nz[zone_no-1] = GZ_SIZE;

           id_west  = mod (i-2+X_ZONES,X_ZONES);
           id_east  = mod (i, X_ZONES);
           jd_south = mod (j-2+Y_ZONES,Y_ZONES);
           jd_north = mod (j, Y_ZONES);
           iz_west[zone_no-1] = id_west +(j-1)*X_ZONES + 1;
           iz_east[zone_no-1] = id_east +(j-1)*X_ZONES + 1;
           iz_south[zone_no-1] =(i-1) + jd_south*X_ZONES + 1;
           iz_north[zone_no-1] =(i-1) + jd_north*X_ZONES + 1;

           if (npb_verbose > 1) {
             printf("%5d:   %5d x %5d x %5d\n", zone_no, nx[zone_no-1], ny[zone_no-1], nz[zone_no-1]);
           }
         }
       }


       return;
}//end


void zone_starts (num_zones,nx,nxmax,ny,nz)
// beg param
       int num_zones;
       int nx[];
       int nxmax[];
       int ny[];
       int nz[];
// end param
{

#include "omp_stuff.h"


      int zone;
      int zone_size;
      int x_face_size;
      int y_face_size;

// ... index start for u & qbc
       do (zone , 1, num_zones,1) {
          zone_size = nxmax[zone-1]*ny[zone-1]*nz[zone-1];
          x_face_size =(ny[zone-1]-2)*(nz[zone-1]-2)*5;
          y_face_size =(nx[zone-1]-2)*(nz[zone-1]-2)*5;

          if (zone == 1) {
             qstart_west[zone-1] = 1;
             start1[zone-1] = 1;
             start5[zone-1] = 1;
          }
          qstart_east[zone-1]  = qstart_west[zone-1] + x_face_size;
          qstart_south[zone-1] = qstart_east[zone-1] + x_face_size;
          qstart_north[zone-1] = qstart_south[zone-1]+ y_face_size;
          if (zone != num_zones) {
             qstart_west[zone] = qstart_north[zone-1] + y_face_size;
             start1[zone] = start1[zone-1] + zone_size;
             start5[zone] = start5[zone-1] + zone_size*5;
          } else {
             if (start1[zone-1]+zone_size-1 > PROC_MAX_SIZE) {
                printf(" Error in size: zone %5d PROC_MAX_SIZE %10d access_size %10d\n", zone,PROC_MAX_SIZE,start1[zone-1]+zone_size-1);
exit(1);
             }
          }
       }

       if (npb_verbose > 1) {
          do (zone , 1, num_zones,1) {
             printf(" zone= %5d start1= %10d start5= %10d\n", zone,start1[zone-1],start5[zone-1]);
          }
       }

       return;
}//end

//---------------------------------------------------------------------
//---------------------------------------------------------------------

