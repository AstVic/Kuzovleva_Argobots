#include "header.h"
#include "omp.h"

void omp_setup (num_zones,nx,ny,nz,tot_threads)
// beg param
       int num_zones;
       int nx[];
       int ny[];
       int nz[];
       int tot_threads;
// end param
{
//
//  Set up OMP related work, including
//     - zone-othread mapping for load balance
//     - set up number of threads
//
//
//
#include "omp_stuff.h"
//
      int nthreads;
//
// ... map zones to outer-level OpenMP threads
      map_zones (num_zones, nx, ny, nz,tot_threads);
//
// ... define number of outer-level threads
#ifdef _OPENMP
      omp_set_dynamic(false);
#endif
      nthreads = num_othreads;
      if (nested==2) nthreads = num_threads;
#ifdef _OPENMP
      omp_set_num_threads(nthreads);
#endif
//
      return;
}//end
//
//>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>c
//
void omp_init (num_zones,proc_zone_id,proc_num_zones)
// beg param
       int num_zones;
       int proc_zone_id[];
       int (*proc_num_zones);
// end param
{
//
//  Set up additional OMP related work
//
//
//
#include "omp_stuff.h"
//
      int zone_count;
      int iz;
//$    integer omp_get_thread_num
//$    external omp_get_thread_num
//
// ... info for current thread
      myid = 0;
#ifdef _OPENMP
    myid = omp_get_thread_num();
#endif
      root = 0;
//
// ... reorganize list of zones for this outer thread
      zone_count = 0;
      do (iz , 1, num_zones,1) {
         if (zone_proc_id[iz-1] == myid) {
            zone_count = zone_count + 1;
            proc_zone_id[zone_count-1] = iz;
         }
      }
      (*proc_num_zones) = zone_count;
      if (zone_count != proc_zone_count[myid]) {
         printf(" %s %d %s %d %d\n", "Warning: ",myid, ": mis-matched zone counts -", zone_count, proc_zone_count[myid]);
      }
//
      return;
}//end
//
//>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>c
//
void env_setup (tot_threads)
// beg param
       int (*tot_threads);
// end param
{
//
//  Set up from environment variables
//
// ... common variables
//
//
#include "omp_stuff.h"
//
// ... local variables
      int ios;
      int curr_threads;
      int ip;
      int mp;
      int group;
      int ip1;
      int ip2;
      int entry_counts[MAX_ZONES];
      char* envstr;
      char line[132];
      logical nflag;
//
//$    integer omp_get_max_threads
//$    logical omp_get_nested
//$    external omp_get_max_threads, omp_get_nested
//
// ... test the OpenMP multi-threading environment
      mp = 0;
#ifdef _OPENMP
    mp = omp_get_max_threads();
#endif
//
      envstr = getenv ("OMP_NUM_THREADS");
      if (envstr != NULL && mp > 0) {
         ios = sscanf(envstr, " %d", &num_othreads);
         if (ios == 0 || num_othreads<1) num_othreads = 1;
         if (mp != num_othreads) {
            printf(" Warning: Requested  %4d outer-level threads, but the active value is  %4d\n", num_othreads, mp);
            num_othreads = mp;
         }
      } else {
         num_othreads = 1;
      }
//
      if (num_othreads > MAX_ZONES) {
         printf(" Error: num_othreads  %5d exceeded max_allowed  %5d\n Please redefine the value for OMP_NUM_THREADS\n", num_othreads, MAX_ZONES);
exit(1);
      }
//
      envstr = getenv ("OMP_NUM_THREADS2");
      if (envstr != NULL && mp > 0) {
         ios = sscanf(envstr, " %d", &num_threads);
         if (ios == 0 || num_threads<1) num_threads = 1;
      } else {
         num_threads = 1;
      }
//
// ... no limit on how many inner-level threads we can use
      max_threads = 0;
//
// ... check nested-par support
      envstr = getenv ("NPB_OMP_NESTED");
      if (envstr != NULL && mp > 0) {
         ios = sscanf(envstr, " %d", &nested);
         if (ios == 0 || nested<0) nested = 0;
      } else {
         nested = 0;
      }
      if (nested==2) num_othreads = 1;
      if (nested==1) num_threads = 1;
//
      if (nested==1 || nested==2) {
#ifdef _OPENMP
      omp_set_nested(false);
#endif
      } else {
#ifdef _OPENMP
      omp_set_nested(true);
#endif
         nflag = true;
#ifdef _OPENMP
       nflag = omp_get_nested();
#endif
         if ((!nflag) && num_threads>1) {
            if (nested==3) {
               printf("*** Nested OpenMP not supported %s\n", " on the system");
            } else {
               printf("*** Nested OpenMP not supported %s\n", ", inner-level threads reset to one");
               num_threads = 1;
            }
         }
      }
//
      envstr = getenv ("NPB_MZ_BLOAD");
      if (envstr != NULL) {
         if (!strcmp(envstr,"on") || !strcmp(envstr,"ON")) {
            mz_bload = 1;
         } else if (envstr[0]=='t' || envstr[0]=='T') {
            mz_bload = 1;
         } else {
            ios = sscanf(envstr, " %d", &mz_bload);
            if (ios == 0) mz_bload = 0;
         }
      } else {
         mz_bload = 1;
      }
//
      envstr = getenv ("NPB_VERBOSE");
      npb_verbose = 0;
      if (envstr != NULL) {
         ios = sscanf(envstr, " %d", &npb_verbose);
         if (ios == 0) npb_verbose = 0;
      }
//
      do (ip , 1, num_othreads,1) {
         proc_num_threads[ip-1] = num_threads;
         proc_group[ip-1] = 0;
      }
//
      fstatus=fopen("loadbt-mz.data","r");
      if (fstatus != NULL) {
         printf(" %s\n", "Reading load factors from loadbt-mz.data");

         if (mz_bload >= 1) {
            mz_bload = -mz_bload;
         }

         do (ip , 1, num_othreads,1) {
            entry_counts[ip-1] = 0;
         }

         while(true) {
lll25:;
            if(fgets(line, 80, fstatus)==NULL) goto lll40;

            if (line[0]==' ' || line[0]=='#') goto lll25;

            decode_line (line,&ip1,&ip2,&curr_threads,&group,&ios);
            if (ios != 0) goto lll40;

            if (mz_bload < 0 && group > 0) {
               mz_bload = -mz_bload;
            }

            if (curr_threads < 1) curr_threads = 1;
            if (mp <= 0) curr_threads = 1;
            if (ip1<0) ip1 = 0;
            if (ip2>=num_othreads) ip2 = num_othreads - 1;

            do (ip , ip1+1, ip2+1,1) {
               proc_num_threads[ip-1] = curr_threads;
               proc_group[ip-1] = group;
               entry_counts[ip-1] = entry_counts[ip-1] + 1;
            }
         }
lll40:;
         fclose(fstatus);

         do (ip , 1, num_othreads,1) {
            if (entry_counts[ip-1] == 0) {
               printf(" %s %d\n", "*** Error: Missing entry for othread ",ip-1);
exit(1);
            } else if (entry_counts[ip-1] > 1) {
               printf(" %s %d %s\n", "*** Warning: Multiple entries for othread ", ip-1, ", only the last one used");
            }
         }

         ip1 = 1;
      } else {
         printf(" %s\n", "Use the default load factors with threads");
         ip1 = 0;
      }

      if (ip1 > 0 || npb_verbose > 0) {
         ip1 = 0;
         do (ip , 1, num_othreads,1) {
            if (ip == 1 ||          proc_num_threads[ip-1] != curr_threads ||          proc_group[ip-1] != group) {

               ip2 = ip-2;
               if (ip2 > ip1+1) printf(" %s\n", " ...");
               if (ip2 > ip1)            printf("  othread %6d num_threads = %5d flag = %5d\n", ip2, curr_threads, group);

               curr_threads = proc_num_threads[ip-1];
               group = proc_group[ip-1];

               ip1 = ip - 1;
               printf("  othread %6d num_threads = %5d flag = %5d\n", ip1, curr_threads, group);

            } else if (ip == num_othreads) {
               ip2 = ip-1;
               if (ip2 > ip1+1) printf(" %s\n", " ...");
               printf("  othread %6d num_threads = %5d flag = %5d\n", ip2, curr_threads, group);
            }
         }
      }
//
      (*tot_threads) = 0;
      do (ip , 1, num_othreads,1) {
         (*tot_threads) = (*tot_threads) + proc_num_threads[ip-1];
      }
      if (mp > 0) {
         printf(" Number of outer-level threads:  %5d\n", num_othreads);
         printf(" Total number of threads:  %6d ( %5.1f inner-threads/outer-thread)\n", (*tot_threads), (double) ((*tot_threads))/num_othreads);
      }
//
      return;
}//end
//
//>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>c
//
void decode_line (line,ip1,ip2,curr_threads,group,ios)
// beg param
       char line[80];
       int (*ip1);
       int (*ip2);
       int (*curr_threads);
       int (*group);
       int (*ios);
// end param
{
//      implicit none
//
//  decode a line from the load data file
//  format:  ip1[:ip2] curr_threads group
//
//
      int is;
      int n;
//
      (*ios) = -1;
//
      n  = strlen (line);
      is = 1;
      while(is<=n && line[is-1]!=':') {
         if (line[is-1]=='!') n = is;
         is = is + 1;
      }
//
      if (is > n) {
         if(sscanf(line, " %d %d %d", ip1, curr_threads, group)==0) goto lll90;
         (*ip2) = (*ip1);
      } else if (is==1 || is==n) {
         goto lll90;
      } else {
         sscanf(line,"%d",ip1);
         sscanf((char *)(line+is),"%d %d %d",ip2, curr_threads, group);
      }
//
      if ((*ip2) < (*ip1)) {
         is  = (*ip2);
         (*ip2) = (*ip1);
         (*ip1) = is;
      }
      (*ios) = 0;
//
lll90:;
      return;
}//end
//
//>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>c
//
void get_comm_index (zone,iproc,comm_index)
// beg param
       int zone;
       int iproc;
       int (*comm_index);
// end param
{
//
//
#include "omp_stuff.h"
//
//  Calculate the communication index of a zone within a thread group
//
//
//     local variables
      int izone;
      int jzone;
      int izone_west;
      int izone_east;
      int jzone_south;
      int jzone_north;
//
      jzone  =(zone - 1)/X_ZONES + 1;
      izone  = mod (zone - 1, X_ZONES) + 1;
      izone_west  = iz_west[zone-1];
      izone_east  = iz_east[zone-1];
      jzone_south = iz_south[zone-1];
      jzone_north = iz_north[zone-1];
//
      (*comm_index) = 0;
      if (zone_proc_id[izone_west-1] == iproc)   (*comm_index) = (*comm_index) + y_size[jzone-1];
      if (zone_proc_id[izone_east-1] == iproc)   (*comm_index) = (*comm_index) + y_size[jzone-1];
      if (zone_proc_id[jzone_south-1] == iproc)   (*comm_index) = (*comm_index) + x_size[izone-1];
      if (zone_proc_id[jzone_north-1] == iproc)   (*comm_index) = (*comm_index) + x_size[izone-1];
//
      return;
}//end
//
//>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>c
//
void map_zones (num_zones,nx,ny,nz,tot_threads)
// beg param
       int num_zones;
       int nx[];
       int ny[];
       int nz[];
       int tot_threads;
// end param
{
//
//  Perform zone-othread mapping for load balance
//
//
//
#include "omp_stuff.h"
//
//     local variables
      int z_order[MAX_ZONES];
      int zone;
      int iz;
      int z2;
      int mz;
      int np;
      int ip;
      int zone_comm;
      int comm_index;
      int imx;
      int imn;
      int inc;
      int icur_size;
      double zone_size[MAX_ZONES];
      double tot_size;
      double cur_size;
      double diff_ratio;
      double max_size;
      double ave_size;
//
      int group;
      int ipg;
      int tot_group_threads;
      double tot_group_size;
      int proc_group_flag[MAX_ZONES];
//
// ... sort the zones in decending order
      tot_size = 0.e0;
      do (iz , 1, num_zones,1) {
         zone_size[iz-1] = nx[iz-1]*ny[iz-1]*nz[iz-1];
         z_order[iz-1] = iz;
         tot_size = tot_size + zone_size[iz-1];
      }
      do (iz , 1, num_zones-1,1) {
         cur_size = zone_size[z_order[iz-1]-1];
         mz = iz;
         do (z2 , iz+1, num_zones,1) {
            if (cur_size<zone_size[z_order[z2-1]-1]) {
               cur_size = zone_size[z_order[z2-1]-1];
               mz = z2;
            }
         }
         if (mz != iz) {
            z2 = z_order[iz-1];
            z_order[iz-1] = z_order[mz-1];
            z_order[mz-1] = z2;
         }
      }
//
      if (npb_verbose > 1) {
         printf("\n Sorted zones:\n seq. zone nx ny nz size\n");
         do (iz , 1, num_zones,1) {
            z2 = z_order[iz-1];
            printf("%5d:  %5d  %5d  %5d  %5d  %9.0f\n", iz,z2,nx[z2-1],ny[z2-1],nz[z2-1],zone_size[z2-1]);
         }
      }
//
// ... use a simple bin-packing scheme to balance the load among othreads
      do (ip , 1, num_othreads,1) {
         proc_zone_count[ip-1] = 0;
         proc_zone_size[ip-1] = 0.e0;
      }
      do (iz , 1, num_zones,1) {
         zone_proc_id[iz-1] = -1;
      }

      iz = 1;
      while(iz <= num_zones) {
//
//  ...   the current most empty thread
         np = 1;
         cur_size = proc_zone_size[0];
         do (ip , 2, num_othreads,1) {
            if (cur_size>proc_zone_size[ip-1]) {
               np = ip;
               cur_size = proc_zone_size[ip-1];
            }
         }
         ip = np - 1;
//
//  ...   get a zone that has the largest communication index with
//        the current group and does not worsen the computation balance
         mz = z_order[iz-1];
         if (iz < num_zones) {
            get_comm_index (mz,ip,&zone_comm);
            do (z2 , iz+1, num_zones,1) {
               zone = z_order[z2-1];

               diff_ratio =(zone_size[z_order[iz-1]-1] - zone_size[zone-1]) / zone_size[z_order[iz-1]-1];
               if (diff_ratio > 0.05e0) goto lll120;

               if (zone_proc_id[zone-1] < 0) {
                  get_comm_index (zone,ip,&comm_index);
                  if (comm_index > zone_comm) {
                     mz = zone;
                     zone_comm = comm_index;
                  }
               }
            }
         }
//
//  ...   assign the zone to the current thread group
lll120:;
         zone_proc_id[mz-1] = ip;
         proc_zone_size[np-1] = proc_zone_size[np-1] + zone_size[mz-1];
         proc_zone_count[np-1] = proc_zone_count[np-1] + 1;
//
//  ...   skip the previously assigned zones
         while(iz<=num_zones) {
            if (zone_proc_id[z_order[iz-1]-1]<0) goto lll130;
            iz = iz + 1;
         }
lll130:;
      }
//
// ... move threads around if needed
      mz = 1;
      if (tot_threads==num_othreads || mz_bload<1) mz = 0;
//
      if (mz != 0) {
//
         do (ipg , 1, num_othreads,1) {
            proc_group_flag[ipg-1] = 0;
         }
//
         ipg = 1;
//
// ...    balance load within a thread group
lll200:;
         while(ipg <= num_othreads) {
            if (proc_group_flag[ipg-1] == 0) goto lll210;
            ipg = ipg + 1;
         }
lll210:;
         if (ipg > num_othreads) goto lll300;
//
         group = proc_group[ipg-1];
         tot_group_size = 0.e0;
         tot_group_threads = 0;
         do (ip , ipg, num_othreads,1) {
            if (proc_group[ip-1] == group) {
               proc_group_flag[ip-1] = 1;
               tot_group_size = tot_group_size + proc_zone_size[ip-1];
               tot_group_threads = tot_group_threads + proc_num_threads[ip-1];
            }
         }
//
         ave_size = tot_group_size/tot_group_threads;
//
//  ...   distribute size evenly among threads
         icur_size = 0;
         do (ip , 1, num_othreads,1) {
            if (proc_group[ip-1] != group) goto lll220;
            proc_num_threads[ip-1] = proc_zone_size[ip-1] / ave_size;
            if (proc_num_threads[ip-1] < 1)          proc_num_threads[ip-1] = 1;
            if (max_threads > 0 &&           proc_num_threads[ip-1] > max_threads)           proc_num_threads[ip-1] = max_threads;
            icur_size = icur_size + proc_num_threads[ip-1];
lll220:;
         }
         mz = tot_group_threads - icur_size;
//
//  ...   take care of any remainers
         inc = 1;
         if (mz < 0) inc = -1;
         while(mz != 0) {
            max_size = 0.e0;
            imx = 0;
            do (ip , 1, num_othreads,1) {
               if (proc_group[ip-1] != group) goto lll230;
               if (mz > 0) {
                  cur_size = proc_zone_size[ip-1] / proc_num_threads[ip-1];
                  if (cur_size>max_size &&(max_threads<=0                || proc_num_threads[ip-1]<max_threads)) {
                     max_size = cur_size;
                     imx = ip;
                  }
               } else if (proc_num_threads[ip-1] > 1) {
                  cur_size = proc_zone_size[ip-1] /(proc_num_threads[ip-1]-1);
                  if (max_size==0 || cur_size<max_size) {
                     max_size = cur_size;
                     imx = ip;
                  }
               }
lll230:;
            }
            proc_num_threads[imx-1] = proc_num_threads[imx-1] + inc;
            mz = mz - inc;
         }
//
         goto lll200;
      }
//
// ... print the mapping
lll300:;
      if (npb_verbose > 0) {
         printf("\n Zone-othread mapping:\n othread nzones zone_size nthreads size_per_thread\n");
         do (ip , 1, num_othreads,1) {
            printf("%5d   %5d   %10.0f   %5d    %10.0f\n", ip-1,proc_zone_count[ip-1], proc_zone_size[ip-1],proc_num_threads[ip-1], proc_zone_size[ip-1]/proc_num_threads[ip-1]);
            do (iz , 1, num_zones,1) {
               if (zone_proc_id[iz-1] == ip-1) {
                  printf("   zone  %5d   %9.0f\n", iz, zone_size[iz-1]);
               }
            }
         }
      }
//
      imx = 1;
      max_size = proc_zone_size[0]/proc_num_threads[0];
      imn = imx;
      ave_size = max_size;
      do (ip , 2, num_othreads,1) {
         cur_size = proc_zone_size[ip-1]/proc_num_threads[ip-1];
         if (cur_size>max_size) {
            imx = ip;
            max_size = cur_size;
         }
         if (cur_size<ave_size) {
            imn = ip;
            ave_size = cur_size;
         }
      }

      if (npb_verbose > 0) {
         printf("\n");
         printf("  %s: othread= %5d nzones= %5d size= %10.0f nthreads= %5d\n", "Max", imx-1, proc_zone_count[imx-1], proc_zone_size[imx-1],proc_num_threads[imx-1]);
         printf("  %s: othread= %5d nzones= %5d size= %10.0f nthreads= %5d\n", "Min", imn-1, proc_zone_count[imn-1], proc_zone_size[imn-1],proc_num_threads[imn-1]);
      }

      printf("\n Calculated speedup =  %9.2f\n\n", tot_size / max_size);
//
      return;
}//end

//---------------------------------------------------------------------
//---------------------------------------------------------------------

