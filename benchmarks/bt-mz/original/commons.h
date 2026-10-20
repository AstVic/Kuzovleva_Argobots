#include "npbparams.h"
// COMMONs
#define do(v,l,h,s) for(v=(l); v<=(h); v+=s)
#define dom(v,l,h,s) for(v=(l); v>=(h); v+=s)
#define mod(x,y)((x)%(y))
#define NUM_ZONES   X_ZONES*Y_ZONES
#define AA   1
#define BB   2
#define CC   3
#define BLOCK_SIZE   5
#define MAX_ZONES   X_ZONES*Y_ZONES
#define T_TOTAL   1
#define T_RHSX   2
#define T_RHSY   3
#define T_RHSZ   4
#define T_RHS   5
#define T_XSOLVE   6
#define T_YSOLVE   7
#define T_ZSOLVE   8
#define T_RDIS1   9
#define T_RDIS2   10
#define T_ADD   11
#define T_LAST   11
FILE *fstatus;


//COMMON from "bt_all.c"

double  u[PROC_MAX_SIZE5];
double us[PROC_MAX_SIZE ];
double vs[PROC_MAX_SIZE ];
double ws[PROC_MAX_SIZE ];
double qs[PROC_MAX_SIZE ];
double rho_i[PROC_MAX_SIZE ];
double square[PROC_MAX_SIZE ];
double rhs[PROC_MAX_SIZE5];
double forcing[PROC_MAX_SIZE5];
double qbc[PROC_MAX_BCSIZE];

//COMMON from "header.h"

int  npb_verbose;
logical  timeron;
double  tx1;
double tx2;
double tx3;
double ty1;
double ty2;
double ty3;
double tz1;
double tz2;
double tz3;
double dx1;
double dx2;
double dx3;
double dx4;
double dx5;
double dy1;
double dy2;
double dy3;
double dy4;
double dy5;
double dz1;
double dz2;
double dz3;
double dz4;
double dz5;
double dssp;
double dt;
double ce[13][5];
double dxmax;
double dymax;
double dzmax;
double xxcon1;
double xxcon2;
double xxcon3;
double xxcon4;
double xxcon5;
double dx1tx1;
double dx2tx1;
double dx3tx1;
double dx4tx1;
double dx5tx1;
double yycon1;
double yycon2;
double yycon3;
double yycon4;
double yycon5;
double dy1ty1;
double dy2ty1;
double dy3ty1;
double dy4ty1;
double dy5ty1;
double zzcon1;
double zzcon2;
double zzcon3;
double zzcon4;
double zzcon5;
double dz1tz1;
double dz2tz1;
double dz3tz1;
double dz4tz1;
double dz5tz1;
double dnxm1;
double dnym1;
double dnzm1;
double c1c2;
double c1c5;
double c3c4;
double c1345;
double conz1;
double c1;
double c2;
double c3;
double c4;
double c5;
double c4dssp;
double c5dssp;
double dtdssp;
double dttx1;
double dttx2;
double dtty1;
double dtty2;
double dttz1;
double dttz2;
double c2dttx1;
double c2dtty1;
double c2dttz1;
double comz1;
double comz4;
double comz5;
double comz6;
double c3c4tx3;
double c3c4ty3;
double c3c4tz3;
double c2iv;
double con43;
double con16;
double  cuf[(PROBLEM_SIZE)-(0)+1];
double q[(PROBLEM_SIZE)-(0)+1];
double ue[5][(PROBLEM_SIZE)-(0)+1];
double buf[5][(PROBLEM_SIZE)-(0)+1];
#pragma omp threadprivate(cuf, q, ue, buf)
int  x_start[X_ZONES];
int x_end[X_ZONES];
int x_size[X_ZONES];
int y_start[Y_ZONES];
int y_end[Y_ZONES];
int y_size[Y_ZONES];
int iz_west[MAX_ZONES];
int iz_east[MAX_ZONES];
int iz_south[MAX_ZONES];
int iz_north[MAX_ZONES];
int  start1[MAX_ZONES];
int start5[MAX_ZONES];
int qstart_west[MAX_ZONES];
int qstart_east[MAX_ZONES];
int  qstart_south[MAX_ZONES];
int qstart_north[MAX_ZONES];

//COMMON from "omp_stuff.h"

int  zone_proc_id[MAX_ZONES];
int  proc_zone_count[MAX_ZONES];
int  proc_num_threads[MAX_ZONES];
int  proc_group[MAX_ZONES];
double  proc_zone_size[MAX_ZONES];
int  myid;
int root;
//#pragma omp threadprivate(myid, root)
int num_othreads;
int num_threads;
int mz_bload;
int max_threads;
int nested;

//COMMON from "work_lhs.h"

double  fjac[(PROBLEM_SIZE)-(0)+1][ 5][5];
double njac[(PROBLEM_SIZE)-(0)+1][ 5][5];
double lhs[(PROBLEM_SIZE)-(0)+1][ 3][ 5][5];
double rtmp[(PROBLEM_SIZE)-(0)+1][5];
double tmp1;
double tmp2;
double tmp3;
//#pragma omp threadprivate(fjac, njac, lhs, rtmp, tmp1, tmp2, tmp3)
