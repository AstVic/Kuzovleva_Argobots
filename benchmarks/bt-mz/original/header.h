#define AA   1
#define do(v,l,h,s) for(v=(l); v<=(h); v+=s)
#define dom(v,l,h,s) for(v=(l); v>=(h); v+=s)
#define mod(x,y)((x)%(y))

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include "c_timers.h"

#include "type.h"

double timer_read(int);
#include "print_results.h"



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
extern      FILE *fstatus;

//---------------------------------------------------------------------
//---------------------------------------------------------------------
//
//  header.h
//
//---------------------------------------------------------------------
//---------------------------------------------------------------------

//      implicit none

//---------------------------------------------------------------------
// The following include file is generated automatically by the
// "setparams" utility. It defines
//      PROBLEM_SIZE:  maximum overall grid size
//      DT_DEFAULT:    default time step for this problem size if no
//                     config file
//      NITER_DEFAULT: default number of iterations for this problem size
//---------------------------------------------------------------------

#include "npbparams.h"

extern      int  myid;
extern      int root;
#pragma omp threadprivate(myid, root)

extern      int  npb_verbose;
//      double  elapsed_time;
extern      logical  timeron;

extern      double  tx1;
extern      double tx2;
extern      double tx3;
extern      double ty1;
extern      double ty2;
extern      double ty3;
extern      double tz1;
extern      double tz2;
extern      double tz3;
extern      double dx1;
extern      double dx2;
extern      double dx3;
extern      double dx4;
extern      double dx5;
extern      double dy1;
extern      double dy2;
extern      double dy3;
extern      double dy4;
extern      double dy5;
extern      double dz1;
extern      double dz2;
extern      double dz3;
extern      double dz4;
extern      double dz5;
extern      double dssp;
extern      double dt;
extern      double ce[13][5];
extern      double dxmax;
extern      double dymax;
extern      double dzmax;
extern      double xxcon1;
extern      double xxcon2;
extern      double xxcon3;
extern      double xxcon4;
extern      double xxcon5;
extern      double dx1tx1;
extern      double dx2tx1;
extern      double dx3tx1;
extern      double dx4tx1;
extern      double dx5tx1;
extern      double yycon1;
extern      double yycon2;
extern      double yycon3;
extern      double yycon4;
extern      double yycon5;
extern      double dy1ty1;
extern      double dy2ty1;
extern      double dy3ty1;
extern      double dy4ty1;
extern      double dy5ty1;
extern      double zzcon1;
extern      double zzcon2;
extern      double zzcon3;
extern      double zzcon4;
extern      double zzcon5;
extern      double dz1tz1;
extern      double dz2tz1;
extern      double dz3tz1;
extern      double dz4tz1;
extern      double dz5tz1;
extern      double dnxm1;
extern      double dnym1;
extern      double dnzm1;
extern      double c1c2;
extern      double c1c5;
extern      double c3c4;
extern      double c1345;
extern      double conz1;
extern      double c1;
extern      double c2;
extern      double c3;
extern      double c4;
extern      double c5;
extern      double c4dssp;
extern      double c5dssp;
extern      double dtdssp;
extern      double dttx1;
extern      double dttx2;
extern      double dtty1;
extern      double dtty2;
extern      double dttz1;
extern      double dttz2;
extern      double c2dttx1;
extern      double c2dtty1;
extern      double c2dttz1;
extern      double comz1;
extern      double comz4;
extern      double comz5;
extern      double comz6;
extern      double c3c4tx3;
extern      double c3c4ty3;
extern      double c3c4tz3;
extern      double c2iv;
extern      double con43;
extern      double con16;


extern      double  cuf[(PROBLEM_SIZE)-(0)+1];
extern      double q[(PROBLEM_SIZE)-(0)+1];
extern      double ue[5][(PROBLEM_SIZE)-(0)+1];
extern      double buf[5][(PROBLEM_SIZE)-(0)+1];
#pragma omp threadprivate(cuf, q, ue, buf)
//$OMP THREADPRIVATE(/work_1d/)

extern      int  x_start[X_ZONES];
extern      int x_end[X_ZONES];
extern      int x_size[X_ZONES];
extern      int y_start[Y_ZONES];
extern      int y_end[Y_ZONES];
extern      int y_size[Y_ZONES];
extern      int iz_west[MAX_ZONES];
extern      int iz_east[MAX_ZONES];
extern      int iz_south[MAX_ZONES];
extern      int iz_north[MAX_ZONES];

extern      int  start1[MAX_ZONES];
extern      int start5[MAX_ZONES];
extern      int qstart_west[MAX_ZONES];
extern      int qstart_east[MAX_ZONES];
extern      int  qstart_south[MAX_ZONES];
extern      int qstart_north[MAX_ZONES];

//-----------------------------------------------------------------------
//   Timer constants
//-----------------------------------------------------------------------

extern      double  fjac[(PROBLEM_SIZE)-(0)+1][ 5][5];
extern      double njac[(PROBLEM_SIZE)-(0)+1][ 5][5];
extern      double lhs[(PROBLEM_SIZE)-(0)+1][ 3][ 5][5];
extern      double rtmp[(PROBLEM_SIZE)-(0)+1][5];
extern      double tmp1;
extern      double tmp2;
extern      double tmp3;
#pragma omp threadprivate(fjac, njac, lhs, rtmp, tmp1, tmp2, tmp3)

#include "subr.h"
