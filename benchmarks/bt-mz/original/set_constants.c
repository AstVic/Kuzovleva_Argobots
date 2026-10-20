#include "header.h"

void set_constants ()
// beg param
// end param
{

//---------------------------------------------------------------------
//---------------------------------------------------------------------


      ce[0][0]  = 2.0e0;
      ce[1][0]  = 0.0e0;
      ce[2][0]  = 0.0e0;
      ce[3][0]  = 4.0e0;
      ce[4][0]  = 5.0e0;
      ce[5][0]  = 3.0e0;
      ce[7-1][0]  = 0.5e0;
      ce[8-1][0]  = 0.02e0;
      ce[9-1][0]  = 0.01e0;
      ce[10-1][0] = 0.03e0;
      ce[11-1][0] = 0.5e0;
      ce[12-1][0] = 0.4e0;
      ce[13-1][0] = 0.3e0;

      ce[0][1]  = 1.0e0;
      ce[1][1]  = 0.0e0;
      ce[2][1]  = 0.0e0;
      ce[3][1]  = 0.0e0;
      ce[4][1]  = 1.0e0;
      ce[5][1]  = 2.0e0;
      ce[7-1][1]  = 3.0e0;
      ce[8-1][1]  = 0.01e0;
      ce[9-1][1]  = 0.03e0;
      ce[10-1][1] = 0.02e0;
      ce[11-1][1] = 0.4e0;
      ce[12-1][1] = 0.3e0;
      ce[13-1][1] = 0.5e0;

      ce[0][2]  = 2.0e0;
      ce[1][2]  = 2.0e0;
      ce[2][2]  = 0.0e0;
      ce[3][2]  = 0.0e0;
      ce[4][2]  = 0.0e0;
      ce[5][2]  = 2.0e0;
      ce[7-1][2]  = 3.0e0;
      ce[8-1][2]  = 0.04e0;
      ce[9-1][2]  = 0.03e0;
      ce[10-1][2] = 0.05e0;
      ce[11-1][2] = 0.3e0;
      ce[12-1][2] = 0.5e0;
      ce[13-1][2] = 0.4e0;

      ce[0][3]  = 2.0e0;
      ce[1][3]  = 2.0e0;
      ce[2][3]  = 0.0e0;
      ce[3][3]  = 0.0e0;
      ce[4][3]  = 0.0e0;
      ce[5][3]  = 2.0e0;
      ce[7-1][3]  = 3.0e0;
      ce[8-1][3]  = 0.03e0;
      ce[9-1][3]  = 0.05e0;
      ce[10-1][3] = 0.04e0;
      ce[11-1][3] = 0.2e0;
      ce[12-1][3] = 0.1e0;
      ce[13-1][3] = 0.3e0;

      ce[0][4]  = 5.0e0;
      ce[1][4]  = 4.0e0;
      ce[2][4]  = 3.0e0;
      ce[3][4]  = 2.0e0;
      ce[4][4]  = 0.1e0;
      ce[5][4]  = 0.4e0;
      ce[7-1][4]  = 0.3e0;
      ce[8-1][4]  = 0.05e0;
      ce[9-1][4]  = 0.04e0;
      ce[10-1][4] = 0.03e0;
      ce[11-1][4] = 0.1e0;
      ce[12-1][4] = 0.3e0;
      ce[13-1][4] = 0.2e0;

      c1 = 1.4e0;
      c2 = 0.4e0;
      c3 = 0.1e0;
      c4 = 1.0e0;
      c5 = 1.4e0;

//     The following three settings are based on average cell size,
//     not actual cell size.

      dnxm1 = 1.0e0 /((double) (GX_SIZE-1)/(double) (X_ZONES));
      dnym1 = 1.0e0 /((double) (GY_SIZE-1)/(double) (Y_ZONES));
      dnzm1 = 1.0e0 /((double) (GZ_SIZE-1));

      c1c2 = c1 * c2;
      c1c5 = c1 * c5;
      c3c4 = c3 * c4;
      c1345 = c1c5 * c3c4;

      conz1 =(1.0e0-c1c5);

      tx1 = 1.0e0 /(dnxm1 * dnxm1);
      tx2 = 1.0e0 /(2.0e0 * dnxm1);
      tx3 = 1.0e0 / dnxm1;

      ty1 = 1.0e0 /(dnym1 * dnym1);
      ty2 = 1.0e0 /(2.0e0 * dnym1);
      ty3 = 1.0e0 / dnym1;

      tz1 = 1.0e0 /(dnzm1 * dnzm1);
      tz2 = 1.0e0 /(2.0e0 * dnzm1);
      tz3 = 1.0e0 / dnzm1;

      dx1 = 0.75e0;
      dx2 = 0.75e0;
      dx3 = 0.75e0;
      dx4 = 0.75e0;
      dx5 = 0.75e0;

      dy1 = 0.75e0;
      dy2 = 0.75e0;
      dy3 = 0.75e0;
      dy4 = 0.75e0;
      dy5 = 0.75e0;

      dz1 = 1.0e0;
      dz2 = 1.0e0;
      dz3 = 1.0e0;
      dz4 = 1.0e0;
      dz5 = 1.0e0;

      dxmax = Max (dx3, dx4);
      dymax = Max (dy2, dy4);
      dzmax = Max (dz2, dz3);

      dssp = 0.25e0 * Max (dx1, Max (dy1, dz1) );

      c4dssp = 4.0e0 * dssp;
      c5dssp = 5.0e0 * dssp;

      dttx1 = dt*tx1;
      dttx2 = dt*tx2;
      dtty1 = dt*ty1;
      dtty2 = dt*ty2;
      dttz1 = dt*tz1;
      dttz2 = dt*tz2;

      c2dttx1 = 2.0e0*dttx1;
      c2dtty1 = 2.0e0*dtty1;
      c2dttz1 = 2.0e0*dttz1;

      dtdssp = dt*dssp;

      comz1  = dtdssp;
      comz4  = 4.0e0*dtdssp;
      comz5  = 5.0e0*dtdssp;
      comz6  = 6.0e0*dtdssp;

      c3c4tx3 = c3c4*tx3;
      c3c4ty3 = c3c4*ty3;
      c3c4tz3 = c3c4*tz3;

      dx1tx1 = dx1*tx1;
      dx2tx1 = dx2*tx1;
      dx3tx1 = dx3*tx1;
      dx4tx1 = dx4*tx1;
      dx5tx1 = dx5*tx1;

      dy1ty1 = dy1*ty1;
      dy2ty1 = dy2*ty1;
      dy3ty1 = dy3*ty1;
      dy4ty1 = dy4*ty1;
      dy5ty1 = dy5*ty1;

      dz1tz1 = dz1*tz1;
      dz2tz1 = dz2*tz1;
      dz3tz1 = dz3*tz1;
      dz4tz1 = dz4*tz1;
      dz5tz1 = dz5*tz1;

      c2iv  = 2.5e0;
      con43 = 4.0e0/3.0e0;
      con16 = 1.0e0/6.0e0;

      xxcon1 = c3c4tx3*con43*tx3;
      xxcon2 = c3c4tx3*tx3;
      xxcon3 = c3c4tx3*conz1*tx3;
      xxcon4 = c3c4tx3*con16*tx3;
      xxcon5 = c3c4tx3*c1c5*tx3;

      yycon1 = c3c4ty3*con43*ty3;
      yycon2 = c3c4ty3*ty3;
      yycon3 = c3c4ty3*conz1*ty3;
      yycon4 = c3c4ty3*con16*ty3;
      yycon5 = c3c4ty3*c1c5*ty3;

      zzcon1 = c3c4tz3*con43*tz3;
      zzcon2 = c3c4tz3*tz3;
      zzcon3 = c3c4tz3*conz1*tz3;
      zzcon4 = c3c4tz3*con16*tz3;
      zzcon5 = c3c4tz3*c1c5*tz3;

      return;
}//end


//---------------------------------------------------------------------
//---------------------------------------------------------------------

