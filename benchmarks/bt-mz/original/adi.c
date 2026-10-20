#include "header.h"

void adi (orho_i,ous,ovs,ows,oqs,osquare,orhs,oforcing,ou,nx,nxmax,ny,nz)
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
// end param adi
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

      compute_rhs (rho_i, us, vs, ws, qs, square, rhs, forcing, u,nx,nxmax,ny,nz);

      x_solve (rho_i, qs, square, u, rhs,nx,nxmax,ny,nz);

      y_solve (rho_i, qs, square, u, rhs,nx,nxmax,ny,nz);

      z_solve (rho_i, qs, square, u, rhs,nx,nxmax,ny,nz);

      add (u, rhs,nx,nxmax,ny,nz);

      return;
}//end


//---------------------------------------------------------------------
//---------------------------------------------------------------------

