#include "header.h"

void exact_solution (xi,eta,zeta,dtemp)
// beg param
       double xi;
       double eta;
       double zeta;
       double dtemp[5];
// end param
{

//---------------------------------------------------------------------
//---------------------------------------------------------------------

//---------------------------------------------------------------------
//     this function returns the exact solution at point xi, eta, zeta  
//---------------------------------------------------------------------


      int m;

      do (m , 1, 5,1) {
         dtemp[m-1] =  ce[0][m-1] + xi*(ce[1][m-1] + xi*(ce[4][m-1] + xi*(ce[8-1][m-1] + xi*ce[11-1][m-1]))) + eta*(ce[2][m-1] + eta*(ce[5][m-1] + eta*(ce[9-1][m-1] + eta*ce[12-1][m-1])))+ zeta*(ce[3][m-1] + zeta*(ce[7-1][m-1] + zeta*(ce[10-1][m-1] + zeta*ce[13-1][m-1])));
      }

      return;
}//end



