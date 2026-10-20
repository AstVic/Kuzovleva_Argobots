
void add (
       void *ou,
       void *orhs,
       int  nx,
       int nxmax,
       int ny,
       int nz
);
void adi (
       void *orho_i,
       void *ous,
       void *ovs,
       void *ows,
       void *oqs,
       void *osquare,
       void *orhs,
       void *oforcing,
       void *ou,
       int  nx,
       int nxmax,
       int ny,
       int nz
);
void error_norm (
       double rms[5],
       void *ou,
       int  nx,
       int nxmax,
       int ny,
       int nz
);
void rhs_norm (
       double  rms[5],
       void *orhs,
       int  nx,
       int nxmax,
       int ny,
       int nz
);
void exact_rhs (
       void *oforcing,
       int  nx,
       int nxmax,
       int ny,
       int nz
);
void exact_solution (
       double  xi,
       double eta,
       double zeta,
       double dtemp[5]
);
void exch_qbc (
       double  u[],
       double qbc[],
       int  nx[],
       int nxmax[],
       int ny[],
       int nz[],
       int proc_zone_id[],
       int proc_num_zones
);
void copy_y_face (
       void *ou,
       void *oqbc,
       int  nx,
       int nxmax,
       int ny,
       int nz,
       int jloc,
       char* dir
);
void copy_x_face (
       void *ou,
       void *oqbc,
       int  nx,
       int nxmax,
       int ny,
       int nz,
       int iloc,
       char* dir
);
void initialize (
       void *ou,
       int  nx,
       int nxmax,
       int ny,
       int nz
);
void lhsinit (
       double  lhs[][3][5][5],
       int  size
);
void omp_setup (
       int  num_zones,
       int nx[],
       int ny[],
       int nz[],
       int tot_threads
);
void omp_init (
       int  num_zones,
       int proc_zone_id[],
       int (*proc_num_zones)
);
void env_setup (
       int  (*tot_threads)
);
void decode_line (
       char line[80],
       int  (*ip1),
       int (*ip2),
       int (*curr_threads),
       int (*group),
       int (*ios)
);
void get_comm_index (
       int  zone,
       int iproc,
       int (*comm_index)
);
void map_zones (
       int  num_zones,
       int nx[],
       int ny[],
       int nz[],
       int tot_threads
);
void compute_rhs (
       void *orho_i,
       void *ous,
       void *ovs,
       void *ows,
       void *oqs,
       void *osquare,
       void *orhs,
       void *oforcing,
       void *ou,
       int  nx,
       int nxmax,
       int ny,
       int nz
);
void set_constants (
);
void matvec_sub (
       void *oablock,
       double avec[5],
       double bvec[5]
);
void matmul_sub (
       void *oablock,
       void *obblock,
       void *ocblock
);
void binvcrhs (
       void *olhs,
       void *oc,
       double r[5]
);
void binvrhs (
       void *olhs,
       double  r[5]
);
void verify (
       int no_time_steps,
       logical  (*verified),
       int num_zones,
       double  rho_i[],
       double us[],
       double vs[],
       double ws[],
       double qs[],
       double square[],
       double rhs[],
       double forcing[],
       double u[],
       int  nx[],
       int nxmax[],
       int ny[],
       int nz[],
       int proc_zone_id[],
       int proc_num_zones
);
void x_solve (
       void *orho_i,
       void *oqs,
       void *osquare,
       void *ou,
       void *orhs,
       int  nx,
       int nxmax,
       int ny,
       int nz
);
void y_solve (
       void *orho_i,
       void *oqs,
       void *osquare,
       void *ou,
       void *orhs,
       int  nx,
       int nxmax,
       int ny,
       int nz
);
void zone_setup (
       int  nx[],
       int nxmax[],
       int ny[],
       int nz[]
);
void zone_starts (
       int  num_zones,
       int  nx[],
       int nxmax[],
       int ny[],
       int nz[]
);
void z_solve (
       void *orho_i,
       void *oqs,
       void *osquare,
       void *ou,
       void *orhs,
       int  nx,
       int nxmax,
       int ny,
       int nz
);
