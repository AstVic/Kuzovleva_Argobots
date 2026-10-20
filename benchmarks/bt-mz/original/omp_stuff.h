//
extern      int  zone_proc_id[MAX_ZONES];
// othread_id for each zone
extern      int  proc_zone_count[MAX_ZONES];
// #zones assigned to othread
extern      int  proc_num_threads[MAX_ZONES];
// #ithreads for each othread
extern      int  proc_group[MAX_ZONES];
// group_id for each othread
extern      double  proc_zone_size[MAX_ZONES];
//
extern      int  myid;
extern      int root;
extern      int num_othreads;
extern      int num_threads;
extern      int mz_bload;
extern      int max_threads;
extern      int nested;
//$omp threadprivate(/omp_cmn2a/)

