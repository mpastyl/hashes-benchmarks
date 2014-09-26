#include <stdio.h>
#include <stdlib.h>
#include <omp.h>

//#include "non_blocking.h"
//#include "split_ordered.h"
#include "refinable.h"
//#include "striped.h"
//#include "cuckoo.h"
//#include "hash_over_numa.h"
//#include "global_lock.h"
#include "timers_lib.h"
#include "tsc.h"

int main(int argc, char **argv){

    int num_threads;
    int finds;
    int deletes;
    int inserts;

    num_threads = atoi(argv[1]);
    finds = atoi(argv[2]);
    deletes = atoi(argv[3]);
    inserts = atoi(argv[4]);
    printf("num_threads: %d finds:%d%% deletes:%d%%  inserts:%d%%\n",num_threads,finds,deletes,inserts);
    int del_limit = deletes;
    int insert_limit = deletes+inserts;


    struct HashSet H;
    initialize(&H,4096*4);
    int count_per_thread = 10000000/num_threads;
    int i;
    double tsc_time=0;
    timer_tt *wall_timer;
    long long int total_succ_enqs=0;
    ///////////// preinsertion /////////
    int count_per_thread_a=5000000/24; 
    int count_a=0;
    //#pragma omp parallel num_threads(24) private(i)
    {
    struct drand48_data drand_buffer_a;
    long int drand_res_a;
    int value;
    int res_a;
    srand48_r(omp_get_thread_num()*100,&drand_buffer_a);
    for(i=0;i<5000000;i++){
        lrand48_r(&drand_buffer_a,&drand_res_a);
        value=drand_res_a;
        res_a=Insert(&H,value);
        if (res_a) count_a++;
        //printf("%d\n",i);
    }
    }
    printf("finshed initialization count %d\n",count_a);
    ///////////////////////////////////    
    
    wall_timer = timer_init();
    timer_start(wall_timer);
    #pragma omp parallel num_threads(num_threads) private(i) reduction(+:tsc_time) reduction(+:total_succ_enqs)
    {
        tsc_t tsc_timer;
        struct drand48_data drand_buffer;
        long int drand_res;
        int j;
        int tid = omp_get_thread_num();
        int op;
        int val;
        int wait;
        int res;
        long long int succ_enqs=0;
        tsc_init(&tsc_timer);
        srand48_r(tid*100,&drand_buffer);
        for (i = 0; i < count_per_thread; i++){
            lrand48_r(&drand_buffer,&drand_res);
            op = drand_res%100;
            lrand48_r(&drand_buffer,&drand_res);
            val = drand_res;
            lrand48_r(&drand_buffer,&drand_res);
            wait = drand_res% 1000;

            //wait 
            for( j = 0;j<wait;j++);
            tsc_start(&tsc_timer);
            if(op<del_limit) res=Erase(&H,val);
            else if (op<insert_limit) {
                res=Insert(&H,val);
                if (res) succ_enqs++;
            }
            else res=Lookup(&H,val);
            tsc_pause(&tsc_timer);
         }
        total_succ_enqs=succ_enqs;
        tsc_time = tsc_getsecs(&tsc_timer);
        #ifdef __HASH_OVER_NUMA_H
        printf("Time(TSC): %4.6lf\n", tsc_time);
        help_other_threads();
        #endif
    }
    timer_stop(wall_timer);
    printf("Time(sec): %4.6lf\n", timer_report_sec(wall_timer));
    printf("Time(TSC): %4.6lf\n", tsc_time / (double) num_threads);
    printf("total succesfull enqueues %lld\n",total_succ_enqs);
    return 1;
}
