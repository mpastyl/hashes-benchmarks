#include <stdio.h>
#include <stdlib.h>
#include <omp.h>

//#include "non_blocking.h"
//#include "split_ordered.h"
//#include "refinable.h"
#include "cuckoo.h"

int main(int argc, char **argv){

    int num_threads;
    int finds;
    int deletes;
    int inserts;

    num_threads = atoi(argv[1]);
    finds = atoi(argv[2]);
    deletes = atoi(argv[3]);
    inserts = atoi(argv[4]);
    printf("num_threads: %d finds:%d deletes:%d  inserts:%d\n",num_threads,finds,deletes,inserts);
    
    struct HashSet H;
    initialize(&H,32768*512);

    //Testing
    int count_per_thread = 300000/num_threads;
    srand(50);
    int rand_value[300000];
    int i;
    long long int ins_sum=0;
    for(i=0;i<300000;i++) rand_value[i] = rand()%100000;
    #pragma omp parallel num_threads(num_threads) private(i) reduction(+:ins_sum)
    {
        int j;
        int res;
        int index;
        for(j=0;j<count_per_thread;j++){
            index = (omp_get_thread_num()*count_per_thread +j )%300000;
            res = Insert(&H,rand_value[index]);
            if(res) ins_sum+=rand_value[index];
            /*else {
                //printf("could not insert: tid %d value %d\n", omp_get_thread_num(),rand_value[index]);
                res = Insert(&H,rand_value[index]);
                if(!res) printf("insert failed\n");
            }
            */
        }
    }
    long long int find_sum=0;
    int res;
    for(i=0;i<100000;i++){
           res=Lookup(&H,i);
            if (res) find_sum+=i;
        
    }
    printf("insert sum %lld find_sum %lld\n",ins_sum,find_sum);
} 
