#ifndef __HASH_OVER_NUMA_H
#define __HASH_OVER_NUMA_H
// Low level implementation

struct NodeType{
    unsigned int key;
    unsigned long long marked_next;
};


unsigned long long get_count(unsigned long long a){

    unsigned long long b = a >>63;
    return b;
}

unsigned long long get_pointer(unsigned long long a){
    unsigned long long b = a << 1;
    b= b >>1;
    return b;
}

unsigned long long set_count(unsigned long long  a, unsigned long long count){
    unsigned long long count_temp =  count << 63;
    unsigned long long b = get_pointer(a);
    b = b | count_temp;
    return b;
}

unsigned long long set_pointer(unsigned long long a, unsigned long long ptr){
    unsigned long long b = 0;
    unsigned long long c = get_count(a);
    b = set_count(b,c);
    ptr = get_pointer(ptr);
    b= b | ptr;
    return b;
}

unsigned long long set_both(unsigned long long a,unsigned long long ptr, unsigned long long count){
    a=set_pointer(a,ptr);
    a=set_count(a,count);
    return a;
}


//they must all be thread-private
struct priv_pointers{

    unsigned long long   * prev;
    unsigned long long   curr;
    unsigned long long   next;
};

struct priv_pointers priv0;
struct priv_pointers priv1;
struct priv_pointers priv2;
struct priv_pointers priv3;
#pragma omp threadprivate(priv0,priv1,priv2,priv3)

unsigned long long * Head=0;

int list_insert(unsigned long long * head, struct NodeType * node,struct priv_pointers *priv){
    
    

    int res;
    int temp=0;
    unsigned int key=node->key;
    
    while (1){
        if (list_find(&head,key,priv)) return 0;
        node->marked_next = set_both(node->marked_next,get_pointer(priv->curr),0);
        
        unsigned long long compare_value = set_both(compare_value,get_pointer(priv->curr),0);
        unsigned long long new_value = set_both(new_value,(unsigned long long ) node,0);
        
        //if((*prev)!=compare_value){ if(temp==0)printf("wtf!! %lld %lld\n",*prev,compare_value);}
        temp++;
        res =__sync_bool_compare_and_swap(priv->prev,compare_value,new_value);
        if (res){
            //Head=head;
            return 1;
        }
     }
}


int list_delete(unsigned long long *head ,unsigned int key,struct priv_pointers *priv){
    
    while (1){
        if (!list_find(&head,key))  return 0;
        unsigned long long compare_value = set_both(compare_value,get_pointer(priv->next),0);
        unsigned long long new_value = set_both(new_value,get_pointer(priv->next),1);

        if(!__sync_bool_compare_and_swap(&(((struct NodeType *)get_pointer(priv->curr))->marked_next),compare_value,new_value)) 
            continue;


        compare_value = set_both(compare_value,get_pointer(priv->curr),0);
        new_value = set_both(new_value,get_pointer(priv->next),0);

        if(__sync_bool_compare_and_swap(priv->prev,compare_value,new_value))
            free((struct NodeType *)get_pointer(priv->curr));

        else list_find(&head,key);
        //Head=head;//TODO: thats not very safe
        return 1;

    }
}


int list_find(unsigned long long ** head,unsigned int key,struct priv_pointers *priv){
    
    try_again:
        priv->prev=(unsigned long long *)*head;
        priv->curr=set_both(priv->curr,get_pointer(*(priv->prev)),get_count(*(priv->prev)));
        //printf("#t %d &curr= %p\n",omp_get_thread_num(),&curr);
        while (1){

            if(get_pointer(priv->curr)==0) return 0;
            
            unsigned long long pointer=get_pointer(((struct NodeType * )get_pointer(priv->curr))->marked_next);
            unsigned long long mark_bit = get_count(((struct NodeType * )get_pointer(priv->curr))->marked_next);
            
            priv->next = set_both(priv->next,pointer,mark_bit);       
            unsigned int ckey= ((struct NodeType *)get_pointer(priv->curr))->key;
            unsigned long long check=set_both(check,priv->curr,0);
            if ((*(priv->prev)) !=check) goto  try_again;

            if (get_count(priv->next)==0){
                if (ckey>=key)
                    return (ckey==key);
                priv->prev = &(((struct NodeType *)get_pointer(priv->curr))->marked_next);   
            }

            else{
                
                unsigned long long compare_value = set_both(compare_value,priv->curr,0);
                unsigned long long new_value = set_both(new_value,priv->next,0);

                if (__sync_bool_compare_and_swap(priv->prev,compare_value,new_value)){
                    free((struct NodeType *)get_pointer(priv->curr));
                    //printf("Hey!\n");
                    }

                else goto try_again;
            }

            priv->curr=set_both(priv->curr,priv->next,get_count(priv->next));
       }
       
}              





//reverse the bits of a 32-bit unsigned int
unsigned reverse32bits(unsigned x) {
   static unsigned char table[256] = {
   0x00, 0x80, 0x40, 0xC0, 0x20, 0xA0, 0x60, 0xE0, 0x10, 0x90, 0x50, 0xD0, 0x30, 0xB0, 0x70, 0xF0,
   0x08, 0x88, 0x48, 0xC8, 0x28, 0xA8, 0x68, 0xE8, 0x18, 0x98, 0x58, 0xD8, 0x38, 0xB8, 0x78, 0xF8,
   0x04, 0x84, 0x44, 0xC4, 0x24, 0xA4, 0x64, 0xE4, 0x14, 0x94, 0x54, 0xD4, 0x34, 0xB4, 0x74, 0xF4,
   0x0C, 0x8C, 0x4C, 0xCC, 0x2C, 0xAC, 0x6C, 0xEC, 0x1C, 0x9C, 0x5C, 0xDC, 0x3C, 0xBC, 0x7C, 0xFC,
   0x02, 0x82, 0x42, 0xC2, 0x22, 0xA2, 0x62, 0xE2, 0x12, 0x92, 0x52, 0xD2, 0x32, 0xB2, 0x72, 0xF2,
   0x0A, 0x8A, 0x4A, 0xCA, 0x2A, 0xAA, 0x6A, 0xEA, 0x1A, 0x9A, 0x5A, 0xDA, 0x3A, 0xBA, 0x7A, 0xFA,
   0x06, 0x86, 0x46, 0xC6, 0x26, 0xA6, 0x66, 0xE6, 0x16, 0x96, 0x56, 0xD6, 0x36, 0xB6, 0x76, 0xF6,
   0x0E, 0x8E, 0x4E, 0xCE, 0x2E, 0xAE, 0x6E, 0xEE, 0x1E, 0x9E, 0x5E, 0xDE, 0x3E, 0xBE, 0x7E, 0xFE,
   0x01, 0x81, 0x41, 0xC1, 0x21, 0xA1, 0x61, 0xE1, 0x11, 0x91, 0x51, 0xD1, 0x31, 0xB1, 0x71, 0xF1,
   0x09, 0x89, 0x49, 0xC9, 0x29, 0xA9, 0x69, 0xE9, 0x19, 0x99, 0x59, 0xD9, 0x39, 0xB9, 0x79, 0xF9,
   0x05, 0x85, 0x45, 0xC5, 0x25, 0xA5, 0x65, 0xE5, 0x15, 0x95, 0x55, 0xD5, 0x35, 0xB5, 0x75, 0xF5,
   0x0D, 0x8D, 0x4D, 0xCD, 0x2D, 0xAD, 0x6D, 0xED, 0x1D, 0x9D, 0x5D, 0xDD, 0x3D, 0xBD, 0x7D, 0xFD,
   0x03, 0x83, 0x43, 0xC3, 0x23, 0xA3, 0x63, 0xE3, 0x13, 0x93, 0x53, 0xD3, 0x33, 0xB3, 0x73, 0xF3,
   0x0B, 0x8B, 0x4B, 0xCB, 0x2B, 0xAB, 0x6B, 0xEB, 0x1B, 0x9B, 0x5B, 0xDB, 0x3B, 0xBB, 0x7B, 0xFB,
   0x07, 0x87, 0x47, 0xC7, 0x27, 0xA7, 0x67, 0xE7, 0x17, 0x97, 0x57, 0xD7, 0x37, 0xB7, 0x77, 0xF7,
   0x0F, 0x8F, 0x4F, 0xCF, 0x2F, 0xAF, 0x6F, 0xEF, 0x1F, 0x9F, 0x5F, 0xDF, 0x3F, 0xBF, 0x7F, 0xFF};
   int i;
   unsigned r;

   r = 0;
   for (i = 3; i >= 0; i--) {
      r = (r << 8) + table[x & 0xFF];
      x = x >> 8;
   }
   return r;
}

void print_list(unsigned long long * head){
        
        struct NodeType * curr=(struct NodeType *)get_pointer((unsigned long long)*head);
        while (curr){
            unsigned int normal_key = curr->key/2;
            normal_key = normal_key*2;
            
            normal_key = reverse32bits(normal_key);
            
            if (curr->key%2==1)printf("%d \n",normal_key);
            else printf("*%d \n",normal_key);
            curr=(struct NodeType *)get_pointer(curr->marked_next);
        }
}


//produce keys according to split ordering
unsigned int so_regularkey(unsigned int key){
    return reverse32bits(key|0x80000000);
}

unsigned int so_dummykey(unsigned int key){
    return reverse32bits(key);
}

//get the parent of a bucket by just unseting the MSB
int get_parent(int bucket){
     int msb = 1<< ((sizeof(int)*8)-__builtin_clz(bucket)-1);
     int result = bucket & ~msb;
     return result;
}    

int count0;
int count1;
int count2;
int count3;
int size0;
int size1;
int size2;
int size3;

int MAX_LOAD = 3;


unsigned long long uninitialized0;//pointer value that stands for invalid bucket
unsigned long long uninitialized1;//pointer value that stands for invalid bucket
unsigned long long uninitialized2;//pointer value that stands for invalid bucket
unsigned long long uninitialized3;//pointer value that stands for invalid bucket

//remember to set all this as shared
unsigned long long * T0;
unsigned long long * T1;
unsigned long long * T2;
unsigned long long * T3;

void initialize_bucket(unsigned long long *T ,int bucket,struct priv_pointers *priv,unsigned long long *uninitialized ){
    
    int parent = get_parent(bucket);

    if (T[parent]==*uninitialized) initialize_bucket(T,parent,priv,uninitialized);
    
    struct NodeType * dummy = (struct  NodeType *)malloc(sizeof(struct NodeType));
    dummy->key=so_dummykey(bucket);
    if(!list_insert(&(T[parent]),dummy,priv)){
        free(dummy);
        dummy=(struct Node_type *)get_pointer(priv->curr);
    }
    T[bucket]=(unsigned long long )dummy;
}


int insert(unsigned long long *T,int *size, int *count ,unsigned int key,struct priv_pointers *priv,unsigned long long * uninitialized){
    
    struct NodeType * node=(struct NodeType *)malloc(sizeof(struct NodeType));
    node->key = so_regularkey(key);
    int bucket = key % *size;

    if(T[bucket]==*uninitialized) initialize_bucket(T,bucket,priv,uninitialized);
    
    if(!list_insert(&(T[bucket]),node,priv)){
        free(node);
        return 0;
    }

    int csize=*size;
    int new = __sync_fetch_and_add(count,1);

    //printf("count is %d\n",new);
    if ((new/csize) >MAX_LOAD){
        //printf("count is %d and size is %d\n",count,size);
        int res= __sync_bool_compare_and_swap(size,csize,2*csize);
    }
    return 1;
}

int _delete(unsigned long long *T ,int *size, int *count,unsigned int key,struct priv_pointers *priv,unsigned long long *uninitialized){
    
    printf("@ delete\n");
    int bucket = key % *size;
    if (T[bucket]==*uninitialized) initialize_bucket(T,bucket,priv,uninitialized);

    if(!list_delete(&(T[bucket]),so_regularkey(key),priv))
        return 0;

    int res=__sync_fetch_and_sub(count,1);
    return 1;
}


int find(unsigned long long *T,int *size, int * count,unsigned int key,struct priv_pointers *priv,unsigned long long *uninitialized){
    
    int bucket = key %*size;
    if (T[bucket]==*uninitialized) initialize_bucket(T,bucket,priv,uninitialized);

    unsigned long long * temp=&T[bucket];
    return list_find(&temp,so_regularkey(key),priv);
        
}

struct HashSet{
    int dummy;
};//Dummy struct 
void _initialize(struct HashSet * H,int _size){

    
    unsigned int dummy_variable0 = 5; 
    unsigned int dummy_variable1 = 5; 
    unsigned int dummy_variable2 = 5; 
    unsigned int dummy_variable3 = 5; 
    uninitialized0 =(unsigned long long) &dummy_variable0;//any bucket that points to this address is considered uninitialized
    uninitialized1 =(unsigned long long) &dummy_variable1;//any bucket that points to this address is considered uninitialized
    uninitialized2 =(unsigned long long) &dummy_variable2;//any bucket that points to this address is considered uninitialized
    uninitialized3 =(unsigned long long) &dummy_variable3;//any bucket that points to this address is considered uninitialized
    //:TODO is the above safe?

    
    priv0.prev=0;
    priv0.curr=0;
    priv0.next=0;
    priv1.prev=0;
    priv1.curr=0;
    priv1.next=0;
    priv2.prev=0;
    priv2.curr=0;
    priv2.next=0;
    priv3.prev=0;
    priv3.curr=0;
    priv3.next=0;

    T0 = (unsigned long long *) malloc(sizeof(unsigned long long)*_size);
    T1 = (unsigned long long *) malloc(sizeof(unsigned long long)*_size);
    T2 = (unsigned long long *) malloc(sizeof(unsigned long long)*_size);
    T3 = (unsigned long long *) malloc(sizeof(unsigned long long)*_size);
    int i;
    size0=_size;//TODO: check this
    size1=_size;
    size2=_size;
    size3=_size;
    for(i=0;i<size0;i++){
        T0[i]= uninitialized0;
        T1[i]= uninitialized1;
        T2[i]= uninitialized2;
        T3[i]= uninitialized3;
    }
    unsigned long long head0=0;
    unsigned long long head1=0;
    unsigned long long head2=0;
    unsigned long long head3=0;
    
    int res;
    struct NodeType * node0= (struct NodeType *) malloc(sizeof(struct NodeType));
    struct NodeType * node1= (struct NodeType *) malloc(sizeof(struct NodeType));
    struct NodeType * node2= (struct NodeType *) malloc(sizeof(struct NodeType));
    struct NodeType * node3= (struct NodeType *) malloc(sizeof(struct NodeType));
    node0->key=0;
    node2->key=0;
    node2->key=0;
    node3->key=0;
    res=list_insert(&head0,node0,&priv0);
    res=list_insert(&head1,node1,&priv1);
    res=list_insert(&head2,node2,&priv2);
    res=list_insert(&head3,node3,&priv3);
    T0[0]=head0;
    T1[0]=head1;
    T2[0]=head2;
    T3[0]=head3;

}
//High level implementation

struct pub_record{
    int pending;
    char pad1[(64-sizeof(int ))/sizeof(char)];
    int serving;
    char pad2[(64-sizeof(int ))/sizeof(char)];
    int op;
    char pad3[(64-sizeof(int ))/sizeof(char)];
    int value;
    char pad4[(64-sizeof(int ))/sizeof(char)];
    int response;
    char pad5[(64-sizeof(int ))/sizeof(char)];
};

struct pub_record *record_array0;
struct pub_record *record_array1;
struct pub_record *record_array2;
struct pub_record *record_array3;

int VAL_RANGE= 100000;
int NODE0_LIMIT= 25000; //VAL_RANGE/4
int NODE1_LIMIT= 50000;
int NODE2_LIMIT= 75000;

//operations code
int INSERT=1;
int LOOKUP=2;
int ERASE=3;


int wait_for_response(int tid,int k){
    
    struct pub_record * record_array;
    if (k<NODE0_LIMIT) {
        record_array=record_array0;
    }
    else if (k<NODE1_LIMIT){
        record_array=record_array1;
    }
    else if (k<NODE2_LIMIT){
        record_array=record_array2;
    }
    else {
        record_array=record_array3;
    }
    long long int count=0;
    //while((count<1000000)&&(record_array[tid].pending==1)) count++;//FIXME
    while((record_array[tid].pending==1)) count++;//FIXME
    while(record_array[tid].serving==1)count++;
    //printf("%lld\n",count);
    return record_array[tid].response;
}

void find_where_i_belong(struct priv_pointers **priv,unsigned long long **T,int **size,int **count,struct pub_record **record_array,unsigned long long ** uninitialized){

    int tid =omp_get_thread_num();
    if(tid<16){
        *T=T0;
        *priv=&priv0;
        *size=&size0;
        *count=&count0;
        *record_array=record_array0;
        *uninitialized = &uninitialized0;
    }
    else if(tid<32){
        *T=T1;
        *priv=&priv1;
        *size=&size1;
        *count=&count1;
        *record_array=record_array1;
        *uninitialized = &uninitialized1;
    }
    else if(tid<48){
        *T=T2;
        *priv=&priv2;
        *size=&size2;
        *count=&count2;
        *record_array=record_array2;
        *uninitialized = &uninitialized2;
    }
    else{
        *T=T3;
        *priv=&priv3;
        *size=&size3;
        *count=&count3;
        *record_array=record_array3;
        *uninitialized = &uninitialized3;
    }
}
void push_request(int k,int tid,int op){
    if (k<NODE0_LIMIT) {
        record_array0[tid].op=op;
        record_array0[tid].value=k;
        record_array0[tid].pending=1;
    }
    else if (k<NODE1_LIMIT){
        record_array1[tid].op=op;
        record_array1[tid].value=k;
        record_array1[tid].pending=1;
    }
    else if (k<NODE2_LIMIT){
        record_array2[tid].op=op;
        record_array2[tid].value=k;
        record_array2[tid].pending=1;
    }
    else {
        record_array3[tid].op=op;
        record_array3[tid].value=k;
        record_array3[tid].pending=1;
    }
}

int PUB_REC_SIZE =64;

void try_access(unsigned long long * T,int *size,int *count,int op,int value, struct pub_record * record_array,struct priv_pointers * priv,unsigned long long *uninitialized){
    
    int tid = omp_get_thread_num();

    int i;
    int res;
    for ( i=0; i<PUB_REC_SIZE; i++){
        if(record_array[i].pending ==1){
            if( record_array[i].serving ==0){
                if(!__sync_lock_test_and_set(&(record_array[i].serving),1)){                if(record_array[i].pending==1){
                    if (record_array[i].op==INSERT){
                        res = insert(T,size,count,record_array[i].value,priv,uninitialized);
                    }
                    else if (record_array[i].op==LOOKUP){
                        res = find(T,size,count,record_array[i].value,priv,uninitialized);
                    }
                    else if(record_array[i].op==ERASE){
                        printf("%d \n",record_array[i].op);
                        res = _delete(T,size,count,record_array[i].value,priv,uninitialized);
                    }
                    else{
                        printf("hey!! %d \n",record_array[i].op);
                    }
                    record_array[i].response =res;
                    record_array[i].pending=0;
                    record_array[i].serving=0;
                 }
                else record_array[i].serving=0;
                }
            }
        }
    }
}


int Insert(struct HashSet *H,int k){
    
    unsigned long long *T;
    int *size;
    int *count;
    struct pub_record * record_array;
    int tid =omp_get_thread_num();
    struct priv_pointers * priv;
    unsigned long long * uninitialized;

    find_where_i_belong(&priv,&T,&size,&count,&record_array,&uninitialized);
    push_request(k,tid,INSERT);
    try_access(T,size,count,INSERT,k,record_array,priv,uninitialized);
    int res =  wait_for_response(tid,k);
    return res;
}

int Lookup(struct HashSet *H, int k){
    unsigned long long *T;
    int *size;
    int *count;
    struct pub_record * record_array;
    int tid =omp_get_thread_num();
    struct priv_pointers * priv;
    unsigned long long * uninitialized;


    find_where_i_belong(&priv,&T,&size,&count,&record_array,&uninitialized);
    push_request(k,tid,LOOKUP);
    /*T=T0;
    size=&size0;
    count=&count0;
    record_array=record_array0;
    priv=&priv0;
    */
    try_access(T,size,count,LOOKUP,k,record_array,priv,uninitialized);
    int res =  wait_for_response(tid,k);
    return res;
}

int Erase(struct HashSet *H,int k){
    unsigned long long *T;
    int *size;
    int *count;
    struct pub_record * record_array;
    int tid =omp_get_thread_num();
    struct priv_pointers * priv;
    unsigned long long * uninitialized;

    find_where_i_belong(&priv,&T,&size,&count,&record_array,&uninitialized);
    push_request(k,tid,ERASE);
    try_access(T,size,count,ERASE,k,record_array,priv,uninitialized);
    int res =  wait_for_response(tid,k);
    return res;
}


int total_threads_finished=0;

void help_other_threads(){
    
    unsigned long long *T;
    int *size;
    int *count;
    struct pub_record * record_array;
    int tid =omp_get_thread_num();
    struct priv_pointers * priv;
    unsigned long long * uninitialized;
    int temp = __sync_fetch_and_add(&total_threads_finished,1);
    if (temp == (64 -1)) exit(0);
    
    find_where_i_belong(&priv,&T,&size,&count,&record_array,&uninitialized);
    while(1){
        try_access(T,size,count,ERASE,0,record_array,priv,uninitialized);
    }
}

void initialize(struct HashSet *H,int size){
    
    record_array0 = (struct pub_record *) malloc(sizeof(struct pub_record)*PUB_REC_SIZE);
    record_array1 = (struct pub_record *) malloc(sizeof(struct pub_record)*PUB_REC_SIZE);
    record_array2 = (struct pub_record *) malloc(sizeof(struct pub_record)*PUB_REC_SIZE);
    record_array3 = (struct pub_record *) malloc(sizeof(struct pub_record)*PUB_REC_SIZE);
    int i;
    for (i=0; i<PUB_REC_SIZE ;i++){
        record_array0[i].pending =0;
        record_array0[i].op =0;
        record_array0[i].serving =0;
        record_array1[i].pending =0;
        record_array1[i].op =0;
        record_array1[i].serving =0;
        record_array2[i].pending =0;
        record_array2[i].op =0;
        record_array2[i].serving =0;
        record_array3[i].pending =0;
        record_array2[i].op =0;
        record_array3[i].serving =0;
    }
    _initialize(H,size);
}
        



#endif /* __HASH_OVER_NUMA_H*/
