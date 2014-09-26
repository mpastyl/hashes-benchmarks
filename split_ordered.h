#ifndef __SPLIT_ORDERED__H
#define __SPLIT_ORDERED__H

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
unsigned long long   * prev;
unsigned long long   curr;
unsigned long long   next;
#pragma omp threadprivate(prev,curr,next)

unsigned long long * Head=0;

int list_insert(unsigned long long * head, struct NodeType * node){
    
    int res;
    int temp=0;
    unsigned int key=node->key;
    
    while (1){
        if (list_find(&head,key)) return 0;
        node->marked_next = set_both(node->marked_next,get_pointer(curr),0);
        
        unsigned long long compare_value = set_both(compare_value,get_pointer(curr),0);
        unsigned long long new_value = set_both(new_value,(unsigned long long ) node,0);
        
        //if((*prev)!=compare_value){ if(temp==0)printf("wtf!! %lld %lld\n",*prev,compare_value);}
        temp++;
        res =__sync_bool_compare_and_swap(prev,compare_value,new_value);
        if (res){
            //Head=head;
            return 1;
        }
     }
}


int list_delete(unsigned long long *head ,unsigned int key){
    
    while (1){
        if (!list_find(&head,key))  return 0;
        unsigned long long compare_value = set_both(compare_value,get_pointer(next),0);
        unsigned long long new_value = set_both(new_value,get_pointer(next),1);

        if(!__sync_bool_compare_and_swap(&(((struct NodeType *)get_pointer(curr))->marked_next),compare_value,new_value)) 
            continue;


        compare_value = set_both(compare_value,get_pointer(curr),0);
        new_value = set_both(new_value,get_pointer(next),0);

        if(__sync_bool_compare_and_swap(prev,compare_value,new_value))
            free((struct NodeType *)get_pointer(curr));

        else list_find(&head,key);
        //Head=head;//TODO: thats not very safe
        return 1;

    }
}


int list_find(unsigned long long ** head,unsigned int key){
    
    try_again:
        prev=(unsigned long long *)*head;
        curr=set_both(curr,get_pointer(*prev),get_count(*prev));
        //printf("#t %d &curr= %p\n",omp_get_thread_num(),&curr);
        while (1){

            if(get_pointer(curr)==0) return 0;
            
            unsigned long long pointer=get_pointer(((struct NodeType * )get_pointer(curr))->marked_next);
            unsigned long long mark_bit = get_count(((struct NodeType * )get_pointer(curr))->marked_next);
            
            next = set_both(next,pointer,mark_bit);       
            unsigned int ckey= ((struct NodeType *)get_pointer(curr))->key;
            unsigned long long check=set_both(check,curr,0);
            if ((*prev) !=check) goto  try_again;

            if (get_count(next)==0){
                if (ckey>=key)
                    return (ckey==key);
                prev = &(((struct NodeType *)get_pointer(curr))->marked_next);   
            }

            else{
                
                unsigned long long compare_value = set_both(compare_value,curr,0);
                unsigned long long new_value = set_both(new_value,next,0);

                if (__sync_bool_compare_and_swap(prev,compare_value,new_value)){
                    free((struct NodeType *)get_pointer(curr));
                    //printf("Hey!\n");
                    }

                else goto try_again;
            }

            curr=set_both(curr,next,get_count(next));
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

int count;
int size;

int MAX_LOAD = 4000;


unsigned long long uninitialized;//pointer value that stands for invalid bucket

//remember to set all this as shared
unsigned long long * T;

void initialize_bucket(int bucket){
    
    int parent = get_parent(bucket);

    if (T[parent]==uninitialized) initialize_bucket(parent);
    
    struct NodeType * dummy = (struct  NodeType *)malloc(sizeof(struct NodeType));
    dummy->key=so_dummykey(bucket);
    if(!list_insert(&(T[parent]),dummy)){
        free(dummy);
        dummy=(struct Node_type *)get_pointer(curr);
    }
    T[bucket]=(unsigned long long )dummy;
}


int insert(unsigned int key){
    
    struct NodeType * node=(struct NodeType *)malloc(sizeof(struct NodeType));
    node->key = so_regularkey(key);
    int bucket = key % size;

    if(T[bucket]==uninitialized) initialize_bucket(bucket);
    
    if(!list_insert(&(T[bucket]),node)){
        free(node);
        return 0;
    }

    int csize=size;
    int new = __sync_fetch_and_add(&count,1);

    //printf("count is %d\n",new);
    if ((new/csize) >MAX_LOAD){
        //printf("count is %d and size is %d\n",count,size);
        printf("resizing\n");
        int res= __sync_bool_compare_and_swap(&size,csize,2*csize);
    }
    return 1;
}

int _delete(unsigned int key){
    
    int bucket = key % size;
    if (T[bucket]==uninitialized) initialize_bucket(bucket);

    if(!list_delete(&(T[bucket]),so_regularkey(key)))
        return 0;

    int res=__sync_fetch_and_sub(&count,1);
    return 1;
}


int find(unsigned int key){
    
    int bucket = key %size;
    if (T[bucket]==uninitialized) initialize_bucket(bucket);

    unsigned long long * temp=&T[bucket];
    return list_find(&temp,so_regularkey(key));
        
}


/* Arrange the N elements of ARRAY in random order.
   Only effective if N is much smaller than RAND_MAX;
   if this may not be the case, use a better random
   number generator. */
void shuffle(int *array, size_t n)
{
    if (n > 1) 
    {
        size_t i;
        for (i = 0; i < n - 1; i++) 
        {
          size_t j = i + rand() / (RAND_MAX / (n - i) + 1);
          int t = array[j];
          array[j] = array[i];
          array[i] = t;
        }
    }
}
struct HashSet{
    int dummy;
};//Dummy struct 
void initialize(struct HashSet * H,int _size){

    
    unsigned int dummy_variable = 5; 
    uninitialized =(unsigned long long) &dummy_variable;//any bucket that points to this address is considered uninitialized
    //:TODO is the above safe?

    
    T = (unsigned long long *) malloc(sizeof(unsigned long long)*_size);
    int i;
    for(i=0;i<size;i++) T[i]= uninitialized;
    unsigned long long head=0;
    size=_size;//TODO: check this
    
    int res;
    struct NodeType * node= (struct NodeType *) malloc(sizeof(struct NodeType));
    node->key=0;
    res=list_insert(&head,node);
    T[0]=head;
}

int Insert(struct HashSet *H,int k){
    return insert(k);
}

int Lookup(struct HashSet *H, int k){
    return find(k);
}

int Erase(struct HashSet *H,int k){
    return _delete(k);
}
#endif /*__SPLIT_ORDERED__H*/
