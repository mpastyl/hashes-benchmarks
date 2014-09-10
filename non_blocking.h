#ifndef __NON_BLOCKING_H
#define __NON_BLOCKING_H
unsigned long long get_count(unsigned long long a){

    unsigned long long b = a >>48;
    return b;
}

unsigned long long get_pointer(unsigned long long a){
    unsigned long long b = a << 16;
    b= b >>16;
    return b;
}

unsigned long long set_count(unsigned long long  a, unsigned long long count){
    unsigned long long count_temp =  count << 48;
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

unsigned long long set_both(unsigned long long a, unsigned long long ptr, unsigned long long count){
    a=set_pointer(a,ptr);
    a=set_count(a,count);
    return a;
}


struct HashSet{
    unsigned long long * bounds;
    struct BucketT * buckets;
    int size;
};

struct BucketT{
    unsigned long long vs;//version,state
    int key;
};

// states:
unsigned long long  VISIBLE=1;
unsigned long long  INSERTING=2;
unsigned long long  MEMBER=3;
unsigned long long  EMPTY=4;
unsigned long long  COLLIDED=5;
unsigned long long  BUSY=6;

int Hash(struct HashSet *H,int key){
    return (key%H->size);
}
struct BucketT *Bucket(struct HashSet *H,int h,int index){
    return (&(H->buckets[(h+index*(index+1)/2)%H->size]));
}

int DoesBucketContainCollision(struct HashSet *H,int h, int index){
    
    unsigned long long old_state = (Bucket(H,h,index))->vs;
    unsigned long long version1 = get_pointer(old_state);
    unsigned long long state1 = get_count(old_state);

    unsigned long long version2;
    unsigned long long state2;
    if((state1 == VISIBLE) || (state1 ==INSERTING) || (state1 == MEMBER)){
            if (Hash(H,Bucket(H,h,index)->key)==h){
                old_state = (Bucket(H,h,index))->vs;
                version2 = get_pointer(old_state);
                state2 = get_count(old_state);
                if ((state2==VISIBLE)||(state2==INSERTING)||(state2==MEMBER)){
                    if(version1==version2)
                        return 1;
                }
             }
     }
     return 0;
}

void InitProbeBound(struct HashSet * H,int h){
    H->bounds[h]=set_both(H->bounds[h],0,0);
}
void Init(struct HashSet *H){
    
    int i;
    for(i=0;i<H->size;i++){
        InitProbeBound(H,i);
        H->buckets[i].vs =set_both(H->buckets[i].vs,0,EMPTY);
    }
}

int GetProbeBound(struct HashSet *H,int h){
    unsigned long long bound = get_pointer(H->bounds[h]);
    return bound;
}

void ConditionallyRaiseBound(struct HashSet *H,int h,int index){
    
    unsigned long long new_bound;
    unsigned long long old_b_scan;
    unsigned long long new_to_set;
    unsigned long long old_bounds;
    unsigned long long scanning;
    unsigned long long * temp;
    do{
        temp=&(H->bounds[h]);
        old_b_scan = H->bounds[h];
        old_bounds = get_pointer(old_b_scan);
        scanning = get_count(old_b_scan);
        if (old_bounds>index) new_bound=old_bounds;
        else new_bound = index;
        new_to_set = set_both(new_to_set,new_bound,0);
    }
    while(!__sync_bool_compare_and_swap(temp,old_b_scan,new_to_set));
}

void ConditionallyLowerBound(struct HashSet *H,int h, int index){
    
    unsigned long long new_to_set;
    unsigned long long old_b_scan = H->bounds[h];
    unsigned long long bounds = get_pointer(old_b_scan);
    unsigned long long scanning = get_count(old_b_scan);
    unsigned long long expected;
    unsigned long long new_to_set_2;
    if(scanning==1){
        new_to_set = set_both(new_to_set,bounds,0);
        __sync_bool_compare_and_swap(&(H->bounds[h]),old_b_scan,new_to_set);
    }
    if(index>0){
        new_to_set = set_both(new_to_set,index,1);
        expected = set_both(expected,index,0);
        while (__sync_bool_compare_and_swap(&(H->bounds[h]),expected,new_to_set));{
            int i = index -1;
            while ((i>0)&&(!DoesBucketContainCollision(H,h,i))){
                i--;
            }
           new_to_set_2 = set_both(new_to_set_2,i,0);
            __sync_bool_compare_and_swap(&(H->bounds[h]),new_to_set,new_to_set_2);
        }
     }
}

int Lookup(struct HashSet *H,int k){

    int h = Hash(H,k);
    int max =GetProbeBound(H,h);
    int index;
    unsigned long long old_state;
    unsigned long long version;
    unsigned long long state;


    for(index=0;index<=max;index++){
        old_state = Bucket(H,h,index)->vs;
        version = get_pointer(old_state);
        state =get_count(old_state);
        if((state==MEMBER)&&(Bucket(H,h,index)->key==k)){
            if((Bucket(H,h,index)->vs)==old_state)
                return 1;
        }
    }
    return 0;
}
 

int Assist(struct HashSet *H,int k,int  h,int i, int ver_i){
    
    int max=GetProbeBound(H,h);
    int j;
    unsigned long long new_to_set;
    unsigned long long old_state;
    unsigned long long version_j;
    unsigned long long state_j;
    unsigned long long expected;

    for(j=0;j<=max;j++){
        if (i!=j){
            old_state = Bucket(H,h,j)->vs;
            version_j = get_pointer(old_state);
            state_j = get_count(old_state);
            if ((state_j==INSERTING)&&(Bucket(H,h,j)->key==k)){
                if (j<i){
                    new_to_set = set_both(new_to_set,version_j,INSERTING);
                    if (Bucket(H,h,j)->vs == new_to_set){
                        expected =set_both(expected,ver_i,INSERTING);
                        new_to_set = set_both(new_to_set,ver_i,COLLIDED);
                        __sync_bool_compare_and_swap(&(Bucket(H,h,i)->vs),expected,new_to_set);
                        return Assist(H,k,h,j,version_j);
                    }
                }
                else{
                    new_to_set = set_both(new_to_set,ver_i,INSERTING);
                    if (Bucket(H,h,i)->vs == new_to_set){
                        expected = set_both(expected,version_j,INSERTING);
                        new_to_set = set_both(new_to_set,version_j,COLLIDED);
                        __sync_bool_compare_and_swap(&(Bucket(H,h,j)->vs),expected, new_to_set);
                    }
                }
            }
           
            old_state = Bucket(H,h,j)->vs;
            version_j = get_pointer(old_state);
            state_j = get_count(old_state);
            if ((state_j == MEMBER)&&(Bucket(H,h,j)->key==k)){
                new_to_set = set_both(new_to_set,version_j,MEMBER);
                if (Bucket(H,h,j)->vs == new_to_set){
                    new_to_set = set_both(new_to_set,ver_i,COLLIDED);
                    expected = set_both(expected,ver_i,INSERTING);
                    __sync_bool_compare_and_swap(&(Bucket(H,h,i)->vs),expected,new_to_set);
                    return 0;
                }
            }
        }
    }
    expected =  set_both(expected,ver_i,INSERTING);
    new_to_set = set_both(expected,ver_i,MEMBER);
    __sync_bool_compare_and_swap(&(Bucket(H,h,i)->vs),expected,new_to_set);
    return 1;
}
                        

int Insert(struct HashSet *H,int k){
    
    int h = Hash(H,k);
    int i=-1;
    int temp;
    unsigned long long old_state; 
    unsigned long long version;
    unsigned long long expected;
    unsigned long long new_to_set;
    do{
        if (++i>=H->size)
            printf("Table full!\n");//TODO: i may be <H->size but Bucket(H,h,i) goes beyond H->size
        old_state = Bucket(H,h,i)->vs;
        version = get_pointer(old_state);
        expected =  set_both(expected,version,EMPTY);
        new_to_set =  set_both(new_to_set,version,BUSY);
        temp = __sync_bool_compare_and_swap(&(Bucket(H,h,i)->vs),expected,new_to_set);
    }
    while(!temp);
    Bucket(H,h,i)->key = k;
    while (1){
        new_to_set = set_both(new_to_set,version,VISIBLE);
        Bucket(H,h,i)->vs = new_to_set;
        ConditionallyRaiseBound(H,h,i);
        new_to_set = set_both(new_to_set,version,INSERTING);
        Bucket(H,h,i)->vs = new_to_set;
        int r=Assist(H,k,h,i,version);
        new_to_set = set_both(new_to_set,version,COLLIDED);
        if (Bucket(H,h,i)->vs!=new_to_set)
            return 1;
        if (!r){
            ConditionallyLowerBound(H,h,i);
            new_to_set = set_both(new_to_set,version+1,EMPTY);
            Bucket(H,h,i)->vs = new_to_set;
            return 0;
        }
        version++;
    }
    
}
    
    
int Erase(struct HashSet *H, int k){
    
    int h = Hash(H,k);
    int max = GetProbeBound(H,h);
    int i;
    unsigned long long old_state,version,state,expected,new_value;
    for(i=0;i<=max;i++){
        old_state = Bucket(H,h,i)->vs;
        version = get_pointer(old_state);
        state = get_count(old_state);
        if ((state==MEMBER)&&(Bucket(H,h,i)->key=k)){
            
            expected  = set_both(expected,version,MEMBER);
            new_value = set_both(new_value,version,BUSY);
            if( __sync_bool_compare_and_swap(&(Bucket(H,h,i)->vs),expected,new_value)){
                ConditionallyLowerBound(H,h,i);
                new_value = set_both(new_value,version+1,EMPTY);
                Bucket(H,h,i)->vs=new_value;
                return 1;
             }
         }
     }
     return 0;
}
void initialize(struct HashSet *H,int size){
    
    H->size=size;
    H->bounds = (unsigned long long *) malloc(sizeof(unsigned long long )*H->size);
    H->buckets = (struct BucketT  *) malloc(sizeof(struct BucketT ) * H->size);
    Init(H);
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

#endif /*__NON_BLOCKING_H*/
