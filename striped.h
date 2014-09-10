#ifndef __STRIPED_H
#define __STRIPED_H

// node of a list (bucket)
struct node_t{
    int value;
    int hash_code;
    struct node_t * next;
};


struct HashSet{
    //int length;
    struct node_t ** table;
    int capacity;
    int setSize;
    int * locks;
    int locks_length;
};

void lock_set (struct HashSet * H, int hash_code){

    
    int indx=hash_code % H->locks_length;
    while (1){
        if (!H->locks[indx]){
            if(!__sync_lock_test_and_set(&(H->locks[indx]),1)) break;
        }
    }
}

void unlock_set(struct HashSet * H, int hash_code){

    int indx=hash_code % H->locks_length;
    H->locks[indx] = 0;
}





//search value in bucket;
int list_search(struct node_t * Head,int val){
    
    struct node_t * curr;
    
    curr=Head;
    while(curr){
        if(curr->value==val) return 1;
        curr=curr->next;
    }
    return 0;
}


//add value in bucket;
//NOTE: duplicate values are allowed...
void list_add(struct HashSet * H, int key,int val,int hash_code){
    
    struct node_t * curr;
    struct node_t * next;
    struct node_t * node=(struct node_t *)malloc(sizeof(struct node_t));
    /*node->value=val;
    node->next=NULL;
    curr=H->table[key];
    if(curr==NULL){
        H->table[key]=node;
        return ;
    }
    while(curr->next){
        curr=curr->next;
        next=curr->next;
    }
    curr->next=node;
    */
    node->value=val;
    node->hash_code=hash_code;
    if(H->table[key]==NULL) node->next=NULL;
    else node->next=H->table[key];
    H->table[key]=node;
}


// delete from bucket. The fist value equal to val will be deleted
int list_delete(struct HashSet *H,int key,int val){
    
    struct node_t * curr;
    struct node_t * next;
    struct node_t * prev;

    curr=H->table[key];
    prev=curr;
    if((curr!=NULL)&&(curr->value==val)) {
        H->table[key]=curr->next;
        free(curr);
        return 1;
    }
    while(curr){
        if( curr->value==val){
            prev->next=curr->next;
            free(curr);
            return 1;
        }
        prev=curr;
        curr=curr->next;
    }
    return 0;
}





void _initialize(struct HashSet * H, int capacity,int lock_length){
    
    int i;
    H->setSize=0;
    H->capacity=capacity;
    H->table = (struct node_t **)malloc(sizeof(struct node_t *)*capacity);
    for(i=0;i<capacity;i++){
        H->table[i]=NULL;
    }
    H->locks_length=lock_length;
    H->locks=(int *)malloc(sizeof(int) * lock_length);
    for(i=0;i<lock_length;i++) H->locks[i]=0;

}


int policy(struct HashSet *H){
    return ((H->setSize/H->capacity) >4);
}

void resize(struct HashSet *);

int contains(struct HashSet *H,int hash_code, int val){
    
    lock_set(H,hash_code);
    int bucket_index = hash_code % H->capacity;
    int res=list_search(H->table[bucket_index],val);
    unlock_set(H,hash_code);
    return res;
}

//reentrant ==1 means we must not lock( we are calling from resize so we have already locked the data structure)
void add(struct HashSet *H,int hash_code, int val, int reentrant){
    
    if(!reentrant) lock_set(H,hash_code);
    int bucket_index = hash_code % H->capacity;
    list_add(H,bucket_index,val,hash_code);
    //H->setSize++;
    __sync_fetch_and_add(&(H->setSize),1);
    if(!reentrant) unlock_set(H,hash_code);
    if (policy(H)) resize(H);
}

int _delete(struct HashSet *H,int hash_code, int val){
    
    lock_set(H,hash_code);
    int bucket_index =  hash_code % H->capacity;
    int res=list_delete(H,bucket_index,val);
    //H->setSize--;
    __sync_fetch_and_sub(&(H->setSize),1);
    unlock_set(H,hash_code);
    return res;
}


void resize(struct HashSet *H){
    
    printf("@resize!\n");
    int i;
    struct node_t * curr;
    int old_capacity = H->capacity;
    for(i=0;i<H->locks_length;i++) lock_set(H,i);
    if(old_capacity!=H->capacity) {
        for(i=0;i<H->locks_length;i++) unlock_set(H,i);
        return; //somebody beat us to it
    }
    int new_capacity =  old_capacity * 2;
    H->capacity =  new_capacity;

    struct node_t ** old_table = H->table;
    H->setSize=0;
    H->table = (struct node_t **)malloc(sizeof(struct node_t *)*new_capacity);
    for(i=0;i<new_capacity;i++){
        H->table[i]=NULL;
    }
    for(i=0;i<old_capacity;i++){
        
        curr=old_table[i];
        while(curr){
                int val = curr->value;
                int hash_code = curr->hash_code;
                //int bucket_index= hash_code % new_capacity;
                add(H,hash_code,val,1);
                curr=curr->next;
        }
    }
    free(old_table);
    for(i=0;i<H->locks_length;i++) unlock_set(H,i);
}

void print_set(struct HashSet * H){
    
    int i;
    for(i=0;i<H->capacity;i++){
        
        struct node_t * curr=H->table[i];
        while(curr){
            printf("(%d) ",curr->value);
            curr=curr->next;
        }
        printf("--\n");
    }
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

void initialize(struct HashSet *H,int size){
    //NOTE: change locks granuallity here    
    if (size>10000000) size = 10000000;
    _initialize(H,size,16);
}


int Insert(struct HashSet *H,int key){
    add(H,key,key,0);
    return 1;
}

int Lookup(struct HashSet *H,int key){
    return contains(H,key,key);
}

int Erase(struct HashSet *H,int key){
    return _delete(H,key,key);
}
#endif /*__STRIPED_H*/
