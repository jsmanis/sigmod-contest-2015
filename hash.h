int hashFunc (element* , int);

extendibleHash* createHash ( void );

Hash* createBucket( int);

int insertRecord ( extendibleHash *, element* ,RangeArray *);

int redistributeEntries( Hash **, Hash *, Hash *, element **, int , element* , RangeArray *);

void copyIndex( Hash **, int);

void printIndex( extendibleHash * ,uint32_t);

void tryCombine( extendibleHash *, int);

int searchKey( Hash **, element* , int);

void collapseIndex( Hash **, Hash **, int );

void mergeBuckets( Hash *, Hash * );

void printBucketContents( Hash * );

void Fixpointers(Hash **, int, int , Hash *, Hash *, int);

int insertElem( Hash* , element* , RangeArray *, int);

RangeArray * GetHashRecord( extendibleHash *, element*);

int destroyHash( extendibleHash * );

int GetHashRecords( extendibleHash *, element *, uint64_t , uint64_t  ,uint32_t  , Column_t *);

int DeleteHashRecord (  extendibleHash *, element * );
