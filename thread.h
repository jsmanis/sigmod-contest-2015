#ifndef _THREAD_H_
#define _THREAD_H_



ThreadList* createThreadList();
void pushThreadListNode(ThreadList *list, validationListNode_t *val);
void printThreadList(ThreadList* list);
void destroyThreadList(ThreadList **list);
void* routine(void* arg);
ValidationThread** initializeThreadArgs(int k);
validationListNode_t* prepareValidationsListTest();
ValidationThread** distributeValidations(validationListNode_t* start, uint64_t endId, int numOfThreads, validationListNode_t**);
void multiThreadedFlush(int k, validationListNode_t* start, uint64_t to, validationListNode_t**);
#endif
