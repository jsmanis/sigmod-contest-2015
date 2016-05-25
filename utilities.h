#ifndef _UTILITIES_H_
#define _UTILITIES_H_

#define PK_INSERT 0
#define PK_DELETE 1


void copyElement (element* , element* );

Hash* buddyBucketTest( extendibleHash * , int );

int colapseTest( extendibleHash * );

int appendInList( ListRange *, uint64_t *);

void freeList( ListRange * );

void deletefromList( ListRange *, ListNode_t *, ListNode_t * );

#endif
