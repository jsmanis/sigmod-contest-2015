#include <stdlib.h>
#include <stdio.h>
#include "constants.h"
#include "utilities.h"


void copyElement (element *dest, element *source )
{
	dest->primaryKey = source->primaryKey;
	dest->numOfTrans = source->numOfTrans;
	dest->transArray = source->transArray;

	return;
}


/*	Checks if the bucket pointed by index[i] has a buddy bucket
	Returns :
	A pointer to the buddy bucket if it exists or NULL
*/

Hash* buddyBucketTest( extendibleHash *hash , int i)
{
	int iSwap, tableSize, powLocal;
	Hash **index;

	tableSize = hash->tableSize;
	index = hash->hashptr;
	powLocal = pow( 2, index[i]->localDepth-1);
	if ( i + powLocal >= tableSize )
	{
		iSwap = i-powLocal;
	}
	else
	{
		iSwap = i+powLocal;
	}
	if( index[i] != index[iSwap] && index[i]->localDepth == index[iSwap]->localDepth)	//	Buddy buckets must have the same local depth
	{
		return index[iSwap];
	}
	return NULL;
}


/*	Tests if the index can shrink to it's half size
	Returns :
	TRUE if it can
	FALSE otherwise
*/

int colapseTest( extendibleHash *hash )
{
	int tableSize = hash->tableSize, i;
	Hash **index;

	index = hash->hashptr;
	if ( tableSize > 1 )
	{
		for ( i = 0; i < tableSize/2; i++ )
		{
			if( index[i] != index[i+tableSize/2] )
			{
				return FALSE;
			}
		}
		return TRUE;
	}
	else
	{
		return FALSE;
	}
}

/*void updateRange( element *elem, RangeArray *tempTr )
{
	int transPos = elem->numOfTrans, i;

	
	if ( elem->transArray[transPos-1].transactionId == tempTr->transactionId )	//	The transaction id already exists
	{
		if ( elem->transArray[transPos-1].journalptr[1] == NULL )
		{
			elem->transArray[transPos-1].journalptr[1] = tempTr->journalptr[1];
		}
		return;
	}
	if ( transPos < RANGECAPACITY )
	{
		elem->transArray[transPos].transactionId = tempTr->transactionId;
		elem->transArray[transPos].journalptr[0] = tempTr->journalptr[0];
		elem->transArray[transPos].journalptr[1] = tempTr->journalptr[1];
		elem->numOfTrans++;
	}
	else	//	The range array is full
	{
		elem->transArray = realloc( elem->transArray, (transPos+RANGECAPACITY)*sizeof(RangeArray));
		elem->transArray[transPos].transactionId = tempTr->transactionId;
		elem->transArray[transPos].journalptr[0] = tempTr->journalptr[0];
		elem->transArray[transPos].journalptr[1] = tempTr->journalptr[1];
		elem->numOfTrans++;
	}
}*/

void updateRange( element *elem, RangeArray *tempTr )
{
	int transPos = elem->numOfTrans, i;

	
	if ( elem->transArray[elem->numOfTrans-1].transactionId == tempTr->transactionId )
		transPos = elem->numOfTrans-1;
	
	if ( transPos != elem->numOfTrans )	//	The transaction id already exists
	{
		elem->transArray[transPos].journalptr[1] = tempTr->journalptr[0];
	}
	else if ( transPos < RANGECAPACITY )
	{
		elem->transArray[transPos].transactionId = tempTr->transactionId;
		elem->transArray[transPos].journalptr[0] = tempTr->journalptr[0];
		elem->transArray[transPos].journalptr[1] = NULL;
		elem->numOfTrans++;
	}
	else	//	The range array is full
	{
		elem->transArray = realloc( elem->transArray, (transPos+RANGECAPACITY)*sizeof(RangeArray));
		elem->transArray[transPos].transactionId = tempTr->transactionId;
		elem->transArray[transPos].journalptr[0] = tempTr->journalptr[0];
		elem->transArray[transPos].journalptr[1] = NULL;
		elem->numOfTrans++;
	}
}

int appendInList( ListRange *List, uint64_t *journalptr)
{
	ListNode_t *newNode;
	ListRange *lastNode;

	newNode = malloc( sizeof(ListNode_t) );
	if ( newNode == NULL )
	{
		perror("malloc!");
		return -1;
	}

	newNode->journalptr = journalptr;
	newNode->next = NULL;

	if ( List->end == NULL )
	{
		List->start = newNode;
		List->end = newNode;
	}
	else
	{
		List->end->next = newNode;
		List->end = newNode;
	}
	return 0;
}


void freeList( ListRange *List )
{
	ListNode_t *tempNode, *toDel;

	if(List == NULL )
		return;
	tempNode = List->start;
	while ( tempNode != NULL )
	{
		toDel = tempNode;
		tempNode = tempNode->next;
		free(toDel);
	}
	free(List);
}

void deletefromList( ListRange *Head, ListNode_t *prev, ListNode_t *currentNode )
{
	if( currentNode == Head->start )
	{
		Head->start = currentNode->next;
		if ( currentNode == Head->end )
		{
			Head->end = NULL;
		}
		free( currentNode );
	}
	else if ( currentNode == Head->end )
	{
		Head->end = prev;
		prev->next = NULL;
		free( currentNode );
	}
	else
	{
		prev->next = currentNode->next;
		free( currentNode );
	}
}



void printJournalRecord( uint64_t *journalptr, uint32_t columnCount )
{
	
	int i;

	printf("{");
	for (i = 0; i < columnCount; i++)
	{
		printf("%ld\t", journalptr[i]);
	}
	printf("}\n");
}

void printList ( ListRange *List , uint32_t columnCount ) 
{
	ListNode_t *tempNode;

	if(List->start == NULL )
	{
		printf("Empty List \n");
		return;
	}
	tempNode = List->start;
	while ( tempNode != NULL )
	{
		printJournalRecord( tempNode->journalptr, columnCount );
		tempNode = tempNode->next;
	}
}

int recordCheck(uint64_t *journalRecord, uint32_t columnCount ,Column_t *columns)
{
	int i, flag = 1;

	for ( i = 0; i < columnCount; i++)
	{
		switch( columns[i].op )
		{
			case Equal:
				if( journalRecord[columns[i].column] != columns[i].value )
					flag = 0;
				break;
			case NotEqual:
				if( journalRecord[columns[i].column] == columns[i].value )
					flag = 0;
				break;
			case Less:
				if( journalRecord[columns[i].column] >= columns[i].value )
					flag = 0;
				break;
			case LessOrEqual:
				if( journalRecord[columns[i].column] > columns[i].value )
					flag = 0;
				break;
			case Greater:
				if( journalRecord[columns[i].column] <= columns[i].value )
					flag = 0;
				break;
			case GreaterOrEqual:
				if( journalRecord[columns[i].column] < columns[i].value )
					flag = 0;
				break;
		}
		if( flag == 0 )
		{
			return flag;
		}
	}
	return flag;
}