#include <stdlib.h>
#include <stdio.h>
#include "constants.h"
#include "hash.h"
#include "utilities.h"

extendibleHash* createHash ( void )
{
	Hash **indexptr;
	extendibleHash* extendptr;
	int i, tableSize;

	extendptr = malloc( sizeof(extendibleHash) );
	if( extendptr == NULL )
	{
		perror("create failure");
		return NULL;
	}
	extendptr->globalDepth = INITIALGLOBALDEPTH;
	tableSize = pow(2, extendptr->globalDepth);
	extendptr->tableSize = tableSize;
	indexptr = malloc( tableSize * sizeof(Hash*));
	if( indexptr == NULL )
	{
		perror("create failure");
		return NULL;
	}
	for ( i = 0; i < tableSize; i++ )
		indexptr[i] = NULL;
	extendptr->hashptr = indexptr;

	return extendptr;
}

/*	Insert the elenent "elem" into the hash table 
	Returns :
 	0 uppon success
 	-1 uppon failure
*/
int insertRecord ( extendibleHash *extendptr, element *elem ,RangeArray *tempTr)
{
	int pos, globalDepth, tableSize, existPos;
	Hash *newBucketptr, *oldBucketptr; 
	Hash **newIndexptr, **indexptr;
	element *failElem = NULL;
	
	indexptr = extendptr->hashptr;
	globalDepth = extendptr->globalDepth;
	tableSize = extendptr->tableSize;

	//Call the hash function for the element

	pos = hashFunc(elem, tableSize);
	existPos = searchKey( indexptr, elem, pos);
	if ( existPos != -1)
	{
		insertElem(indexptr[pos], elem, tempTr, existPos);
		free(elem);
		return 0;
	}
	if( indexptr[pos] == NULL )
	{
		newBucketptr = createBucket( globalDepth );		//creates a new bucket and inserts the element 
		if ( newBucketptr == NULL )
			return -1;
		else
		{
			insertElem( newBucketptr, elem, tempTr, -1);
			indexptr[pos] = newBucketptr;
		}
	}
	else
	{
		if( insertElem( indexptr[pos], elem, tempTr, -1) != 0 )
		{
			if ( indexptr[pos]->localDepth < globalDepth )
			{

				/* Here we split the bucket */

				indexptr[pos]->localDepth++;
				oldBucketptr = indexptr[pos];		//pointer to the old bucket
				newBucketptr = createBucket( indexptr[pos]->localDepth );		//creates a new empty bucket 
				if ( newBucketptr == NULL )
				{
					return -1;
				}
				else
				{
					Fixpointers(indexptr, globalDepth, tableSize, newBucketptr, oldBucketptr, pos);	
				}
				if( redistributeEntries( indexptr, oldBucketptr, newBucketptr, &failElem, tableSize, elem, tempTr) == -1)
				{
					if( insertRecord( extendptr, failElem, tempTr) == 0 )
					{
						return 0;
					}
					else 
					{
						printf("Failed to insert the record\n");
						return -1;
					}
				}

			}
			else
			{

				/* At this point we have to double the table size */

				globalDepth++;
				tableSize = pow(2, globalDepth);
				indexptr = realloc( indexptr, tableSize*sizeof(Hash*));
				if( indexptr == NULL )
				{
					perror("double index failure(realloc)");
					return -1;
				}
				copyIndex( indexptr, tableSize);
				extendptr->hashptr = indexptr;
				extendptr->globalDepth = globalDepth;
				extendptr->tableSize = tableSize;

				/* Now we split the bucket that caused the collision*/

				indexptr[pos]->localDepth++;
				oldBucketptr = indexptr[pos];		//pointer to the old bucket
				newBucketptr = createBucket(globalDepth);		//creates a new bucket and inserts the element
				pos = hashFunc( elem, tableSize);	//the new hash value
				if ( newBucketptr == NULL )
				{
					return -1;
				}
				else
				{
					indexptr[pos] = newBucketptr;
				}
				if( redistributeEntries( indexptr, oldBucketptr, newBucketptr, &failElem, tableSize, elem, tempTr) == -1)
				{
					if( insertRecord( extendptr, failElem, tempTr) != -1 )
					{
						return 0;
					}
					else 
					{
						printf("Failed to insert the record\n");
						return -1;
					}
				}
				else
				{
					return 0;
				}	
			}
		}

	} 
	return 0;
}

/*	Fix the index pointers when we do a bucket split 
	half the pointers points to the old bucket and the 
	other to the new. 
*/

void Fixpointers(Hash **index, int globalDepth, int tableSize, Hash *newBucketptr, Hash *oldBucketptr, int hashPos)
{
	int i, initPos, var = 0, step, depthDif, numOfPtr, j;

	if ( oldBucketptr->localDepth == globalDepth )
	{	
		if ( hashPos+tableSize/2 < tableSize) 
		{
			index[hashPos+tableSize/2] = newBucketptr;
			return;
		}
		else
		{
			index[hashPos-tableSize/2] = newBucketptr;
			return;
		}
	}
	depthDif = globalDepth - oldBucketptr->localDepth + 1;
	numOfPtr = pow( 2, depthDif);
	step = tableSize/numOfPtr;

	initPos = hashPos;
	for (i = 0; i < numOfPtr-1; i++)
	{
		initPos -= step;
		if ( initPos < step )
			break;
	}

	i = initPos;
	for( j = i+step; j < tableSize; j+= 2*step)
	{
		index[j] = newBucketptr;
	}
}


/*	Deletes an element for the hash index
	Returns :
 	0 uppon success
 	-1 uppon failure
*/

int DeleteHashRecord (  extendibleHash *extendptr, element *elem )
{
	int keyPos, tableSize, hashPos;
	Hash **index;
	element tempElem;


	tableSize = extendptr->tableSize;
	index = extendptr->hashptr;
	hashPos = hashFunc( elem, tableSize);
	keyPos = searchKey(  index, elem, hashPos );
	if( keyPos == -1 )
	{
		printf("There is no record with key value %lu\n",elem->primaryKey );
		return -1;
	}
	free( index[hashPos]->elementArr[keyPos]->transArray );
	free( index[hashPos]->elementArr[keyPos] );
	index[hashPos]->elementArr[keyPos] = NULL;
	index[hashPos]->numOfElems--;
	tryCombine(extendptr, hashPos);
	return 0;
}

int hashFunc ( element *elem, int tableSize)
{
	return (elem->primaryKey) % tableSize; 
}

/*	Creates a new bucket and inserts the new element 
	Returns:
	a pointer pointing at the new bucket uppon success 
	NULL uppon failure 
*/

Hash* createBucket( int depth)		
{
	Hash *newBucketptr;
	int i;


	newBucketptr = malloc( sizeof(Hash));
	if( newBucketptr == NULL )
	{
		perror("create bucket failure");
		return NULL;
	}
	newBucketptr->localDepth = depth;
	newBucketptr->numOfElems = 0;
	for ( i = 0; i < BUCKETCAPACITY; i++)
		newBucketptr->elementArr[i] = NULL;
	return newBucketptr;
} 



/*	Inserts an element in the bucket where bucketptr points at
 	Returns :
 	0 uppon success
 	-1 uppon failure
 */
int insertElem( Hash* bucketptr, element *elem, RangeArray *tempTr, int searchPos)
{
	int numOfElems = bucketptr->numOfElems, i;
	RangeArray *transArr;

	if( searchPos == -1 )
	{
		if ( numOfElems == BUCKETCAPACITY )	//bucket is full
		{
			return -1;
		}

		if ( elem->transArray == NULL )	//	We have a new record ( not from redistribution )
		{
			transArr = malloc(RANGECAPACITY*sizeof(RangeArray));

			/*	Copy the contents of the "tempTr" into the range RangeArray	*/

			transArr[0].transactionId = tempTr->transactionId;
			transArr[0].journalptr[0] = tempTr->journalptr[0];
			transArr[0].journalptr[1] = NULL;
			
			elem->numOfTrans = 1;
			elem->transArray = transArr;
		}
		
		for(i=0; i<BUCKETCAPACITY; i++)
		{
			if( bucketptr->elementArr[i] == NULL )
				break;
		}
		bucketptr->elementArr[i] = elem;
		bucketptr->numOfElems++;
	}
	else
	{
		updateRange( bucketptr->elementArr[searchPos] ,tempTr);
	}
	return 0;
}


/*	Redistributes the records between the new and the old bucket 
	Returns : 
	0 uppon success
 	-1 uppon failure 
*/

int redistributeEntries( Hash **index, Hash *oldBucketptr, Hash *newBucketptr, element **failElem, int tableSize, element *elem, RangeArray *tempTr )
{
	int newPos, i;
	element tempElem, *tempArr[BUCKETCAPACITY+1];

		oldBucketptr->numOfElems = 0;

		for ( i = 0; i < BUCKETCAPACITY; i++)
			tempArr[i] = oldBucketptr->elementArr[i];

		tempArr[BUCKETCAPACITY] = elem;

		for ( i = 0; i < BUCKETCAPACITY; i++)
			oldBucketptr->elementArr[i] = NULL;

		for ( i = 0; i <= BUCKETCAPACITY; i++)
		{
			newPos = hashFunc( tempArr[i], tableSize );
			if( insertElem( index[newPos], tempArr[i], tempTr, -1 ) == -1)
			{
				*failElem = tempArr[i];
				return -1;
			}
		}
	return 0;
}


/*	Copy the contents of the initial index to the new
*/

void copyIndex( Hash **oldIndex, int tableSize)
{
	int i, prevSize = tableSize/2;
	
	for ( i = 0; i < prevSize; i++)
	{
		oldIndex[i+prevSize] = oldIndex[i];
	}
	return;
}

void collapseIndex( Hash **oldIndex, Hash **newIndex, int tableSize)
{
	int i;
	
	for ( i = 0; i < tableSize; i++)
	{
		newIndex[i] = oldIndex[i];
	}
}

// void printIndex( extendibleHash *hash , uint32_t columnCount )
// {
// 	int i, j, q;
// 	Hash **Index;

// 	Index = hash->hashptr;
// 	// printf("\nGLOBALDEPTH : %d\nTABLESIZE : %d\n", hash->globalDepth, hash->tableSize);
// 	for ( i = 0; i < hash->tableSize; i++)
// 	{
// 		if ( Index[i] != NULL )
// 		{
// 			// printf("In position %d the bucket has local depth \"%d\" ,\"%d\" record(s)\n", i, Index[i]->localDepth, Index[i]->numOfElems);
// 			for ( j = 0; j < BUCKETCAPACITY; j++)
// 			{
// 				if (Index[i]->elementArr[j].isDeleted != 1)
// 				{
// 					// printf("\t|Primary Key : %lu|# of transactions %d|\n",Index[i]->elementArr[j].primaryKey, Index[i]->elementArr[j].numOfTrans );
// 					for(q = 0; q < Index[i]->elementArr[j].numOfTrans; q++)
// 					{				
// 						// printf("\t\t-------------------------------------\n");
// 						// printf("\t\t|transaction id : %lu| numOfTrans : %d |\n", Index[i]->elementArr[j].transArray[q].transactionId, Index[i]->elementArr[j].numOfTrans);
// 						if ( Index[i]->elementArr[j].transArray[q].journalptr[0] != NULL )
// 						{
// 							if ( Index[i]->elementArr[j].transArray[q].journalptr[0][0] != Index[i]->elementArr[j].primaryKey )
// 							{
// 								printf("1\n");
// 								// printf("%lu\n", Index[i]->elementArr[j].primaryKey );
// 								// printJournalRecord( Index[i]->elementArr[j].transArray[q].journalptr[0], columnCount );
// 							}
// 						}
// 						if ( Index[i]->elementArr[j].transArray[q].journalptr[1] != NULL )
// 						{
// 							if ( Index[i]->elementArr[j].transArray[q].journalptr[1][0] != Index[i]->elementArr[j].primaryKey )
// 							{
								
// 								printf("1\n");
// 								// printf("%lu\n", Index[i]->elementArr[j].primaryKey );
// 								printJournalRecord( Index[i]->elementArr[j].transArray[q].journalptr[1], columnCount );
// 							}
// 						}
// 					}
// 					// printf("\t\t-------------------------------------\n");
// 				}
// 				else
// 				{
// 					// printf("This Record is Deleted !\n");
// 				}
// 			}
// 		}
// 		else
// 		{
// 			// printf("NULL pointer\n");
// 		}
// 	} 
// }

/*	This function searches th hash table for a record with key value = elem.primaryKey
	Returns :
	-1 if the record can't be found
	it's position in elementArr otherwise
*/

int searchKey( Hash **index, element *elem, int pos)
{
	Hash *bucketptr;
	int i;

	bucketptr = index[pos];
	if ( bucketptr == NULL )
		return -1; 
	for ( i = 0; i < BUCKETCAPACITY; i++)
	{
		if( bucketptr->elementArr[i] != NULL )
		{
			if ( bucketptr->elementArr[i]->primaryKey == elem->primaryKey )
			{
				return i;
			}
		}
	}
	return -1;
}

void printBucketContents( Hash *bucket )
{
	int i;
	
	printf("########BUDDY BUCKET#######\n\n");
	for ( i = 0; i < BUCKETCAPACITY; i++)
	{
		printf("\t  %lu,  %d\n", bucket->elementArr[i]->primaryKey, bucket->elementArr[i]->numOfTrans);
	}
	printf("\n#################\n");
} 

/*	Checks if  with the deletion of one record the bucket 
	can merge with it's buddy bucket an shrink the index. 
*/

void tryCombine( extendibleHash *extendptr, int hashPos)
{
	Hash *buddy, **index, **newIndex;
	int tableSize, i;

	index = extendptr->hashptr;
	tableSize = extendptr->tableSize;
	buddy = buddyBucketTest( extendptr, hashPos);
	if ( buddy != NULL )
	{
		printBucketContents(buddy);

		if( (buddy->numOfElems+index[hashPos]->numOfElems) <= BUCKETCAPACITY )	//The two buckets can merge into one 
		{
			mergeBuckets( index[hashPos], buddy);
			for ( i = 0; i < tableSize; i++)	
			{
				if ( index[i] == buddy )
					index[i] = index[hashPos];	//Set the pointers to the buddy bucket in order to point in the initial bucket
			}
			free( buddy );
			if ( colapseTest(extendptr) == TRUE )		//The index can shrink
			{
				extendptr->globalDepth--;
				tableSize = pow(2,extendptr->globalDepth);
				extendptr->tableSize = tableSize; 
				newIndex = malloc (tableSize*sizeof(Hash*));	//Allocate a new index half the size of the previous one
				if ( newIndex == NULL )
				{
					perror("malloc (new index)");
					return;
				}
				collapseIndex( index, newIndex, tableSize);
				free( index );
				extendptr->hashptr = newIndex;
				if( hashPos > tableSize )
					hashPos = hashPos - tableSize;
				tryCombine( extendptr, hashPos);	//Recursive call of the function in order to shrink the index once more
			}
		}
	}
	return;
}

/*	Put the records of two buckets into one.
	Whatever is left is filled with the value of primary key "-1"
*/

void mergeBuckets( Hash *dest, Hash *source )
{
	int i, j;

	dest->localDepth--;
	dest->numOfElems += source->numOfElems;
	i = 0;
	j = 0;
	while( i < BUCKETCAPACITY )
	{
		if ( dest->elementArr[i] == NULL )
		{
			while( j < BUCKETCAPACITY )
			{
				if ( source->elementArr[j] != NULL )
				{
					j++;
					break;
				}
				j++;
			}
			dest->elementArr[i] = source->elementArr[j-1];
		}
		i++;
	}
}

int destroyHash( extendibleHash *extendptr )
{
	int i, j, tableSize, globalDepth, count, step, numOfPtr, c;
	Hash ** index;

	index = extendptr->hashptr;
	tableSize = extendptr->tableSize;
	globalDepth = extendptr->globalDepth;
	for ( i = 0; i < tableSize; i++)
	{
		if ( index[i] != NULL )
		{
			if( index[i]->localDepth != globalDepth )
			{
				numOfPtr = pow(2, globalDepth-index[i]->localDepth);
				step = tableSize/numOfPtr;
				c = i + step;				
				for ( j = 0; j < numOfPtr-1; j++)
				{
					index[c] = NULL;
					c += step;
				}
			}
			for ( count = 0; count < BUCKETCAPACITY; count++)
			{
				if ( index[i]->elementArr[count] != NULL )
				{
					free(index[i]->elementArr[count]->transArray);
				}
				free(index[i]->elementArr[count]);
			}
			free( index[i] );
		}
	}
	free(index);
	free(extendptr);
	return 0;
}

/*
	Returns the array of transactions the key is in !
	Sto elem apothikeuw to stoixeio pou vrika opote  to stelneis &elem otan thn kaleis opote 
	apo kei mporeisna vreis posa stoixeia exei o pikanas ara kai to teleutaio 
	transaction pou sumetexei to kleidi (elem.numOfTrans.journalptr[0]) <-o deikths sto journal
	 
*/
RangeArray * GetHashRecord ( extendibleHash *extendptr, element *elem )
{
	int pos, existPos;
	Hash **index;

	index = extendptr->hashptr;
	pos = hashFunc(elem, extendptr->tableSize);
	existPos = searchKey( index, elem, pos);
	if ( existPos == -1 )
	{
		return NULL;
	}
	copyElement( elem, index[pos]->elementArr[existPos]);

	return index[pos]->elementArr[existPos]->transArray;
}

int GetHashRecords( extendibleHash *extendptr, element *elem, uint64_t rangeStart, uint64_t rangeEnd ,uint32_t columnCount , Column_t *columns)
{
	int i, start = -1;
	RangeArray * range;
	
	range = GetHashRecord(extendptr, elem);
	if (range == NULL )
	{
		return 0;
	}

	if (range[0].transactionId > rangeStart )
	{
		start = 0;
	}
	else if (range[elem->numOfTrans-1].transactionId < rangeStart)
	{
		start = -1;
	}
	else 
	{
		start = BinarySearchRange( range, rangeStart, 0, elem->numOfTrans-1);
		
	}
	if ( start == -1 )
		return 0;


	for (i = start; i < elem->numOfTrans; i++)
	{
		if ( range[i].transactionId <= rangeEnd )
		{
			if ( recordCheck(range[i].journalptr[0], columnCount, columns) == 1 )
					return 1;
			if ( range[i].journalptr[1] != NULL )
			{
				if ( recordCheck(range[i].journalptr[1], columnCount, columns) == 1 )
					return 1;
			}
		}
		else 
		{
			break;
		}
	}

	return 0;
}

int BinarySearchRange( RangeArray *range, uint64_t key, int imin, int imax)
{
	// test if array is empty
	if (imax < imin)
	{
		// set is empty, so return value showing not found
		return -1;
	}
	else
	{
		// calculate midpoint to cut set in half
		int imid = (imin+imax)/2;
	
		// three-way comparison
		if (range[imid].transactionId > key)
		{
			// key is in lower subset
			if (range[imid-1].transactionId < key)
			{
				return imid;
			}
			return BinarySearchRange(range, key, imin, imid - 1);
		}
		else if (range[imid].transactionId  < key)
		{
			// key is in upper subset
			return BinarySearchRange(range, key, imid + 1, imax);
		}
		else
		{
			// key has been found
			return imid;
		}
	}
}
