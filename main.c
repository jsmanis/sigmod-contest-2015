/*
	* Inspired from SIGMOD Programming Contest 2015.
	* http://db.in.tum.de/sigmod15contest/task.html
	* Simple requests parsing and reporting.
**/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <pthread.h>
#include "constants.h"
#include "utilities.h"
#include "Journal.h"
#include "JournalRecord.h"
#include "hash.h"
#include "thread.h"


//Global variables for multi-threaded flush
int threadCounter;
pthread_mutex_t condMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t counterMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond;

void* routine(void* arg);
void multiThreadedFlush(int k, validationListNode_t* start, uint64_t to, validationListNode_t** );
//---------------------------------------------------------------------------

int flushCounter = 0;

static uint32_t* schema = NULL;
extendibleHash **hashTables;
int relationCount;
validationList_t *ValidationList;

//The array of journals for each relation
Journal **journals;


static void processDefineSchema(DefineSchema_t *s)
{
	int i;
	relationCount = s->relationCount;
	if ( schema == NULL) free(schema);
	schema = malloc( sizeof(uint32_t)*s->relationCount );
	hashTables = malloc( sizeof(extendibleHash*) * s->relationCount );
	ValidationList = malloc( sizeof(validationList_t) );
	ValidationList->start = NULL;
	ValidationList->currentValidation = NULL;
	ValidationList->end = NULL;
	if ( hashTables == NULL )
	{
	perror("malloc!");
	return;
	}

	for(i = 0; i < s->relationCount; i++)
	{
	hashTables[i] = createHash();
	if ( hashTables[i] == NULL )
	{
		perror("malloc error!");
		return;
	}
	schema[i] = s->columnCounts[i];
	}
}

static void processTransaction(Transaction_t *t)
{
	int i, j, lastPos;
	const char* reader = t->operations;
	element elem, *mainElem;
	RangeArray *range = NULL;

	//This array will be used to count how many operations happen for each relation
	int numberOfRows[relationCount];
	for(i=0; i<relationCount; i++)
	{
		numberOfRows[i] = 0;
	}
	//This array counts the number of deletion operations for each relation in the transaction
	int numberOfDels[relationCount];
	for(i=0; i<relationCount; i++)
	{
		numberOfDels[i] = 0;
	}


	for(i=0; i < t->deleteCount; i++)
	{
		const TransactionOperationDelete_t* o = (TransactionOperationDelete_t*)reader;
		int j;
		int rows,dels;
		rows = dels = 0;
		for(j=0; j<o->rowCount; j++)
		{
			elem.primaryKey = o->keys[j];
			elem.numOfTrans = 0;
			elem.transArray = NULL;
			range = GetHashRecord(hashTables[o->relationId],&elem);
			if ( range != NULL )
			{
				if (range[elem.numOfTrans-1].journalptr[1] == NULL )
				{
					if (range[elem.numOfTrans-1].journalptr[0][schema[o->relationId]] == 0)
					{
						rows++;
						dels++;
					}
				}
				else
				{
					if (range[elem.numOfTrans-1].journalptr[1][schema[o->relationId]] == 0)
					{
						rows++;
						dels++;
					}
				}
			}
			
		}
		
		numberOfRows[o->relationId] += rows;
		numberOfDels[o->relationId] += dels;
		reader+=sizeof(TransactionOperationDelete_t)+(sizeof(uint64_t)*o->rowCount);
	}
	range = NULL;


	for(i=0; i < t->insertCount; i++)
	{
		const TransactionOperationInsert_t* o = (TransactionOperationInsert_t*)reader;
		numberOfRows[o->relationId] += o->rowCount;

		reader+=sizeof(TransactionOperationInsert_t)+(sizeof(uint64_t)*o->rowCount*schema[o->relationId]);
	}

	//Stores the journal records for each relation.
	//records[0] holds the records for relation 0
	// records[1] holds the records for relation 1 e.t.c.
	JournalRecord **records = malloc(relationCount*sizeof(JournalRecord*));
	if(records == NULL)
	{
		perror("processTransaction: malloc:");
		exit(EXIT_FAILURE);
	}

	//Allocate memory for storing each relation's operations
	for(i=0; i<relationCount; i++)
	{
		//If the current transaction operates on the ith relation
		if(numberOfRows[i] != 0)
		{
			//Create a journal record for it
			records[i] = createJournalRecord(numberOfRows[i], schema[i]);
			records[i]->transactionId = t->transactionId;
		}
		else
		{
			//If there are not operations for the ith relation on the current transaction the entry will be NULL
			records[i] = NULL;
		}
	}

	//Reset the pointer to where the deletion operations are stored
	reader = t->operations;

	// printf("Inserting deletions into the journals(%lu)\n", t->deleteCount);
	fflush(stdout);

	int k;
	for(i=0; i < t->deleteCount; i++)
	{
		const TransactionOperationDelete_t* o = (TransactionOperationDelete_t*)reader;
		uint64_t *tempCols;
		uint64_t *columnValues[numberOfDels[o->relationId]];
		//Allocate memory for all the column values, not just the pk
		int count = 0;
		for( j = 0; j < o->rowCount; j++ )
		{
			elem.primaryKey = o->keys[j];
			elem.numOfTrans = 0;
			elem.transArray = NULL;
			range = GetHashRecord(hashTables[o->relationId],&elem);
			if ( range == NULL )
			{
				continue ;
			}
			if (range[elem.numOfTrans-1].journalptr[1] == NULL )
			{
				if (range[elem.numOfTrans-1].journalptr[0][schema[o->relationId]] == 1)
				{
					continue;
				}
				else
				{
					columnValues[count] = range[elem.numOfTrans-1].journalptr[0];
				}               
			}
			else
			{
				if (range[elem.numOfTrans-1].journalptr[1][schema[o->relationId]] == 1)
				{
					continue;
				}
				else
				{
					columnValues[count] = range[elem.numOfTrans-1].journalptr[1];
				}
			} 


			insertColumnValues( records[o->relationId], count, columnValues[count], OP_DELETE );
			
			mainElem = malloc(sizeof(element));
			mainElem->primaryKey = o->keys[j];
			mainElem->numOfTrans = 0;
			mainElem->transArray = NULL;

			range = malloc(sizeof(RangeArray));
			range->transactionId = t->transactionId;
			range->journalptr[0] = (records[o->relationId]->columnValues[count]);

			insertRecord( hashTables[o->relationId], mainElem, range );
			free( range );
			count++;
		}

		reader+=sizeof(TransactionOperationDelete_t)+(sizeof(uint64_t)*o->rowCount);
	}


	for(i=0; i < t->insertCount; i++)
	{
		const TransactionOperationInsert_t* o = (TransactionOperationInsert_t*)reader;
		
		int count = 0;
		for(j=0; j<(o->rowCount*schema[o->relationId]); j+=schema[o->relationId] )
		{
			
			uint64_t tempValues[ schema[o->relationId] ];
			int a;

			for(a=0; a<schema[o->relationId]; a++)
			{
				tempValues[a] = o->values[a + j];
			}
			

			fflush(stdout);
			if(insertColumnValues(records[o->relationId], numberOfDels[o->relationId] + count, tempValues, OP_INSERT ) < 0)
			{
				fprintf(stderr, "Failed to insert column values\n");
				exit(EXIT_FAILURE);
			}
			
			mainElem = malloc(sizeof(element));

			mainElem->primaryKey = o->values[j];
			mainElem->numOfTrans = 0;
			mainElem->transArray = NULL;

			range = malloc(sizeof(RangeArray));
			range->transactionId = t->transactionId;
			range->journalptr[0] = (records[o->relationId]->columnValues[numberOfDels[o->relationId] + count]);

			if ( insertRecord( hashTables[o->relationId], mainElem, range ) != 0)
				return;
			free( range );
			count++;
		}

		reader+=sizeof(TransactionOperationInsert_t)+(sizeof(uint64_t)*o->rowCount*schema[o->relationId]);
	}


	for(i=0; i<relationCount; i++)
	{
		if(records[i] != NULL)
		{
			insertJournalRecord(journals[i], records[i]);
		}
	}
	free(records);

}


int cmpfunc( const void *a , const void *b )
{
	Op_t l = (( Column_t *)a)->op;
	Op_t r = (( Column_t *)b)->op;
	return (l - r);
}

void sortQueries( Column_t *c, uint32_t columnCount )
{
	int i,  position = 0, flag = 0, n, flag1 = 0;
	Column_t swap;
	
	n = (int)columnCount;

	if ( n == 0 )
	return;


	for ( i = 0 ; i < n ; i++ )
	{

	if ( ( c[i].column != 0 || c[i].op != Equal) && flag == 0 )
	{
		flag = 1;
		position = i;
	}
	if ( flag == 1 )
	{
		if ( c[i].column == 0 && c[i].op == Equal )
		{
		swap.column = c[i].column;
		swap.op = c[i].op;
		swap.value = c[i].value;

		c[i].column = c[position].column;
		c[i].op = c[position].op;
		c[i].value = c[position].value;

		c[position].column = swap.column;
		c[position].op = swap.op;
		c[position].value = swap.value;

		flag1 = 1;
		break;
		}
	}
	}
	if ( flag1 == 0 )
	qsort(c, n, sizeof(Column_t), cmpfunc);

}


void checkQuery( ListRange *List, uint32_t columnNum, Op_t option, uint64_t val)
{
	int flag;
	ListNode_t *tempNode = List->start, *prev, *next;

	prev = tempNode;
	while ( tempNode != NULL )
	{
	flag = 0;
	next = tempNode->next;
	switch( option )
	{
		case Equal:
		if( tempNode->journalptr[columnNum] != val )
		{
			deletefromList( List, prev, tempNode );
			flag = 1;
		}
		break;
		case NotEqual:
		if( tempNode->journalptr[columnNum] == val )
		{
			deletefromList( List, prev, tempNode );
			flag = 1;
		}               
		break;
		case Less:
		if( tempNode->journalptr[columnNum] >= val )
		{
			deletefromList( List, prev, tempNode );
			flag = 1;
		}               
		break;
		case LessOrEqual:
		if( tempNode->journalptr[columnNum] > val )
		{
			deletefromList( List, prev, tempNode );
			flag = 1;
		}               
		break;
		case Greater:
		if( tempNode->journalptr[columnNum] <= val )                    
		{
			deletefromList( List, prev, tempNode );
			flag = 1;
		}               
		break;
		case GreaterOrEqual:
		if( tempNode->journalptr[columnNum] < val )
		{
			deletefromList( List, prev, tempNode );
			flag = 1;
		}               
		break;
	}
	if ( flag == 0 )
	{
		prev = tempNode;
	}
	tempNode = next;
	}
}

static void processValidationQueries(ValidationQueries_t *v){
	int i, j;
	const char *query = v->queries;
	ListRange *List;
	element elem;
	validationListNode_t *ListNode;

	ListNode = malloc(sizeof(validationListNode_t));
	ListNode->validation = v;
	ListNode->next = NULL;

	if ( ValidationList->start == NULL )
	{
		ValidationList->start = ListNode;
		ValidationList->currentValidation = ListNode;
		ValidationList->end = ListNode;
	}
	else
	{
		ValidationList->end->next = ListNode;
		ValidationList->end = ListNode;
	}
}

static int computeQueries(ValidationQueries_t* v, int multiThread)
{
	const char *query ;
	int i,j,retVal;
	retVal = 2;
	query = v->queries;
	element elem;
	Query_t *q;
	for ( i = 0; i < v->queryCount; i++ )
	{
		q = (Query_t *)query;
		if ( q->columnCount == 0 )
		{
			retVal = getJournalRecords( journals[q->relationId], v->from, v->to, -1, q->columns);
			if (retVal == 1)
			{
				if(multiThread)
				{
					return retVal;
				}		
				printf("%d\n", retVal);
				return retVal;
			}	
		}
		else
		{
			sortQueries( q->columns, q->columnCount );

			if ( q->columns[0].column == 0 && q->columns[0].op == Equal )
			{
				elem.primaryKey = q->columns[0].value;
				elem.numOfTrans = 0;
				elem.transArray = NULL;
				retVal = GetHashRecords( hashTables[q->relationId], &elem, v->from, v->to, q->columnCount, q->columns);
				if ( retVal == -1 )
				{
					printf("Validation Failure!\n");
					return -1;
				}
				else if ( retVal == 1 )
				{
					if(multiThread)
					{
						return retVal;	
					}
					printf("%d\n",retVal);
					return retVal;
				}
			}
			else
			{
				retVal = getJournalRecords( journals[q->relationId], v->from, v->to, q->columnCount, q->columns);
				if ( retVal == -1 )
				{
					printf("Validation Failure!\n");
					return;
				}
				else if ( retVal == 1 )
				{
					if(multiThread)
					{
						return retVal;
					}
					printf("%d\n",retVal);
					return retVal;
				}
			}
		}
		query += sizeof(Query_t) + sizeof(Column_t)*q->columnCount ;
		if( i == v->queryCount - 1)
		{
			if( retVal == 0 )
			{
				if(multiThread)
				{
					return retVal;
				}
				printf("%d\n",retVal);
				return retVal;
			}
		} 
	}
}

static validationListNode_t* computeValidations(uint64_t to, validationListNode_t* current, int multiThread)
{
	int i, j, retVal = 2;
	ListRange *List = NULL;
	element elem;
	const char *query ;
	validationListNode_t *temp;
	ValidationQueries_t* v = current->validation;
	Query_t *q;
	while( v->validationId <= to )
	{
		
		computeQueries(v, multiThread);
		if( current->next == NULL )
			break;
		current = current->next;
		v = current->validation;
	}
	return current;
}

/**
Goes through the list of validations and prints the result for each one
*/
static void printFlushResults(validationListNode_t* initial, uint64_t endId)
{
	validationListNode_t* current = initial;
	ValidationQueries_t* v = initial->validation;
	while(v->validationId <= endId)
	{
		printf("%d\n", current->result);
		if(current->next == NULL)
		{
			break;
		}
		current = current->next;
		v = current->validation;
	}
}

static void processFlush(Flush_t *fl){

	int i, j, retVal = 2;
	ListRange *List = NULL;
	element elem;
	ValidationQueries_t *v;
	const char *query ;
	validationListNode_t *temp;
	Query_t *q;

	flushCounter++;
	if(flushCounter == FLUSH_ROUND)
	{
		flushCounter = 0;
		if( ValidationList->currentValidation == ValidationList->start )
		temp =ValidationList->currentValidation;
		else
		temp = ValidationList->currentValidation->next;

		if(temp == NULL)
		{
			return;
		}

		//Store the starting pointer to use it later for printing
		validationListNode_t* init = temp;

		if(MULTI_THREADED_MODE)
		{
			
			multiThreadedFlush(THREADS_NO, temp, fl->validationId, &temp);
			printFlushResults(init, fl->validationId);
			ValidationList->currentValidation = temp;
		}
		else
		{
			ValidationList->currentValidation = computeValidations(fl->validationId, temp, 0);
			
		}
	}
	
	
}


static void processForget(Forget_t *fo){
	// printf("Forget %lu\n", fo->transactionId);

}
static void processDestroy(void){
	int i;
	validationListNode_t *temp, *prev;
	
	for(i = 0; i < relationCount; i++)
	{
	destroyHash(hashTables[i]);
	destroyJournal(journals[i]);
	}
	free(journals);
	free(hashTables);
	temp = ValidationList->start;

	while ( temp != NULL )
	{
	prev = temp;
	free( temp->validation );
	temp = temp->next;
	free( prev );                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
	}
	free(ValidationList);
	free(schema);
}


Journal** initializeJournals();
void cleanup();


int main(int argc, char **argv)
{
	MessageHead_t head;
	void *body = NULL;
	uint32_t len;

	flushCounter = 0;

	while(1)
	{
		// Retrieve the message head
		if (read(0, &head, sizeof(head)) <= 0) { return -1; } // crude error handling, should never happen

		// Retrieve the message body
		if (head.messageLen > 0 )
		{
		body = malloc(head.messageLen*sizeof(char));
		if (read(0, body, head.messageLen) <= 0) { printf("err");return -1; } // crude error handling, should never happen
		len-=(sizeof(head) + head.messageLen);
		}
		int i;
		// And interpret it
		switch (head.type)
		{
		 case Done:
			// cleanup();
			processDestroy();
			return 0;
		 case DefineSchema:
			processDefineSchema(body);
			//Put all information relative to the schemas in the schemaInfo data structure
			journals = initializeJournals();
			free(body);
			break;
		 case Transaction:
			processTransaction(body);
			free(body);
			break;
		 case ValidationQueries:
			processValidationQueries(body);
			break;
		 case Flush:
			processFlush(body);
			free(body);

			break;
		 case Forget:
			processForget(body);
			free(body);
			break;
		 default:
			return -1; // crude error handling, should never happen
		}
	}



	return 0;
}



Journal** initializeJournals()
{
	Journal **journal = malloc(relationCount*sizeof(Journal*));
	if(journal == NULL)
	{
	perror("initializeJournals: malloc");
	exit(EXIT_FAILURE);
	}
	int i=0;
	for(i=0; i<relationCount; i++)
	{
	journal[i] = createJournal(schema[i]);
	}
	return journal;
}


/**
Spawns k threads to compute all validations starting from 'start' (included) up to the validation
with id 'to'
*/
void multiThreadedFlush(int k, validationListNode_t* start, uint64_t to, validationListNode_t** newCurrent)
{
	int status;
	threadCounter = k;
	pthread_t tids[k];


	if(status = pthread_cond_init(&cond, NULL))
	{
		strerror(status);
		exit(EXIT_FAILURE);
	}

	if( status = pthread_mutex_lock(&condMutex) )
	{
		strerror(status);
		exit(EXIT_FAILURE);
	}
	int i;

	ValidationThread **arguments = distributeValidations(start, to, k, newCurrent);
	if(arguments == NULL)
	{

	}

	for(i=0; i<k; i++)
	{
		int status = pthread_create(&tids[i], NULL, routine, (void*)arguments[i]);
		if(status != 0)
		{
			strerror(status);
			exit(EXIT_FAILURE);
		}
	}
	pthread_cond_wait(&cond, &condMutex);
	if(status = pthread_mutex_unlock(&condMutex))
	{
		strerror(status);
		exit(EXIT_FAILURE);
	}


	for(i=0; i<k; i++)
	{
		pthread_join(tids[i], NULL);
	}

	if(status = pthread_cond_destroy(&cond))
	{
		strerror(status);
	}
	for(i=0; i<k; i++)
	{
		free(arguments[i]);
	}
	free(arguments);

	return;
}


void* routine(void* arg)
{
	ValidationThread* args = (ValidationThread*) arg;
	int status;
	ThreadList* threadList = args->validationList;
	ThreadListNode* currentNode = threadList->head;
	while(currentNode != NULL)
	{
		validationListNode_t* n = currentNode->val;
		n->result = computeQueries(n->validation, 1);		
		currentNode = currentNode->next;
	}
	if( status = pthread_mutex_lock(&counterMutex) )
	{
		strerror(status);
		exit(EXIT_FAILURE);
	}
	threadCounter--;
	if(threadCounter == 0)
	{
		if(status = pthread_mutex_lock(&condMutex))
		{
			strerror(status);
			exit(EXIT_FAILURE);
		}
		pthread_cond_signal(&cond);
		if(status = pthread_mutex_unlock(&condMutex))
		{
			strerror(status);
			exit(EXIT_FAILURE);
		}
	}
	if(status = pthread_mutex_unlock(&counterMutex))
	{
		strerror(status);
		exit(EXIT_FAILURE);
	}
	destroyThreadList(&(args->validationList));
	pthread_exit(NULL);
}