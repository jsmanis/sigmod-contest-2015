#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include "constants.h"
#include "Journal.h"

Journal *createJournal(int columnCount)
{
	Journal *journal = malloc(sizeof(Journal));
	if(journal == NULL)
	{
		perror("malloc");
		exit(EXIT_FAILURE);
	}
	journal->records = malloc(INIT_SIZE*sizeof(JournalRecord*));
	if(journal->records == NULL)
	{
		perror("malloc");
		exit(EXIT_FAILURE);
	}
	journal->arraySize = INIT_SIZE;
	int i=0;
	for(i=0; i<journal->arraySize; i++)
	{
		journal->records[i] = NULL;
	}
	journal->columnCount = columnCount;
	journal->recordCount = 0;
	return journal;
}

void destroyJournal(Journal *journal)
{
	if(journal == NULL)
	{
		fprintf(stderr, "Invalid journal argument");
		exit(EXIT_FAILURE);
	}
	int i=0;
	for(i=0; i<journal->recordCount; i++)
	{
		destroyJournalRecord( journal->records[i] );
	}
	free(journal->records);
	free(journal);
}

void insertJournalRecord(Journal *journal, JournalRecord *record)
{
	Journal debug = *journal;
	if(journal == NULL)
	{
		fprintf(stderr, "Invalid journal argument\n");
		exit(EXIT_FAILURE);
	}
	if(record == NULL)
	{
		fprintf(stderr, "Invalid journal record argument\n");
		exit(EXIT_FAILURE);
	}
	//If the maximum capacity of the current journal has been reached, use realloc to extend the journal
	if(journal->arraySize == journal->recordCount)
	{
		journal = extendJournal(journal, INIT_SIZE);
	}
	debug = *journal;
	journal->records[journal->recordCount] = record;
	journal->recordCount++;
}

Journal* extendJournal(Journal* journal, int extensionSize)
{
	JournalRecord** newArr =  realloc(journal->records, ( journal->arraySize + extensionSize )*sizeof(JournalRecord*));
	if(newArr != NULL)
	{
		journal->records = newArr;
	}
	else
	{
		fprintf(stderr, "Realloc failed\n");
		exit(EXIT_FAILURE);
	}
	//journal = realloc(journal, ( journal->arraySize + extensionSize )*sizeof(JournalRecord*) );
	if(journal == NULL)
	{
		perror("realloc");
		exit(EXIT_FAILURE);
	}
	int i;
	int newArraySize = journal->arraySize + extensionSize;
	for(i=journal->arraySize; i<newArraySize; i++)
	{
		journal->records[i] = NULL;
	}
	journal->arraySize = newArraySize;
	return journal;
}

void deleteJournalEntry(Journal* journal, int index)
{
	if( journal == NULL )
	{
		fprintf(stderr, "Invalid journal argument\n");
		exit(EXIT_FAILURE);
	}
	if( index < 0 )
	{
		fprintf(stderr, "Index out of bounds\n");
		exit(EXIT_FAILURE);
	}
	else if( index >= (journal->recordCount) )
	{
		fprintf(stderr, "Index out of bounds\n");
		exit(EXIT_FAILURE);
	}
	deleteJournalRecord(&(journal->records[index]));
}

int getJournalRecords(Journal *journal, uint64_t startRange, uint64_t endRange, uint32_t columnCount , Column_t *columns)
{
	//printf("Getting journal records with start range %lu and end range %lu with column count %d\n", startRange, endRange, journal->columnCount);
	int flag = 0, start = -1;
	fflush(stdout);

	if(journal == NULL)
	{
		fprintf(stderr, "Invalid journal\n");
		return -1;
	}
	int i=0;

	if ( journal->records[0]->transactionId > startRange )
	{
		start = 0;
	}
	else if (journal->records[journal->recordCount-1]->transactionId < startRange )
	{
		start = -1;
	}
	else
	{
		start = BinarySearch( journal, startRange, 0, journal->recordCount-1);
	}
	if ( start == -1 )
		return 0;
	if ( columnCount == -1 && start != -1 )
	{
		return 1;
	}
	
	for(i=start; i<journal->recordCount; i++)
	{
		if (journal->records[i]->transactionId <= endRange)
		{
			int j;
			for(j=0; j<journal->records[i]->rowCount; j++)
			{
				if( recordCheck(journal->records[i]->columnValues[j], columnCount, columns) == 1)
					return 1;			
			}
		}
		else 
			break;
	}
	return 0;
}


int BinarySearch( Journal *journal, uint64_t key, int imin, int imax)
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
		if (journal->records[imid]->transactionId > key)
		{
			// key is in lower subset
			if (journal->records[imid-1]->transactionId < key)
			{
				return imid;
			}
			return BinarySearch(journal, key, imin, imid - 1);
		}
		else if (journal->records[imid]->transactionId  < key)
		{
			// key is in upper subset
			return BinarySearch(journal, key, imid + 1, imax);
		}
		else
		{
			// key has been found
			return imid;
		}
	}
}


void printJournal(Journal* journal)
{
	if(journal == NULL)
	{
		fprintf(stderr, "Invalid journal\n");
		exit(EXIT_FAILURE);
	}
	int i=0;
	for(i=0; i<journal->recordCount; i++)
	{
		printf("Journal Record (TID) %lu\n",journal->records[i]->transactionId);
		printValues(journal->records[i]);
		printf("\n");
	}
}
