#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include "constants.h"
#include "JournalRecord.h"


JournalRecord *createJournalRecord(int rowCount, int columnCount)
{
	//Allocate memory for the new journal record
	JournalRecord *record = malloc(sizeof(JournalRecord));
	if(record == NULL)
	{
		perror("malloc");
		exit(EXIT_FAILURE);
	}
	record->rowCount = rowCount;
	record->columnCount = columnCount;
	record->isDeleted = 0;
	record->columnValues = malloc(rowCount*sizeof(uint64_t*));
	if(record->columnValues == NULL)
	{
		perror("malloc");
		exit(EXIT_FAILURE);
	}
	int i=0;
	for(i=0; i<(record->rowCount); i++)
	{
		record->columnValues[i] = malloc( (columnCount + 1)*sizeof(uint64_t) );
		if(record->columnValues[i] == NULL)
		{
			perror("malloc");
			exit(EXIT_FAILURE);
		}
	}
	int j;
	for(i=0; i<rowCount; i++)
	{
		for(j=0; j<columnCount; j++)
		{
			record->columnValues[i][j] = 0;
		}
	}
	return record;
}


//Inserts values to the 'rowIndex' row of the 'record'. If opType is OP_DELETE changes dirty bit to 1
int insertColumnValues(JournalRecord* record, int rowIndex, uint64_t* values, char opType)
{
	if(record == NULL)
	{
		return -1;
	}
	else if(values == NULL)
	{
		return -1;
	}
	if( (rowIndex < 0) && (rowIndex > (record->rowCount - 1)) )
	{
		return -1;
	}
	int i=0;
	for(i=0; i<(record->columnCount); i++)
	{
		record->columnValues[rowIndex][i] = values[i];
	}
	if(opType == OP_DELETE)
	{
		record->columnValues[rowIndex][record->columnCount] = 1;
	}
	else if(opType == OP_INSERT)
	{
		record->columnValues[rowIndex][record->columnCount] = 0;
	}
	else
	{
		fprintf(stderr, "Invalid operation\n");
		exit(EXIT_FAILURE);
	}
	return 0;
}


void printValues(JournalRecord *record)
{
	if(record == NULL)
	{
		fprintf(stderr, "Invalid journal record\n");
		exit(EXIT_FAILURE);
	}
	int i,j;
	for(i=0; i<record->rowCount; i++)
	{
		for(j=0; j<(record->columnCount); j++)
		{
			printf("%ld\t", record->columnValues[i][j]);
		}
		printf(" | ");
		if(record->columnValues[i][record->columnCount] == 1)
		{
			printf("DEL");
		}
		else if(record->columnValues[i][record->columnCount] == 0)
		{
			printf("INS");
		}
		else
		{
			fprintf(stderr, "Invalid dirty bit\n");
			exit(EXIT_FAILURE);
		}			
		printf("\n");
	}
	fflush(stdout);
}

void deleteJournalRecord(JournalRecord* record)
{
	if(record == NULL)
	{
		fprintf(stderr, "Invalid journal record argument\n");
		exit(EXIT_FAILURE);
	}
	record->isDeleted = 1;
}

int isJournalRecordActive(JournalRecord *record)
{
	if(record == NULL)
	{
		fprintf(stderr, "Invalid journal record argument\n");
		exit(EXIT_FAILURE);
	}
	return !record->isDeleted;
}

void destroyJournalRecord(JournalRecord* journalRecord)
{
	int i;
	for(i=0; i<journalRecord->rowCount; i++)
	{
		free( (journalRecord->columnValues[i]) );
	}
	free(journalRecord->columnValues);
	free(journalRecord);
	//(*journalRecord) = NULL;
}
