#ifndef _JOURNAL_RECORD_H_
#define _JOURNAL_RECORD_H_

#include <stdint.h>


//Creates a new Journal record with 'rowCount' rows and 'columnCount' columns
JournalRecord *createJournalRecord(int rows, int columnCount);

//Inserts 'values' in the 'rowIndex' row. Returns -1 on error
int insertColumnValues(JournalRecord *record, int rowIndex, uint64_t* values, char opType);

//Prints the columnValues matrix
void printValues(JournalRecord *record);

//Marks the journal record as deleted
void deleteJournalRecord(JournalRecord *record);

//Checks the record's dirty bit. Returns 1 if the record is active and 0 if it is deleted
int isJournalRecordActive(JournalRecord *record);

//Completely removes the journal record from memory
void destroyJournalRecord(JournalRecord* journalRecord);


#endif // _JOURNAL_RECORD_H_
