#ifndef _JOURNAL_H_

#define _JOURNAL_H_

#include <stdint.h>

//Defines the initial size of the records array
#define INIT_SIZE 100000


//Creates and returns a pointer to a Journal
Journal *createJournal(int columnCount);

void destroyJournal(Journal *journal);

//Inserts a new journal record at the end of the current list
void insertJournalRecord(Journal *journal, JournalRecord *record);

//Increases the size of the journal by adding 'extensionSize' free positions
Journal* extendJournal(Journal* journal, int extensionSize);

//Searches the journal bottom to top for the most recent record with the given primary key
//and returns the values. Returns null if the primary key was not found
uint64_t* getMostRecentValues(Journal *journal, uint64_t primaryKey);

//Marks the journal record at the specified index as deleted
void deleteJournalEntry(Journal* journal, int index);

//Returns a list with all journal records with transaction id between the startRange and the endRange
int getJournalRecords(Journal *, uint64_t , uint64_t , uint32_t , Column_t* );

//Prints all the journal records
void printJournal(Journal* journal);

int BinarySearch( Journal *, uint64_t , int , int );

#endif // _JOURNAL_H_
