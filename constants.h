#ifndef _CONSTANTS_H_
#define _CONSTANTS_H_

#include <math.h>
#include <stdint.h>

#define BUCKETCAPACITY 1
#define JOURNALPOINTER 2
#define INITIALGLOBALDEPTH 1
#define RANGECAPACITY 128
#define TRUE 1
#define FALSE 0

#define OP_INSERT 0
#define OP_DELETE 1

#define MULTI_THREADED_MODE 1
#define THREADS_NO 2
#define FLUSH_ROUND 3

typedef struct
{
	uint64_t transactionId;
	uint64_t *journalptr[JOURNALPOINTER];
}RangeArray;

typedef struct
{
	uint64_t primaryKey;
	int numOfTrans;
	RangeArray *transArray;
}element;

/*	The bucket structure 	*/
typedef struct
{
	int localDepth;
	int numOfElems;
	element *elementArr[BUCKETCAPACITY];
}Hash;

/*	A structure to keep extendible hash informations	*/
typedef struct
{
	int tableSize;
	int globalDepth;
	Hash **hashptr;
}extendibleHash;


typedef struct ListNode
{
	uint64_t *journalptr;
	struct ListNode *next;
}ListNode_t;


typedef struct
{
	ListNode_t *start;
	ListNode_t *end;
}ListRange;


//Represents a Journal node, stores the transaction id and a pointer
//to an array containing the column values for the relation
typedef struct
{
	uint64_t transactionId;
	uint64_t **columnValues;
	int rowCount;
	int columnCount;
	char isDeleted;
}JournalRecord;


//The Journal will be a linked list. New nodes will only be inserted at the end of the list
typedef struct
{
	JournalRecord** records;
	int columnCount;	//The number of columns in the relation
	int recordCount;	//The number of transaction records stored in the journal
	int arraySize;		//The current size of the array (including empty positions)
}Journal;

/// Message types
typedef enum { Done, DefineSchema, Transaction, ValidationQueries, Flush, Forget } Type_t;

/// Support operations
typedef enum { Equal, NotEqual, Less, LessOrEqual, Greater, GreaterOrEqual } Op_t;

//---------------------------------------------------------------------------
typedef struct MessageHead {
		/// Total message length, excluding this head
		uint32_t messageLen;
		/// The message type
		Type_t type;
} MessageHead_t;

//---------------------------------------------------------------------------
typedef struct DefineSchema {
		/// Number of relations
		uint32_t relationCount;
		/// Column counts per relation, one count per relation. The first column is always the primary key
		uint32_t columnCounts[];
} DefineSchema_t;

//---------------------------------------------------------------------------
typedef struct Transaction {
		/// The transaction id. Monotonic increasing
		uint64_t transactionId;
		/// The operation counts
		uint32_t deleteCount,insertCount;
		/// A sequence of transaction operations. Deletes first, total deleteCount+insertCount operations
		char operations[];
} Transaction_t;

//---------------------------------------------------------------------------
typedef struct TransactionOperationDelete {
		/// The affected relation
		uint32_t relationId;
		/// The row count
		uint32_t rowCount;
		/// The deleted values, rowCount primary keyss
		uint64_t keys[];
} TransactionOperationDelete_t;

//---------------------------------------------------------------------------
typedef struct TransactionOperationInsert {
		/// The affected relation
		uint32_t relationId;
		/// The row count
		uint32_t rowCount;
		/// The inserted values, rowCount*relation[relationId].columnCount values
		uint64_t values[];
} TransactionOperationInsert_t;

//---------------------------------------------------------------------------
typedef struct ValidationQueries {
		/// The validation id. Monotonic increasing
		uint64_t validationId;
		/// The transaction range
		uint64_t from,to;
		/// The query count
		uint32_t queryCount;
		/// The queries
		char queries[];
} ValidationQueries_t;

//---------------------------------------------------------------------------
typedef struct Column {
	/// The column id
	uint32_t column;
	/// The operations
	Op_t op;
	/// The constant
	uint64_t value;
} Column_t;

//---------------------------------------------------------------------------
typedef struct Query {

		/// The relation
		uint32_t relationId;
		/// The number of bound columns
		uint32_t columnCount;
		/// The bindings
		Column_t columns[];
} Query_t;

//---------------------------------------------------------------------------
typedef struct Flush {
		/// All validations to this id (including) must be answered
		uint64_t validationId;
} Flush_t;

//---------------------------------------------------------------------------
typedef struct Forget {
		/// Transactions older than that (including) will not be tested for
		uint64_t transactionId;
} Forget_t;

//----------------------------------------------------------------------------
typedef struct validationListNode
{
	ValidationQueries_t *validation;
	int result;
	struct validationListNode *next;
}validationListNode_t;

//---------------------------------------------------------------------------
typedef struct validationList
{
	validationListNode_t *start;
	validationListNode_t *currentValidation;
	validationListNode_t *end;
}validationList_t;

typedef struct ThreadListNode
{
	validationListNode_t *val;
	struct ThreadListNode* next;
}ThreadListNode;

typedef struct ThreadList
{
	ThreadListNode* head;
}ThreadList;

typedef struct ValidationThread
{
	int threadNo;
	ThreadList *validationList;
}ValidationThread;

#endif