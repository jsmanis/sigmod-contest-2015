#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include "constants.h"
#include "thread.h"




ThreadList* createThreadList()
{
	ThreadList* list = malloc(sizeof(ThreadList));
	if(list == NULL)
	{
		perror("malloc");
	}
	list->head = NULL;
	return list;
}

void destroyThreadList(ThreadList **list)
{
	ThreadList* l = *list;
	ThreadListNode *current = l->head;
	while(current != NULL)
	{
		ThreadListNode* temp = current;
		current = current->next;
		free(temp);
	}
	free(*list);
	*list = NULL;
}

void pushThreadListNode(ThreadList *list, validationListNode_t* val)
{
	ThreadListNode* node = malloc(sizeof(ThreadListNode));
	if(node == NULL)
	{
		perror("malloc");
	}
	node->next = NULL;
	node->val = val;
	if(list->head == NULL)
	{
		list->head = node;
	}
	else
	{
		ThreadListNode* current = list->head;
		while(current->next != NULL)
		{
			current = current->next;
		}
		current->next = node;
	}
}


void printThreadList(ThreadList* list)
{
	if(list->head == NULL)
	{
		printf("The list is empty\n");
	}
	ThreadListNode* current = list->head;
	while(current != NULL)
	{
		printf("Node\n");
		current = current->next;
	}
}

//Distributes validations to threads. Returns an array of 'numOfThreads' pointers to ValidationThread arguments
ValidationThread** distributeValidations(validationListNode_t* start, uint64_t endId, int numOfThreads, validationListNode_t** newCurrent)
{
	
	ValidationThread** args = initializeThreadArgs(numOfThreads);
	ValidationQueries_t* v;
	if( start != NULL )
		v = start->validation;
	else 
		return NULL;
	validationListNode_t* current = start;
	int currentThread = 0;
	validationListNode_t* prev = start;
	while(v->validationId <= endId)
	{
		
		pushThreadListNode(args[currentThread]->validationList,current);
		//Hold the previous validation
		prev = current;
		//Move to the next validation
		current = current->next;
		//If the list ends before reaching the given thread id, stop the loop
		if(current == NULL)
		{
			*newCurrent = prev;
			break;
		}
		//Get the new current validation
		v = current->validation;
		//Go to the next thread
		currentThread = (currentThread + 1) % numOfThreads;
	}
	if(current != NULL)
	{
		*newCurrent = current;
	}
	return args;
}

//Initializes the arguments structure for each thread and returns them in an array of k pointers
ValidationThread** initializeThreadArgs(int k)
{
	ValidationThread** args = malloc(k*sizeof(ValidationThread*));
	if(args == NULL)
	{
		perror("malloc:");
		exit(EXIT_FAILURE);
	}
	int i=0;
	//Loop over the array of ValidationThreads to allocate memory for each individual thread argument
	for(i=0; i<k; i++)
	{
		args[i] = malloc(sizeof(ValidationThread));
		if(args[i] == NULL)
		{
			perror("malloc");
			exit(EXIT_FAILURE);
		}
		//Set a number to each argument to distinguish each thread
		args[i]->threadNo = i+1;
		//Initialize the list of validations that will be computed by each thread
		args[i]->validationList = createThreadList();
	}
	return args;
}

