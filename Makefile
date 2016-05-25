OBJS 	= main.o utilities.o hash.o Journal.o JournalRecord.o thread.o
SOURCE	= main.c utilities.c hash.c Journal.c JournalRecord.c thread.c
OUT  	= project
CC	= gcc
FLAGS   = -g -c

all: $(OBJS)
	$(CC) -g $(OBJS) -o $(OUT) -lm -lpthread

main.o: main.c
	$(CC) $(FLAGS) main.c

utilities.o: utilities.c
	$(CC) $(FLAGS) utilities.c

hash.o: hash.c
	$(CC) $(FLAGS) hash.c

JournalRecord.o: JournalRecord.c
	$(CC) $(FLAGS) JournalRecord.c

Journal.o: Journal.c
	$(CC) $(FLAGS) Journal.c

thread.o: thread.c
	$(CC) $(FLAGS) thread.c

# clean object files of project
clean:
	rm -f $(OBJS) $(OUT)
