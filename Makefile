CC=g++

CFLAGS=-Wall -g -std=c++0x -pthread -lpthread

all:
	$(CC) $(CFLAGS) process.cpp -o process

clean:
	rm -rf *o process
