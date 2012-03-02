CFLAGS=-Wall -g

all: disk_worker

disk_worker: src/disk_worker.c
	cc src/disk_worker.c -o bin/disk_worker -lhiredis
clean:
	rm -f bin/disk_worker
	make all