CFLAGS=-Wall -g

all: disk_worker

disk_worker: src/disk_worker.c src/hiredis/libhiredis.so
	cc src/disk_worker.c -o bin/disk_worker -lhiredis

src/hiredis/libhiredis.so:
	cd src/hiredis && make

clean:
	rm -f bin/disk_worker
	make all