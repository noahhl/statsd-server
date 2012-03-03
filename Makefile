CFLAGS=-Wall -g

all: bin/disk_worker

bin/disk_worker: src/disk_worker.c src/config.h src/hiredis/libhiredis.so
	cc src/disk_worker.c src/config.c -o bin/disk_worker -lhiredis

src/hiredis/libhiredis.so:
	cd src/hiredis && make

clean:
	rm -f bin/disk_worker
	make all