CFLAGS=-Wall -g

all: bin/disk_worker bin/truncate

bin/disk_worker: src/disk_worker.c src/config.h src/diskstore.h src/hiredis/libhiredis.so
	cc -static src/disk_worker.c src/md5.c src/config.c src/diskstore.c -o bin/disk_worker -Lsrc/hiredis -lhiredis

bin/truncate: src/truncate.c src/diskstore.c
	cc src/truncate.c src/md5.c src/diskstore.c -o bin/truncate

src/hiredis/libhiredis.so:
	cd src/hiredis && make

clean:
	rm -f bin/disk_worker
	make all