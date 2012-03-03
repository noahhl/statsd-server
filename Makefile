CFLAGS=-Wall -g -O1

all: bin/disk_worker bin/truncate

bin/disk_worker: src/disk_worker.c src/config.c src/diskstore.c src/hiredis/libhiredis.so
	cc -static src/disk_worker.c src/md5.c src/config.c src/diskstore.c -o bin/disk_worker -Lsrc/hiredis -lhiredis

bin/truncate: src/truncate.c src/diskstore.c src/config.c src/hiredis/libhiredis.so
	cc -static src/truncate.c src/md5.c src/diskstore.c src/config.c -o bin/truncate -Lsrc/hiredis -lhiredis

src/hiredis/libhiredis.so:
	cd src/hiredis && make

clean:
	rm -f bin/disk_worker
	make all