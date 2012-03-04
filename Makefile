CFLAGS=-Wall -g -O1

all: bin/disk_worker bin/truncate

bin/disk_worker: src/disk_worker.c src/config.c src/diskstore.c src/contrib/hiredis/libhiredis.so
	cc -static src/disk_worker.c src/contrib/md5.c src/config.c src/diskstore.c -o bin/disk_worker -Lsrc/contrib/hiredis -lhiredis

bin/truncate: src/truncate.c src/diskstore.c src/config.c src/contrib/hiredis/libhiredis.so
	cc -static src/truncate.c src/contrib/md5.c src/diskstore.c src/config.c -o bin/truncate -Lsrc/contrib/hiredis -lhiredis

src/contrib/hiredis/libhiredis.so:
	cd src/contrib/hiredis && make

clean:
	rm -f bin/disk_worker
	make all

check:
	@cd tests && make -s