CFLAGS=-Wall -g

all: bin/disk_worker bin/truncate

debug: CFLAGS+=-DDEBUG
debug: all

bin/disk_worker: src/disk_worker.c src/config.c src/diskstore.c src/queue.c src/contrib/hiredis/libhiredis.so
	cc $(CFLAGS) -static src/disk_worker.c src/contrib/md5.c src/config.c src/diskstore.c src/queue.c -o bin/disk_worker -Lsrc/contrib/hiredis -lhiredis

bin/truncate: src/truncate.c src/diskstore.c src/config.c src/contrib/hiredis/libhiredis.so
	cc $(CFLAGS) -static src/truncate.c src/contrib/md5.c src/diskstore.c src/config.c -o bin/truncate -Lsrc/contrib/hiredis -lhiredis

src/contrib/hiredis/libhiredis.so:
	cd src/contrib/hiredis && make

clean:
	rm -f bin/disk_worker
	rm -f bin/truncate
	make all

check:
	@cd tests && make -s