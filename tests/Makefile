CFLAGS=-Wall -g

all: config diskstore queue
	@rm *.test

debug: CFLAGS+=-DDEBUG
debug: all

config: check_config.c
	@cc $(CFLAGS) check_config.c ../src/config.c -o config.test
	@./config.test

queue: check_queue.c
	@cc $(CFLAGS) check_queue.c ../src/queue.c -o queue.test
	@./queue.test

diskstore: check_diskstore.c
	@cc $(CFLAGS) check_diskstore.c ../src/diskstore.c ../src/contrib/md5.c -o diskstore.test
	@./diskstore.test