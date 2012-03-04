CFLAGS=-Wall -g -O1

all: config diskstore queue
	@rm *.test

config: check_config.c
	@cc check_config.c ../src/config.c -o config.test
	@./config.test

queue: check_queue.c
	@cc check_queue.c ../src/queue.c -o queue.test
	@./queue.test

diskstore: check_diskstore.c
	@cc check_diskstore.c ../src/diskstore.c ../src/contrib/md5.c -o diskstore.test
	@./diskstore.test