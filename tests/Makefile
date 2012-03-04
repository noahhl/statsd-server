CFLAGS=-Wall -g -O1

all: config
	@rm *.test

config: check_config.c
	@cc check_config.c -o config.test
	@./config.test