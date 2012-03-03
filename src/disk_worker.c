#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <errno.h>
#include "hiredis/hiredis.h"
#include "config.h"
#include "diskstore.h"


void handle_diskstore_job(char *job, redisContext *redisInstance)
{
  int i = 0;
  char *end_str, *job_type, *filename, *value;
  char *originalJob = job;
  char *token = strtok_r(job, "\x1", &end_str);

  while (token != NULL) {
    if (i == 0) { job_type = token; };
    if (i == 1) { filename = token; };
    if (i == 2) { value = token; };
    token = strtok_r(NULL, "\x1", &end_str);
    i++;
  }

  if (strcmp("store!", job_type) == 0) {
    append_value_to_file(filename, value);
  } else if (strcmp("truncate!", job_type) == 0) {
    truncate_file(filename, value);
  } else {
      /* 
         Anything we aren't equipped to handle, stick back into
         the diskstoreQueue -- the ruby worker alone will touch that an
         handle it
      */
      printf("Passing on %s\n", originalJob);
      redisCommand(redisInstance, "LPUSH diskstoreQueue %s", originalJob);
  }
}


int main(int argc, char *argv[])
{
  printf("Booting up...\n");
  if( argc != 2 || fopen(argv[1], "r") == NULL) {
    printf("Config file not specified or found. Exiting...\n");
    return 1;
  }

  statsdConfig *config = loadStatsdConfig(argv[1]);
  redisReply *reply;
  redisContext *redisInstance = redisConnect(config->redis_host, config->redis_port);
  if (redisInstance->err) {
    printf("Error: %s\n", redisInstance->errstr);
    exit(1);
  }

  /* PING server */
  printf("Pinging redis at...");
  reply = redisCommand(redisInstance,"PING");
  printf("%s\n", reply->str);
  freeReplyObject(reply);

  printf("Starting to process disk writing jobs...\n");
  while(1) {
    reply = redisCommand(redisInstance,"BRPOP gaugeQueue truncateQueue 30");
    if (reply->type == REDIS_REPLY_ARRAY) {
      if (reply->elements == 2) {
        handle_diskstore_job(reply->element[1]->str, redisInstance);
      }
    }
    freeReplyObject(reply);
  }
  
  return 0;
}