#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <errno.h>
#include "contrib/hiredis/hiredis.h"
#include "config.h"
#include "queue.h"
#include "diskstore.h"
#include "aggregate.h"


void handle_diskstore_job(char *job_spec, redisContext *redisInstance)
{
  Job *job = parse_queue_job(job_spec);
  if (strcmp("store!", job->type) == 0) {
    append_value_to_file(job->args[0], job->args[1]);
  } else if (strcmp("truncate!", job->type) == 0) {
    truncate_file(job->args[0], job->args[1]);
  } else if (strcmp("aggregate!", job->type) == 0) {
    
  } else {
      /* 
         Anything we aren't equipped to handle, stick back into
         the diskstoreQueue -- the ruby worker alone will touch that an
         handle it
      */
      printf("Passing on %s\n", job_spec);
      redisCommand(redisInstance, "LPUSH diskstoreQueue %s", job_spec);
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
  printf("Pinging redis...");
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