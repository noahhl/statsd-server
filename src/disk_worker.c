#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <time.h>
#include "hiredis/hiredis.h"


void append_value_to_file(char *filename, char *value)
{
  FILE *file;
  if(file = fopen(filename,"a+")) {   
    fprintf(file, "%s\n", value);
    fclose(file); 
  }
}

void truncate_file(char *filename, char *timestamp)
{
  printf("%s %s\n", filename, timestamp);
}

void handle_diskstore_job(char *job)
{
  int i = 0;
  char *end_str, *job_type, *filename, *value;
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

  }
}

void log_message(char *message)
{
  time_t seconds = time ( NULL);
  printf ( "%ld: %s\n", seconds, message );
}

int main(int argc, char *argv[])
{
  log_message("Booting up...");
  
  redisReply *reply;
  redisContext *c = redisConnect("127.0.0.1", 6379);
  if (c->err) {
    printf("Error: %s\n", c->errstr);
    exit(1);
  }

  /* PING server */
  log_message("Pinging redis...");
  reply = redisCommand(c,"PING");
  log_message(reply->str);
  freeReplyObject(reply);

  log_message("Starting to process disk writing jobs...");
  while(1) {
    reply = redisCommand(c,"BRPOP gaugeQueue 30");
    if (reply->type == REDIS_REPLY_ARRAY) {
      if (reply->elements == 2) {
        handle_diskstore_job(reply->element[1]->str);
      }
    }
    freeReplyObject(reply);
  }
  
  return 0;
}