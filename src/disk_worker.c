#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
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
  FILE *file, *tempfile;
  char *existing_ts, line[256], *newline, tmp[256];
  strcpy(tmp, filename);
  strcat(tmp, ".tmp");
  if(tempfile = fopen(tmp, "r")) { 
    printf("Couldn't truncate %s before %s because a tempfile was already present.", filename, 
                                                                                    timestamp);
    return; 
  }

  if(file = fopen(filename, "r")) {
    tempfile = fopen(tmp, "w");
    while ( fgets ( line, sizeof line, file ) != NULL ) {
      newline = line;
      existing_ts = strtok(line, " ");
      if(strcmp(existing_ts, timestamp) > 0) {
        fprintf(tempfile, "%s", newline);
      }
    }
    fclose(file);
    fclose(tempfile);
  }
  if(rename(tmp, filename)) {
    printf("Error truncating %s\n", filename);
  }
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


int main(int argc, char *argv[])
{
  printf("Booting up...\n");
  redisReply *reply;
  redisContext *c = redisConnect("127.0.0.1", 6379);
  if (c->err) {
    printf("Error: %s\n", c->errstr);
    exit(1);
  }

  /* PING server */
  printf("Pinging redis...");
  reply = redisCommand(c,"PING");
  printf("%s\n", reply->str);
  freeReplyObject(reply);

  printf("Starting to process disk writing jobs...\n");
  while(1) {
    reply = redisCommand(c,"BRPOP gaugeQueue truncateQueue 30");
    if (reply->type == REDIS_REPLY_ARRAY) {
      if (reply->elements == 2) {
        handle_diskstore_job(reply->element[1]->str);
      }
    }
    freeReplyObject(reply);
  }
  
  return 0;
}