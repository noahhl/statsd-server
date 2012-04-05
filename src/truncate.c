#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <errno.h>
#include <time.h>
#include "contrib/hiredis/hiredis.h"
#include "config.h"
#include "diskstore.h"

int calculate_truncation_threshold(char *aggregation, char *retentions)
{
  time_t now;
  now = time (NULL);
  char *end_str;
  char *token = strtok_r(retentions, ",", &end_str);
  while (token != NULL) {
    char *count;
    char *period = strtok_r(token, ":", &count);
    if(strcmp(aggregation, period) == 0) {
      return (now - atoi(period) * atoi(count));
    }
    token = strtok_r(NULL, ",", &end_str);
  }
  return 0;
}


int main(int argc, char *argv[])
{

  printf("Booting up...\n");
  if( argc != 3 || fopen(argv[1], "r") == NULL) {
    printf("Usage: truncate path/to/config/file aggregationLevel\n");
    printf("Exiting...\n");
    return 1;
  }

  statsdConfig *config = loadStatsdConfig(argv[1]);
  redisReply *reply;
  redisContext *redisInstance = redisConnect(config->redis_host, config->redis_port);
  if (redisInstance->err) {
    printf("Error: %s\n", redisInstance->errstr);
    exit(1);
  }

  int since = calculate_truncation_threshold(argv[2], config->retention);
  if (since == 0) {
    printf("Aggregation specified is invalid. Aborting...\n");
    return 1;
  }

  printf("Starting to truncate the %s level of aggregations since %d.\n", argv[2], since);

  /* PING server */
  printf("Pinging redis...");
  reply = redisCommand(redisInstance,"PING");
  printf("%s\n", reply->str);
  freeReplyObject(reply);

  reply = redisCommand(redisInstance,"SMEMBERS datapoints");
  if (reply->type == REDIS_REPLY_ARRAY) {
    int i;
    for (i=0; i < reply->elements; i++) {
      char metric[256];
      sprintf(metric, "%s:%s", reply->element[i]->str, argv[2]);
      char *filename = calculate_statsd_filename(metric, config->db_path);
      char timestamp[10];
      sprintf(timestamp, "%d", since);
      #ifdef DEBUG
        printf("truncating %s\n", filename);
      #endif
      truncate_file(filename, timestamp);
    }
  }
  freeReplyObject(reply);
  redisFree(redisInstance);
  return 0;
}