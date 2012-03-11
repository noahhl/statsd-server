#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <assert.h>
#include <signal.h>
#include <errno.h>
#include "config.h"

statsdConfig *loadStatsdConfig(char *path)
{
  statsdConfig *config = malloc(sizeof(struct statsdConfig));
  assert(config != NULL);
  FILE *config_file;
  config_file = fopen(path, "r");
  char line[256];
  while ( fgets ( line, sizeof line, config_file ) != NULL ) {
    if('#' != line[0] && '\n' != line[0] && '-' != line[0]) {
      char *value;
      char *key = strtok_r(line, ":", &value);
      value = strtok(value, "\n");
      // strip whitespace and quotes
      while(isspace(*value) || *value == '"') { value++; };
      char *end = value + strlen(value) - 1;
      while(end > value && (isspace(*end) || *end == '"')) end--;
      *(end+1) = 0;


      if( strcmp(key, "redis_host") == 0 ) { config->redis_host = strdup(value); }
      if( strcmp(key, "redis_port") == 0 ) {config->redis_port = atoi(value); };
      
      if( strcmp(key, "retention") == 0 ) {config->retention = strdup(value); };
      //support coalmine_data_path for legacy config files
      if( strcmp(key, "db_path") == 0 || strcmp(key, "coalmine_data_path") == 0 ) {config->db_path = strdup(value); };
      
    }
  }
  return config;
}
