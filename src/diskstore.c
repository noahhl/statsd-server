#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <errno.h>
#include "diskstore.h"
#include "md5.h"


char *calculate_statsd_filename(char *name, char *db_path)
{
  md5_state_t state;
  md5_byte_t digest[16];
  int i;
  md5_init(&state);
  md5_append(&state, (const md5_byte_t *)name, strlen(name));
  md5_finish(&state, digest);
  
  char hash[32];
  char tmp[2];
  int j = 0;
  for (i = 0; i < 16; i++) {
    sprintf(tmp, "%02x", digest[i]);
    if (i == 0 ) {
      strcpy(hash, tmp);
    } else {
      strcat(hash, tmp);
    }
  }

  char result[256];
  sprintf(result, "%s%c%c/%c%c/%s", db_path, hash[0], hash[1], hash[2], hash[3], hash);
  return strdup(result);
}

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
  char *existing_ts, line[256], newline[256];
  char tmp[256];
  strcpy(tmp, filename);
  strcat(tmp, ".tmp");
  if(tempfile = fopen(tmp, "r")) { 
    printf("Couldn't truncate %s before %s because a tempfile was already present.\n", filename, 
                                                                                    timestamp);
    return; 
  }

  if(file = fopen(filename, "r")) {
    tempfile = fopen(tmp, "w");
    while ( fgets ( line, sizeof line, file ) != NULL ) {
      strcpy(newline, line);
      existing_ts = strtok(line, " ");
      if(strcmp(existing_ts, timestamp) > 0) {
        fprintf(tempfile, "%s", newline);
      }
    }
    fclose(file);
    fclose(tempfile);
  }
  if(rename(tmp, filename) == -1) {
    printf("Error truncating %s: error %s\n", filename, strerror(errno));
  }
  unlink(tmp);
}
