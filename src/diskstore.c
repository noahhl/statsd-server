#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <errno.h>
#include "diskstore.h"
#include "contrib/md5.h"


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

char *calculate_statsd_directory_from_path(char *path)
{
  char *token, *end_str, dirpath[256];
  char *components[20];
  token = strdup(path);
  token = strtok_r(token, "/", &end_str);
  components[0] = token;
  int i = 1;
  while(token != NULL) {
    token = strtok_r(NULL, "/", &end_str);
    components[i] = token;
    i++;
  }
  strcpy(dirpath, components[0]);
  int j;
  for(j=1; j < (i-2); j++) {
    strcat(dirpath, "/");
    strcat(dirpath, components[j]);
  }
  strcat(dirpath, "/");
  return strdup(dirpath);
}

void mkdir_p(char *path)
{
  char *end_str, *token = strdup(path);

  token = strtok_r(token, "/", &end_str);
  char *targetdir = strdup(token);
  while(token != NULL) {
    printf("Creating %s\n", targetdir);
    mkdir(targetdir);
    token = strtok_r(NULL, "/", &end_str);
    strcat(targetdir, "/");
    strcat(targetdir, token);

  }
}

void append_value_to_file(char *filename, char *value)
{
  FILE *file;
  if(file = fopen(filename,"a+")) {
    fprintf(file, "%s\n", value);
    fclose(file); 
  } else {
    if (errno == 2) {
      char *dirpath = calculate_statsd_directory_from_path(filename);
      printf("Needed directory at %s, creating it.\n", dirpath);
      mkdir_p(dirpath);
      if(file = fopen(filename,"a+")) {
        fprintf(file, "%s\n", value);
        fclose(file); 
      } else {
        printf("Error appending to %s: error %s\n", filename, strerror(errno));  
      }
    
    }
    
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
