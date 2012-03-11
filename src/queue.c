#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <assert.h>
#include <errno.h>
#include "queue.h"

Job *parse_queue_job(char *job_spec)
{
  Job *job = malloc(sizeof(struct Job));
  assert( job != NULL );
  job->defined = 0;
  char *spec = strdup(job_spec);
  char *end_str;
  char *token = strtok_r(spec, "<X>", &end_str);
  int i = 0;
  job->nargs = 0;

  while (token != NULL) {
    if (i == 0) { 
      job->type = strdup(token);
      job->defined = 1;
    } else {
      job->args[job->nargs] = strdup(token);
      job->nargs++;
    }
    token = strtok_r(NULL, "<X>", &end_str);
    i++;
  }
  return job;

}