#ifndef QUEUE_H
#define QUEUE_H


typedef struct Job {
  int defined;
  int nargs;
  char *type;
  char *args[];
} Job;

Job *parse_queue_job(char *job_spec);

#endif