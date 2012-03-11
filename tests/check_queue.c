#include <stdio.h>
#include <string.h>
#include "minunit.h"
#include "../src/queue.h"

int tests_run = 0;
int assertions_run = 0;

static char * test_parsing_a_store_job() {
   char *job_spec = "store!<X>myfile<X>myvalue";
   Job *job = parse_queue_job(job_spec);
   mu_assert("store! job is created", job->defined == 1);
   mu_assert("store! job type is extracted correctly", strcmp(job->type, "store!") == 0);
   mu_assert("store! returns two arguments", job->nargs == 2);
   mu_assert("store! first argument is the filename", strcmp(job->args[0], "myfile") == 0);
   mu_assert("store! second argument is the value", strcmp(job->args[1], "myvalue") == 0);
   return 0;
}

static char * test_parsing_a_truncate_job() {
   char *job_spec = "truncate!<X>myfile<X>since";
   Job *job = parse_queue_job(job_spec);
   mu_assert("truncate! job is created", job->defined == 1);
   mu_assert("truncate! job type is extracted correctly", strcmp(job->type, "truncate!") == 0);
   mu_assert("truncate! returns two arguments", job->nargs == 2);
   mu_assert("truncate! first argument is the filename", strcmp(job->args[0], "myfile") == 0);
   mu_assert("truncate! second argument is the since", strcmp(job->args[1], "since") == 0);
   return 0;
}

static char * test_parsing_an_aggregation_job() {
   char *job_spec = "aggregate!<X>123456<X>60<X>metric<X>mean";   
   Job *job = parse_queue_job(job_spec);
   mu_assert("aggregation job is created", job->defined == 1);
   mu_assert("aggregation job type is extracted correctly", strcmp(job->type, "aggregate!") == 0);
   mu_assert("aggregate returns four arguments", job->nargs == 4);
   mu_assert("aggregate! first argument is the time", strcmp(job->args[0], "123456") == 0);
   mu_assert("aggregate! second argument is the interval", strcmp(job->args[1], "60") == 0);
   mu_assert("aggregate! third argument is the metric name", strcmp(job->args[2], "metric") == 0);
   mu_assert("aggregate! fourth argument is the type of aggregation", strcmp(job->args[3], "mean") == 0);
   return 0;
}

static char * all_tests() {
   mu_run_test(test_parsing_a_store_job);
   mu_run_test(test_parsing_a_truncate_job);
   mu_run_test(test_parsing_an_aggregation_job);
   return 0;
}

int main(int argc, char **argv) {
   printf("Running queue tests.\n");
   time_t start, end;
   start = time(NULL);
   char *result = all_tests();
   if (result != 0) {
       printf("\nFAILED: %s\n", result);
   }
   end = time(NULL);
   printf("\nRan %d tests with %d assertions in %lld seconds.\n", tests_run, assertions_run, (long long int)(end-start));
   return result != 0;
   return 0;
}