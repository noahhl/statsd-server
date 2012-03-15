#include <stdio.h>
#include <string.h>
#include "minunit.h"
#include "../src/aggregate.h"
int tests_run = 0;
int assertions_run = 0;

static char * test_mean() {
  double values[] = {1.0, 2.0, 3.0, 4.0};
  mu_assert("Mean calculation is correct", aggregate_mean(values, 4) == 2.5);
  return 0;
}

static char * test_min() {
  return 0;
}

static char * test_max() {
  return 0;
}

static char * test_sum() {
  return 0;
}

static char * all_tests() {
  mu_run_test(test_min);
  mu_run_test(test_max);
  mu_run_test(test_mean);
  mu_run_test(test_sum);
  return 0;
}


int main(int argc, char **argv) {
   printf("Running aggregation tests.\n");
   time_t start, end;
   start = time(NULL);
   
   char *result = all_tests();
   if (result != 0) {
       printf("\nFAILED: %s\n", result);
   }

   end = time(NULL);
   printf("\nRan %d tests with %d assertions in %lld seconds.\n", tests_run, assertions_run, (long long int)(end-start));
   return result != 0;
}
