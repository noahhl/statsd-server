#include <stdio.h>
#include "minunit.h"
#include "../src/diskstore.h"

int tests_run = 0;
int assertions_run = 0;

static char * test_filename_calculation() {
   char *expected = "tmp/statsd/8b/c9/8bc944dbd052ef51652e70a5104492e3";
   char *actual = calculate_statsd_filename("testfile", "tmp/statsd/");
   mu_assert("Filename is correctly calculated", strcmp(expected, actual) == 0);
   return 0;
}

static char * test_appending() {
   unlink("/tmp/test");
   append_value_to_file("/tmp/test", "123456 10");
   FILE *file = fopen("/tmp/test", "r");
   char line[256];
   fgets(line, sizeof line, file);
   mu_assert("Appending writes the correct value to the file specified with an added newline.", 
            strcmp(line, "123456 10\n") == 0);
   return 0;
}

static char * test_truncating() {
   int i;
   FILE *file = fopen("/tmp/test", "w");
   for(i = 0; i < 10; i++) {
      fprintf(file, "%d 123456\n", 100000 + i * 30);
   }
   fclose(file);
   truncate_file("/tmp/test", "100120");
   file = fopen("/tmp/test", "r");
   char line[256];
   fgets(line, sizeof line, file);
   mu_assert("Truncation leaves the first timestamp greater in the file.", 
            strcmp(line, "100150 123456\n") == 0);  
   return 0;
}

static char * all_tests() {
   mu_run_test(test_filename_calculation);
   mu_run_test(test_appending);
   mu_run_test(test_truncating);
   return 0;
}


int main(int argc, char **argv) {
   printf("Running diskstore tests.\n");
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