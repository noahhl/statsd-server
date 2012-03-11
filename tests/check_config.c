#include <stdio.h>
#include <string.h>
#include "minunit.h"
#include "../src/config.h"

int tests_run = 0;
int assertions_run = 0;

static char * test_config_parsing() {
   statsdConfig *config = loadStatsdConfig("fixtures/config.yml");
   mu_assert("Redis host is extracted correctly", strcmp(config->redis_host, "localhost") == 0);
   mu_assert("Redis port is extracted correctly", config->redis_port ==  6379);
   mu_assert("db_host is extracted correctly", strcmp(config->db_path, "tmp/statsd/") == 0);
   mu_assert("retentions are extracted correctly", strcmp(config->retention, "10:2160,60:10080,600:262974") == 0);
   return 0;
}
static char * test_legacy_config() {
   statsdConfig *config = loadStatsdConfig("fixtures/legacy_config.yml");
   mu_assert("db_host is extracted correctly", strcmp(config->db_path, "tmp/statsd/") == 0);
   return 0;
}

static char * all_tests() {
   mu_run_test(test_config_parsing);
   mu_run_test(test_legacy_config);
   return 0;
}


int main(int argc, char **argv) {
   printf("Running configuration tests.\n");
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