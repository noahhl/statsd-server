#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <errno.h>
#include "diskstore.h"

int main(int argc, char *argv[])
{

   char *hash = calculate_statsd_filename("foo", "/u/statsd/");
   printf("%s\n", hash);

  return 0;
}