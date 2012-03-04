#include <time.h>
 /* file: minunit.h */
 #define mu_assert(message, test) do { printf("."); assertions_run++; if (!(test)) return message; } while (0)
 #define mu_run_test(test) do {char *message = test(); tests_run++; \
                                if (message) return message; } while (0)
 extern int tests_run;
 extern int assertions_run;
