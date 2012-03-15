#include <stdio.h>
#include <math.h>
#include "contrib/hiredis/hiredis.h"
#include "aggregate.h"

double aggregate_mean(double values[], int n) {
  double value = 0.0;
  int i;
  for(i = 0; i < n; i++) {
    value += values[i];
  }
  return value / n;
}

double aggregate_min(double *values[]) {
  return 1.0;
}

double aggregate_max(double *values[]) {
  return 1.0;
}

double aggregate_sum(double *values[]) {
  return 1.0;
}

void perform_aggregation_for_metric(char *metric_name, int *begin_score, int *end_score, redisContext *redisInstance) {
  reply = redisCommand(redisInstance, "ZRANGEBYSCORE %s %s %s", metric_name, begin_score, end_score);

}