#ifndef CONFIG_H
#define CONFIG_H

typedef struct statsdConfig {
  char *redis_host;
  int redis_port;
  char *retention;
  char *db_path;
} statsdConfig;
statsdConfig *loadStatsdConfig(char *path);

#endif