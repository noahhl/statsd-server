typedef struct statsdConfig {
  char *redis_host;
  int redis_port;
} statsdConfig;
statsdConfig *loadStatsdConfig(char *path);
