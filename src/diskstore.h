#ifndef DISKSTORE_H
#define DISKSTORE_H

char *calculate_statsd_filename(char *name, char *db_path);
char *calculate_statsd_directory_from_path(char *path);
void append_value_to_file(char *filename, char *value);
void truncate_file(char *filename, char *timestamp);

#endif