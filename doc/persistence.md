# Persistence

##### Some historical context
Etsy's original ("reference") implementation of statsd primarily served as a first level aggregation
and passthrough of data to Graphite (http://graphite.wikidot.com) and its associated Whisper storage
engine. In this case, it performs the first aggregation over the course of its flush interval
(typically 10 seconds in operation) and then passes those aggregated values to Graphite to store.
It does not directly provide a persistence model.

StatsdServer uses two datastores to store historical data.

+ Redis is used to store recent, near realtime data (as well as for job queueing). In most cases,
  this will be used for every-10-second data for the last 1-24 hours.
+ Longer history, aggregated data is stored in "flat" files on disk. This could be anywhere from a
  one week retention to a 5 year retention

##Configuration

The configuration of retention levels is very similar to Graphite, and is specified in the config
file:

    retention: "10:2160,60:10080,600:262974"

This is translated into three retention levels:
+ Data aggregated in 10 second intervals, stored for 2160 datapoints, for 6 hours of history.
+ Data aggregated into 60 second intervals, sotred for 10080 datapoints, for a week of history.
+ Data aggregated into 600 second intervals, stored for 262,974 datapoints, for five years of history.

The shortest-term aggregation is always stored in Redis; the latter ones always stored to disk. In
the future, this may be configurable.

##Redis persistency

Near term data stored in redis is stored in sorted sets, one per datapoint, with the Unix timestamp
as the score and a string structured as `#{now}\x01R#{value}` as the value.

A cleanup process is run occasionally to truncate the sorted sets using a ZREMRANGEBYSCORE. The
frequency of this cleanup is controlled by the cleanup_interval setting in the configuration file.

##Diskstore

Longer duration data is stored in flat files located within subdirectories of the db_path specified
in the configuration file.

The location of a file will be determined by the MD5 hash of `#{metric_name}:#{aggregationLevel}`.
Files are then stored two subdirectories deep using the first four characters of that hash.

For example, with a `db_path` of `/statsd`, the path for the 60 second aggregation of `test_metric`
will be `/statsd/88/b4/88b4ca597dfc2d67438cc26140b2615b`.

Data is written to these files in `timestamp value` format, with each measurement separated by a `\n`
newline character, in sequential order.