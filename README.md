StatsD
======

A network daemon for aggregating statistics (counters and timers), rolling them up, then sending them to a hybrid redis + disk store. 


### Installation

    git clone git://github.com/noahhl/statsd-server && cd statsd-server && gem build statsd-server && sudo gem
    install

### Configuration

Create config.yml to your liking. Data at the first retention level is stored
in redis; further data retentions are stored on disk

Example config.yml
    ---
    bind: 127.0.0.1
    port: 8125

    # Flush interval should be your finest retention in seconds
    flush_interval: 10
    # Cleanup interval is how frequently sets will be checked for 
    # expiration. This is a tradeoff of CPU usage vs. memory
    cleanup_interval: 30

    # Redis
    redis_host: localhost 
    redis_port: 6379
    # Uses the same format as graphite's schema retentions, accomplished by
    # storing aggregated keys at each retention level. 
    # "10:2160,60:10080,600:262974" translates to:
    # 6 hours of 10 second data (what we consider "near-realtime")
    # 1 week of 1 minute data
    # 5 years of 10 minute data
    retention: "10:2160,60:10080,600:262974"
    coalmine_data_path: "test/data/"


### Server
Run the server:

    statsd -c config.yml 

Debug mode
    statsd -c config.yml -d

Counting
--------

    gorets:1|c

This is a simple counter. Add 1 to the "gorets" bucket. It stays in memory until the flush interval.


Timing
------

    glork:320|ms

The glork took 320ms to complete this time. StatsD figures out 90th percentile, average (mean), mean squared error, lower and upper bounds for the flush interval.

Sampling
--------

    gorets:1|c|@0.1

Tells StatsD that this counter is being sent sampled ever 1/10th of the time.

