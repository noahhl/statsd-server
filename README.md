# This repository is no longer maintained. Please see [noahhl/batsd](https://github.com/noahhl/batsd) for an improved ruby implementation that is mostly compatible with this version.

##Upgrading to batsd

Batsd uses the same storage format for on-disk storage, but a slightly
different format for redis storage. The preferred way to migrate is:

  1) Install and configure batsd to use the same data path (called `root` in
  batsd).

  2) Stop statsd-server
  
  3) Execute `FLUSHALL` in the redis instance. You may wish to dump the
  `datapoints` set to a file and reload it afterwards if you rely on that being
  fully comprehensive.
  
  4) Start batsd.

  5) Switch to the batsd client.

This does mean some data will be lost at the lowest granularity. It will be
available at the next level of granularity that was stored to disk.

Alternately, you can mirror incoming statsd traffic to multiple ports running
both statsd-server and batsd, pointing at different Redis instances to build up
equivalent amounts of short term data in both.

You can accomplish this with `socat` -- the following will send UDP traffic from
port 8125 to both port 8135 and 8140, so you can run batsd and statsd-server
simultaneously.

    socat - udp4-listen:8125,fork | tee >(socat - udp-sendto:127.0.0.1:8135) >(socat - udp-sendto:127.0.0.1:8140)

