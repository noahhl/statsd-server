#!/usr/bin/env ruby
require 'socket'
require 'redis'

#Sends N gauge measurements and times how long it takes for them to be written to disk
@client = UDPSocket.new
@client.connect "127.0.0.1", 8125
@redis = Redis.new

`rm -rf tmp/statsd/*`

n = (ARGV[0] || 1000).to_i

n.times { @client.send "test#{(rand*100).to_i}:#{rand}|g", 0 }
while len = @redis.llen("gaugeQueue") == 0
end
start_ts = Time.now.to_f
while len = @redis.llen("gaugeQueue") > 0
end
end_ts = Time.now.to_f
puts "Elapsed: #{end_ts - start_ts}"