require 'benchmark'
require 'redis'
require 'statsd/redis-timeseries'

module Statsd

  class RedisStore
    class << self
      attr_accessor :host, :port, :flush_interval, :key_size
    end


    def self.flush_stats(counters, timers)
     
      print "#{Time.now} Flushing #{counters.count} counters and #{timers.count} timers to Redis\n"
      redis = Redis.new(:host => host, :port => port)
      num_stats = 0
      timestep = key_size * flush_interval 
      
      
      #store counters
      counters.each_pair do |key, value|
          ts = RedisTimeSeries.new("statsd:#{key}", timestep, redis)
          ts.add(value.to_s)
          num_stats += 1
      end
   
      timers.each_pair do |key, values|
        if (values.length > 0) 
          pct_threshold = 90
          values.sort!
          count = values.count
          min = values.first
          max = values.last

          mean = min
          max_at_threshold = max

          if (count > 1)
            # strip off the top 100-threshold
            threshold_index = (((100 - pct_threshold) / 100.0) * count).round
            values = values[0..-threshold_index]
            max_at_threshold = values.last

            # average the remaining timings
            sum = values.inject( 0 ) { |s,x| s+x }
            mean = sum / values.count
          end

          # Flush Values to Store
          RedisTimeSeries.new("statsd_timers:#{key}:mean", timestep, redis).add(mean.to_s)
          RedisTimeSeries.new("statsd_timers:#{key}:max", timestep, redis).add(max.to_s)
          RedisTimeSeries.new("statsd_timers:#{key}:min", timestep, redis).add(min.to_s)
          RedisTimeSeries.new("statsd_timers:#{key}:upper_#{pct_threshold}", timestep, redis).add(max_at_threshold.to_s)
          RedisTimeSeries.new("statsd_timers:#{key}:count", timestep, redis).add(count.to_s)
          num_stats += 1
        end
      end



    end


  end
end
