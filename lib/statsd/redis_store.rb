require 'benchmark'
require 'redis'
require 'statsd/redis-timeseries'

module Statsd

  class RedisStore
    class << self
      attr_accessor :redis, :host, :port, :flush_interval, :retentions
    end

    def self.cleanup
      datapoints = self.redis.smembers("datapoints")
      print "#{Time.now} Cleaning up #{datapoints.length} datapoints.\n" 
      datapoints.each do |datapoint|
        RedisTimeSeries.new(prefix = "#{key}", timestep = interval, self.redis).cleanup(self.retentions)
      end
    end

    def self.store_all_retentions(key, value, redis)
      main_interval = retentions[0].split(":")[0].to_i
      retentions.each_with_index do |retention, index|
        interval = retention.split(":")[0].to_i
        num_to_save = retention.split(":")[1].to_i
        expiration = num_to_save * interval
        
        if index == 0
          RedisTimeSeries.new(prefix = "#{key}", timestep = interval, redis).add(value.to_s)
        else
          aggregation = case key
                        when /min/ then "min"
                        when /max/ then "max"
                        when /mean|upper_/ then "mean"
                        else "sum"
                        end
          RedisTimeSeries.new(prefix = "#{key}", timestep = main_interval, redis).aggregate(interval, aggregation)
        end
      end
      
    end


    def self.flush_stats(counters, timers)
     
      print "#{Time.now} Flushing #{counters.count} counters and #{timers.count} timers to Redis\n"
      self.redis ||= Redis.new(:host => host, :port => port)
      num_stats = 0
      
      
      timestep = flush_interval 
      
      #store counters
      counters.each_pair do |key, value|
          store_all_retentions("counters:#{key}", value, self.redis)
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
          
          store_all_retentions("timers:#{key}:mean", mean.to_s, self.redis)
          store_all_retentions("timers:#{key}:max", max.to_s, self.redis)
          store_all_retentions("timers:#{key}:min", min.to_s, self.redis)
          store_all_retentions("timers:#{key}:upper_#{pct_threshold}", max_at_threshold.to_s, self.redis)
          store_all_retentions("timers:#{key}:count", count.to_s, self.redis)
          
          num_stats += 1
        end
      end



    end

  end
end
