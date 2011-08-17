require 'benchmark'
require 'redis'
require 'statsd/redis-timeseries'

module Statsd

  class RedisStore
    class << self
      attr_accessor :host, :port, :flush_interval, :key_size, :retentions, :expirations
    end
    
    def self.prepare

      expirations = []
      retentions.each_with_index do |retention, i|
        expirations[i] = {:seconds => retention.split(":")[1].to_i, 
          :fraction => (1-(retention.split(":")[0].to_f/retentions[i+1].split(":")[0].to_i rescue 0)) * (1- (expirations[i-1][:fraction] rescue 0)) + (expirations[i-1][:fraction] rescue 0)
                         }
      end

      self.expirations = expirations

    end

    def self.flush_stats(counters, timers)
     
      print "#{Time.now} Flushing #{counters.count} counters and #{timers.count} timers to Redis\n"
      redis = Redis.new(:host => host, :port => port)
      num_stats = 0
      
      
      timestep = flush_interval 
      
      #store counters
      counters.each_pair do |key, value|
          RedisTimeSeries.new("statsd:#{key}", timestep, redis, expirations.find{|e| e[:fraction] >= rand}[:seconds]*flush_interval).add(value.to_s)
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
          
          expire = expirations.find{|e| e[:fraction] >= rand}[:seconds]
          RedisTimeSeries.new("statsd_timers:#{key}:mean", timestep, redis, expire*flush_interval).add(mean.to_s)
          RedisTimeSeries.new("statsd_timers:#{key}:max", timestep, redis, expire*flush_interval ).add(max.to_s)
          RedisTimeSeries.new("statsd_timers:#{key}:min", timestep, redis, expire*flush_interval).add(min.to_s)
          RedisTimeSeries.new("statsd_timers:#{key}:upper_#{pct_threshold}", timestep, redis, expire*flush_interval).add(max_at_threshold.to_s)
          RedisTimeSeries.new("statsd_timers:#{key}:count", timestep, redis, expire*flush_interval).add(count.to_s)
          num_stats += 1
        end
      end



    end

  end
end
