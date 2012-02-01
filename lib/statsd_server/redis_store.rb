module StatsdServer
  class RedisStore
    class << self
      
      def cleanup!
        $redis.smembers("datapoints") do |datapoints|
          timing = Benchmark.measure do 
            StatsdServer.logger "Cleaning up #{datapoints.length} datapoints from redis." if $options[:debug]
            retention = $config['retention'][0]
            since = Time.now.to_i - (retention[:interval] * retention[:count])
            datapoints.each do |datapoint|
              StatsdServer.logger "Clearing #{datapoint} from redis since #{since}." if $options[:debug]
              $redis.zremrangebyscore datapoint, 0, since
            end
          end
          StatsdServer.logger "Finished truncating redis in #{timing.real} seconds" if $options[:debug]
        end
      end

      def flush!(counters, gauges, timers)
        StatsdServer.logger "Flushing #{counters.count} counters, #{gauges.count} gauges and #{timers.count} timers to Redis\n"
        @now = Time.now.to_i
        
        #store counters
        counters.each_pair do |key, value|
          store_all_retentions "counters:#{key}", value
          $num_stats += 1
        end

        gauges.each_pair do |key, value|
          $redis.sadd "datapoints", "gauges:#{key}"
          value.each do |v|
            StatsdServer::Diskstore.enqueue("store!", "gauges:#{key}", v[0], v[1])
          end
        end
     
        timers.each_pair do |key, values|
          if (values.length > 0) 
            pct_threshold = 90
            # Flush Values to Store
            store_all_retentions "timers:#{key}:mean", values.mean.to_s
            store_all_retentions "timers:#{key}:max", values.max.to_s
            store_all_retentions "timers:#{key}:min", values.min.to_s
            store_all_retentions "timers:#{key}:upper_#{pct_threshold}", values.send("percentile_#{pct_threshold}").to_s
            store_all_retentions "timers:#{key}:count", values.count.to_s
            store_all_retentions "timers:#{key}:mean_squared", values.mean_squared.to_s
            $num_stats += 1
          end
        end
      end
      
      def compute_value_for_key(data, now)
        data = tsencode(data)
        "#{now}\x01#{data}"
      end

      private

        def store_all_retentions(key, value)
          $config["retention"].each_with_index do |retention, index|
            if index.zero? 
              $redis.sadd "datapoints", key 
              $redis.zadd key, @now, compute_value_for_key(value.to_s, @now)
            else
              $redis.sadd("needsAggregated:#{retention[:interval]}", key)
            end
          end
        end

        def tsencode(data)
          if data.index("\x00") or data.index("\x01")
              "E#{Base64.encode64(data)}"
          else
              "R#{data}"
          end
        end
    end
  end
end
