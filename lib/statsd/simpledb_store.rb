require 'simpledb-timeseries'

module Statsd

  class SimpleDBStore 
    class << self
      attr_accessor :timestep

      def store(key, value, sdb)
        if sdb
          begin
            SimpleDBTimeSeries.new(prefix = "#{key}", timestep = self.timestep, sdb).add(value.to_s) 
          rescue 
            nil
          end
        end
      end


      def flush_stats(counters, timers)
       
        print "#{Time.now} Flushing #{counters.count} counters and #{timers.count} timers to SimpleDB\n"
        @simpledb ||= AwsSdb::Service.new rescue nil
        num_stats = 0
        
        #store counters
        counters.each_pair do |key, value|
            store("counters:#{key}", value, @simpledb)
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
            
            store("timers:#{key}:mean", mean.to_s, @simpledb)
            store("timers:#{key}:max", max.to_s, @simpledb)
            store("timers:#{key}:min", min.to_s, @simpledb)
            store("timers:#{key}:upper_#{pct_threshold}", max_at_threshold.to_s, @simpledb)
            store("timers:#{key}:count", count.to_s, @simpledb)
            
            num_stats += 1
          end
        end



      end
      
    end

  end
end
