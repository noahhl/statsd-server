require 'array'
module StatsdServer
  class Aggregation


    # For a given interval, grab the list of keys that
    # need to be aggregated from the appropriate redis set.
    # Aggregate them across the interval, and store them to disk
    def self.aggregate_pending!(interval)
      StatsdServer.logger "Starting aggregation for #{interval} interval" if $options[:debug]
      $redis.smembers("needsAggregated:#{interval}") do |keys|
        timing = Benchmark.measure do
          keys.each do |key|
            aggregation = case key
                          when /min/ then "min"
                          when /max/ then "max"
                          when /mean_squared/ then "sum"
                          when /mean|upper_/ then "mean"
                          else "sum"
                          end

            self.new(key, interval, aggregation).store!
          end
        end
        StatsdServer.logger "Finished aggregation and writing to diskstore for #{interval} in #{timing.real} seconds" if $options[:debug]
      end
    end

    def initialize(key, interval, aggregation)
      @now = Time.now.to_i
      @key = key
      @since = @now - interval
      @interval = interval
      @aggregation = aggregation
    end

    def store!
      begin
        key_time = normalize_time(@now, @interval)
        calculate_aggregation do |aggregation|
          StatsdServer::Diskstore.enqueue "store!", "#{@key}:#{@interval}", key_time.to_s, aggregation.to_s
          $redis.srem("needsAggregated:#{@interval}", @key)
        end
      rescue Exception => e
        StatsdServer.logger "Encountered an error trying to store #{@key}:#{@interval}: #{e}"
      end
    end
    
    def calculate_aggregation(values=nil, &f)
      if values.nil?
        values_from_redis do |values|
          return unless values.is_a? Array
          if EventMachine.reactor_running?
            values.send(@aggregation).tap(&f)
          else
            return values.send(@aggregation).tap(&f)
          end
        end
      else
        return unless values.is_a? Array
        return values.send(@aggregation)
      end
    end

      def normalize_time(t, step)
        t = t.to_i
        t - (t % step)
      end

      def values_from_redis(&f)
        end_time = normalize_time(@now, $config["flush_interval"]) + 1
        start_time = end_time - (@interval+1)
        $redis.zrangebyscore(@key, start_time, end_time) do |keys|
          keys.map{|key| decode_record(key) rescue nil}.compact.tap(&f)
        end
      end
    private
      
      def decode_record(key)
        s = key.split("\x01")
        tsdecode(s[1]).to_f
      end
      
      def tsdecode(data)
        if data[0..0] == 'E'
            Base64.decode64(data[1..-1])
        else
            data[1..-1]
        end
      end

  end
end
