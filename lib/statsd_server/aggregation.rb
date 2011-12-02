module StatsdServer
  class Aggregation


    # For a given interval, grab the list of keys that
    # need to be aggregated from the appropriate redis set.
    # Aggregate them across the interval, and store them to disk
    def self.aggregate_pending!(interval)
      StatsdServer.logger "Performing aggregation for #{interval} interval"
      $redis.smembers("needsAggregated:#{interval}") do |keys|
        timing = Benchmark.measure do
          keys.each do |key|
            aggregation = case key
                          when /min/ then "min"
                          when /max/ then "max"
                          when /mean|upper_/ then "mean"
                          else "sum"
                          end

            self.new(key, interval, aggregation).store!
          end
        end
        StatsdServer.logger "Finished aggregation and writing to disktsore for #{interval} in #{timing.real} seconds"
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
        Diskstore.store! "#{@key}:#{@interval}", key_time.to_s, calculate_aggregation.to_s
        $redis.srem("needsAggregated:#{@interval}", @key)
      rescue Exception => e
        StatsdServer.logger "Encountered an error trying to store #{@key}:#{@interval}: #{e}"
      end
    end
    
    def calculate_aggregation(values=nil)
      if values.nil?
        values = values_from_redis
      end
      return unless values.is_a? Array
      values.method(@aggregation).call
    end

    private
      def normalize_time(t, step)
        t = t.to_i
        t - (t % step)
      end

      def values_from_redis
        end_time = normalize_time(@now, @interval+1)
        start_time = end_time - (@interval+1)
        $redis.zrangebyscore(@key, start_time, end_time) do |keys|
          keys.map{|key| decode_record(key)[:data] rescue nil}.compact
        end
      end
      
      def decode_record(key)
        res = {}
        s = key.split("\x01")
        res[:time] = s[0].to_f
        res[:data] = tsdecode(s[1]).to_f
        return res
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
