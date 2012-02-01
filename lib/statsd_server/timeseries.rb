module StatsdServer
  class Timeseries 
    class << self

      def tsdecode(data)
        if data[0..0] == 'E'
            Base64.decode64(data[1..-1])
        else
            data[1..-1]
        end
      end

      def decode_record(r)
        res = {}
        s = r.split("\x01")
        res[:time] = s[0].to_f
        res[:data] = tsdecode(s[1]).to_f
        return res
      end

      def fetch(metric, begin_time, end_time, retentions, &block)
        #histories = retentions.split(",").collect{|r| i,n = r.split(":"); i.to_i * n.to_i }
        history_to_use =  retentions.index{|h| (h[:interval] * h[:count]) >= (Time.now.to_i - begin_time)} || (retentions.length - 1)
        suffix = history_to_use.zero? ? "" : ":#{retentions[history_to_use][:interval]}"
        values = []
        if metric.split(":")[0] == 'gauges'
          StatsdServer::Diskstore.read(@key, begin_time.to_i.to_s, end_time.to_i.to_s).sort{|a,b| a[:time] <=> b[:time]}.tap do |values|
            yield values 
          end

        elsif suffix == "" 
          $redis.zrangebyscore("#{metric}#{suffix}", begin_time, end_time) do |keys|
            yield keys.collect{|k| decode_record(k)}
          end
        else
          StatsdServer::Diskstore.read("#{metric}#{suffix}", begin_time.to_s, end_time.to_s).tap do |values|
            yield values
          end
        end
      end

    end
  end
end