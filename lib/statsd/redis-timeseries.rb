require 'rubygems'
require 'redis'
require 'base64'
require 'coalmine'

class Array 
  
 def array_sum
   inject( nil ) { |sum,x| sum ? sum+x : x }; 
 end

 def mean
   self.array_sum / self.length
 end
 
 def median
   self.sort[self.length/2]
 end
end

class RedisTimeSeries

  def initialize(prefix, timestep, redis)
      @prefix = prefix
      @timestep = timestep
      @redis = redis
      @redis.sadd "datapoints", prefix
      @coalmine = Coalmine::Client.new rescue nil
  end

  def cleanup(retentions)
    histories = retentions.split(",").collect{|r| i,n = r.split(":"); i.to_i * n.to_i }
    histories.each_with_index do |history, index|
      suffix = index.zero? ? "" : ":#{retentions.split(",")[index].split(":")[0]}"
      @redis.zremrangebyscore "#{@prefix}#{suffix}", 0, Time.now.to_i - history
    end

  end

  def normalize_time(t, step=@timestep)
      t = t.to_i
      t - (t % step)
  end

  def tsencode(data)
      if data.index("\x00") or data.index("\x01")
          "E#{Base64.encode64(data)}"
      else
          "R#{data}"
      end
  end

  def tsdecode(data)
      if data[0..0] == 'E'
          Base64.decode64(data[1..-1])
      else
          data[1..-1]
      end
  end

  def compute_value_for_key(data, now)
      data = tsencode(data)
      value = "#{now}\x01#{data}"
      return value
  end

  def add(data, origin_time=nil)
      now= Time.now.to_i
      @redis.zadd(@prefix, now, compute_value_for_key(data.to_s, now))
  end

  def aggregate(history, aggregation = 'mean')
      @redis.sadd "aggregations", history.to_s
      aggregation = (aggregation == "sum") ? "array_sum" :  aggregation
      start_time = normalize_time(Time.now.to_f, history+1)
      end_time = start_time + history+1
      aggregate_value = fetch_range(start_time, end_time).collect{|d| d[:data].to_f}.method(aggregation).call rescue nil
      key_time = normalize_time(end_time, history)
      @redis.zremrangebyscore("#{@prefix}:#{history}", key_time - 1, key_time + 1)
      Coalmine::Diskstore.store("#{@prefix}:#{history} #{key_time} #{compute_value_for_key(aggregate_value.to_s, key_time)}") rescue nil
  end

  def decode_record(r)
      res = {}
      s = r.split("\x01")
      res[:time] = s[0].to_f
      res[:data] = tsdecode(s[1]).to_f
      return res
  end

  def seek(time)
      @aggregations ||= [""] + @redis.smembers("aggregations").collect{|a| ":#{a}"}
      start_time = time - @timestep/2
      end_time = time + @timestep/2
      keys = []
      @aggregations.each do |aggregation|
        if keys.empty?
          if aggregation == ""
            keys = @redis.zrangebyscore "#{@prefix}#{aggregation}", begin_time, end_time
          else
              Coalmine::Diskstore.read("#{@prefix}#{suffix} #{begin_time.to_i} #{end_time.to_i}").collect{|k| k.split(":")[1] rescue nil}.compact
          end
        end
      end
      keys.collect{|k| decode_record(k)}
  end

  def fetch_range(begin_time,end_time, strict=false)
    @aggregations ||= [""] + @redis.smembers("aggregations").collect{|a| ":#{a}"}
    keys = []
    @aggregations.each do |aggregation|
      if keys.empty?
        if aggregation == ""
          keys = @redis.zrangebyscore "#{@prefix}#{aggregation}", begin_time, end_time
        else
            Coalmine::Diskstore.read("#{@prefix}#{suffix} #{begin_time.to_i} #{end_time.to_i}").collect{|k| k.split(":")[1] rescue nil}.compact
        end
      end
    end
    keys.collect{|k| decode_record(k) rescue nil}.compact
  end

  def fetch_consistent_range(begin_time, end_time, retentions)
    histories = retentions.split(",").collect{|r| i,n = r.split(":"); i.to_i * n.to_i }
    history_to_use =  histories.index{|h| h >= (Time.now.to_i - begin_time)}
    suffix = history_to_use.zero? ? "" : ":#{retentions.split(",")[history_to_use].split(":")[0]}"
    if history_to_use == histories.first?
      keys = @redis.zrangebyscore "#{@prefix}#{suffix}", begin_time, end_time
      keys.collect{|k| decode_record(k)}
    else
      Coalmine::Diskstore.read("#{@prefix}#{suffix} #{begin_time.to_i} #{end_time.to_i}").collect{|k| k.split(":")[1] rescue nil}.compact.collect{|k| decode_record(k) rescue nil}.compact
    end
  end

end




