require 'rubygems'
require 'redis'
require 'base64'
require 'diskstore'

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
    @redis ||= redis
  end

  def cleanup(retentions)
    histories = retentions.split(",").collect{|r| i,n = r.split(":"); i.to_i * n.to_i }
    histories.each_with_index do |history, index|
      suffix = index.zero? ? "" : ":#{retentions.split(",")[index].split(":")[0]}"
      if suffix == ""
        @redis.zremrangebyscore "#{@prefix}#{suffix}", 0, Time.now.to_i - history
      else
        Diskstore.truncate("#{@prefix}#{suffix}",  "#{Time.now.to_i - history}") 
      end
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
    @redis.sadd "datapoints", @prefix
    @redis.zadd(@prefix, now, compute_value_for_key(data.to_s, now))
  end

  def aggregate(history, aggregation = 'mean')
    @redis.sadd "aggregations", history.to_s
    aggregation = (aggregation == "sum") ? "array_sum" :  aggregation
    end_time = normalize_time(Time.now.to_f, history+1)
    start_time = end_time - (history+1)
    aggregate_value = fetch_range(start_time, end_time).collect{|d| d[:data].to_f}.method(aggregation).call rescue nil
    key_time = normalize_time(end_time, history)
    Diskstore.store("#{@prefix}:#{history}", key_time.to_s, aggregate_value.to_s)
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
            Diskstore.read("#{@prefix}#{suffix}", begin_time.to_i.to_s, end_time.to_i.to_s)
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
          Diskstore.read("#{@prefix}#{suffix}", begin_time.to_i.to_s, end_time.to_i.to_s)
        end
      end
    end
    keys.collect{|k| decode_record(k) rescue nil}.compact
  end

  def fetch_consistent_range(begin_time, end_time, retentions)
    histories = retentions.split(",").collect{|r| i,n = r.split(":"); i.to_i * n.to_i }
    history_to_use =  histories.index{|h| h >= (Time.now.to_i - begin_time)}
    suffix = history_to_use.zero? ? "" : ":#{retentions.split(",")[history_to_use].split(":")[0]}"
    if suffix == "" 
      keys = @redis.zrangebyscore "#{@prefix}#{suffix}", begin_time, end_time
      keys.collect{|k| decode_record(k)}
    else
      Diskstore.read("#{@prefix}#{suffix}", begin_time.to_i.to_s, end_time.to_i.to_s)
    end
  end
end
