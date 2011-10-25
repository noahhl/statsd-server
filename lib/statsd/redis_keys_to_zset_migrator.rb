require 'rubygems'
require 'redis'
require 'base64'
class RedisKeysToZsetMigrator
  # Usage: 
  # redis = Redis.new
  # migrator = RedisKeysToZsetMigrator.new(redis)
  # migrator.migrate!

  def initialize(redis)
    @redis = redis
  end

  def migrate!
    keys = @redis.keys "ts:*"
    keys.each_with_index do |key, index|
      migrate_key(key)
      (index % 50).zero? ? print(".") : nil
    end
  end

  def migrate_key(key)
    begin
      keyname, timeinfo = key.gsub("ts:", "").split(/:[0-9][0-9][0-9][0-9]/)
      ts, history = timeinfo.split(":")
      unless history.nil?
        keyname = "#{keyname}:#{history}"
      end
      val = decode_record(@redis.get(key))
      @redis.zadd keyname, ts.to_i, encode(val[:time], val[:data])
      @redis.del key
    rescue Exception => e
      puts "ERROR: #{e}"
    end
  end

  def encode(now, data)
    data = tsencode(data)
    value = "#{now}\x01#{data}"
    return value
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

  def decode_record(r)
      res = {}
      s = r.split("\x01")
      res[:time] = s[0].to_f
      res[:data] = tsdecode(s[1])
      if s[2]
          res[:origin_time] = tsdecode(s[2])
      else
          res[:origin_time] = nil
      end
      return res
  end


end
