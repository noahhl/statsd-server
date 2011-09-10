require 'rubygems'
require 'redis'
require 'base64'

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
    def initialize(prefix, timestep, redis, expires=nil)
        @prefix = prefix
        @timestep = timestep
        @redis = redis
        @expires= expires
        @suffix = ""
    end

    def normalize_time(t, step=@timestep)
        t = t.to_i
        t - (t % step)
    end

    def getkey(t, step=@timestep)
        "ts:#{@prefix}:#{normalize_time(t, step)}"
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

    def compute_value_for_key(data, now, origin_time=nil)
        data = tsencode(data)
        origin_time = tsencode(origin_time) if origin_time
        value = "#{now}\x01#{data}"
        value << "\x01#{origin_time}" if origin_time
        value << "\x00"
        return value
    end

    def add(data, origin_time=nil)
        now = Time.now.to_f
        value = compute_value_for_key(data, now, origin_time)
        if @expires.nil?
          @redis.append(getkey(now.to_i),value)
        else
            @redis.setex(getkey(now.to_i),@expires, value)
        end
    end

    def aggregate(history, ttl, aggregation = 'mean')
        aggregation = (aggregation == "sum") ? "array_sum" :  aggregation
        start_time = normalize_time(Time.now.to_f, history+1)
        end_time = start_time + history+1
        aggregate_value = fetch_range(start_time, end_time, strict=true).collect{|d| d[:data].to_f}.method(aggregation).call rescue nil
        unless aggregate_value.nil?
          @redis.setex("#{getkey(end_time, history)}:#{history}", ttl, compute_value_for_key(aggregate_value.to_s, end_time))
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

    def seek(time)
        best_start = nil
        best_time = nil
        rangelen = 64
        key = @redis.keys("#{getkey(time.to_i)}*").min
        len = @redis.strlen(key)
        return 0 if len == 0
        min = 0
        max = len-1
        while true
            p = min+((max-min)/2)
            # puts "Min: #{min} Max: #{max} P: #{p}"
            # Seek the first complete record starting from position 'p'.
            # We need to search for two consecutive \x00 chars, and enlarnge
            # the range if needed as we don't know how big the record is.
            while true
                range_end = p+rangelen-1
                range_end = len if range_end > len
                r = @redis.getrange(key,p,range_end)
                # puts "GETRANGE #{p} #{range_end}"
                if p == 0
                    sep = -1
                else
                    sep = r.index("\x00")
                end
                sep2 = r.index("\x00",sep+1) if sep
                if sep and sep2
                    record = r[((sep+1)...sep2)]
                    record_start = p+sep+1
                    record_end = p+sep2-1
                    dr = decode_record(record)

                    # Take track of the best sample, that is the sample
                    # that is greater than our sample, but with the smallest
                    # increment.
                    if dr[:time] >= time and (!best_time or best_time>dr[:time])
                        best_start = record_start
                        best_time = dr[:time]
                        # puts "NEW BEST: #{best_time}"
                    end
                    # puts "Max-Min #{max-min} RS #{record_start}"
                    return best_start if max-min == 1
                    break
                end
                # Already at the end of the string but still no luck?
                return len+1 if range_end = len
                # We need to enlrange the range, it is interesting to note
                # that we take the enlarged value: likely other time series
                # will be the same size on average.
                rangelen *= 2
            end
            # puts dr.inspect
            return record_start if dr[:time] == time
            if dr[:time] > time
                max = p
            else
                min = p
            end
        end
    end

    def produce_result(res,key,range_begin,range_end, strict=false, dupe_check = true)
        unless strict
            key = @redis.keys("#{key}*").min
            if dupe_check
              if check_overlap(key) 
                return []
              end
            end
        end
        r = @redis.getrange(key,range_begin,range_end)
        if r
            s = r.split("\x00")
            s.each{|r|
                record = decode_record(r)
                res << record
            }
        end
    end

    def fetch_range(begin_time,end_time, strict=false)
        res = []
        time_range = [begin_time.to_s, end_time.to_s]
        common_time = time_range[0].slice(0,(0...time_range[0].size).find {|i| time_range.map {|s| s[i..i]}.uniq.size > 1})
        (end_time.to_s.length - common_time.length).times {common_time += "?"}
        if strict
          keys_in_set = @redis.keys("ts:#{@prefix}:#{common_time}#{@suffix}").sort
        else
          keys_in_set = @redis.keys("ts:#{@prefix}:#{common_time}*").sort
        end
        
        if keys_in_set.empty?
          return []
        end

        begin_index = keys_in_set.index{|k| k >= getkey(begin_time)}
        end_index = keys_in_set.index{|k| k == keys_in_set.reverse.find{|k| k <= getkey(end_time)}}
        keys = ([getkey(begin_time)] + keys_in_set[begin_index..end_index] + [getkey(end_time)]).uniq

        begin_off = seek(begin_time)
        end_off = seek(end_time)
        if keys.first == keys.last
            produce_result(res,keys.first,begin_off,end_off-1, strict)
        else
            produce_result(res,keys.shift,begin_off,-1, strict)
            keys.each do |key|
                break if key == keys.last
                produce_result(res,key,0,-1, strict)
            end
            produce_result(res,keys.last,0,end_off-1, strict)
        end
        @suffix = ""
        res
    end

    def fetch_consistent_range(begin_time, end_time, retentions)
      histories = retentions.split(",").collect{|r| i,n = r.split(":"); i.to_i * n.to_i }
      history_to_use =  histories.index{|h| h >= (Time.now.to_i - begin_time)}
      
      unless history_to_use.zero? #Within most granular, no suffix range.
        @suffix = ":#{retentions.split(",")[history_to_use].split(":")[0]}"
      end
      fetch_range(begin_time, end_time, strict=true)

    end

    def fetch_timestep(time, strict=false)
        res = []
        key = getkey(time)
        produce_result(res,key,0,-1, strict)
        res
    end

    def check_overlap(key)
      overlap = false
      unless key.nil?  
        start_time = key.split(":")[-2].to_i - key.split(":")[-1].to_i
        end_time = key.split(":")[-2].to_i
        if start_time >0
          t = start_time
          while t < end_time
            unless produce_result([], getkey(t), 0, -1, strict=false, dupe_check=false).empty?
              overlap = true
              break
            end
            t += @timestep
          end
        end
      end
      overlap
    end
end




