require 'digest'
require 'fileutils'

module StatsdServer
  class Diskstore
    class << self
      
      def cleanup!
        $redis.smembers("datapoints") do |datapoints|
          timing = Benchmark.measure do 
            StatsdServer.logger "Cleaning up #{datapoints.length} datapoints from diskstore.\n" 
            datapoints.each do |datapoint|
              retention = $config["retention"].find{|r| r[:interval] != $config["flush_interval"]}
              truncate! "#{datapoint}:#{retention[:interval]}", (Time.now.to_i - (retention[:interval] * retention[:count]))
            end
          end
          StatsdServer.logger "Finished truncating diskstore in #{timing.real} seconds" if $options[:debug]
        end
      end

      def calc_filename(statistic)
        file_hash = Digest::MD5.hexdigest(statistic)
        FileUtils.mkdir_p File.join($config["coalmine_data_path"], file_hash[0,2], file_hash[2,2])
        File.join($config["coalmine_data_path"], file_hash[0,2], file_hash[2,2], file_hash)
      end

      def enqueue(statistic, ts, value)
        puts "Queueing value: #{statistic} #{ts} #{value}" if $options[:debug]
        filename = calc_filename(statistic)
        value = "#{ts} #{value}"
        $redis.lpush "diskstoreQueue", "#{filename}\x0#{value}"
      end

      def store!(filename, value)
        File.open(filename, 'a+') do |file|
          file.write("#{value}\n")
          file.close
        end
      end

      def read(statistic, start_ts, end_ts)
        datapoints = []
        filename = calc_filename(statistic)
        File.open(filename, 'r') do |file| 
          while (line = file.gets)
            ts, value = line.split
            if ts >= start_ts && ts <= end_ts
              datapoints << {:time => ts.to_i, :data => value}
            end
          end
          file.close
        end
        datapoints
      end

      def truncate!(statistic, since)
        filename = calc_filename(statistic)
        StatsdServer.logger "Truncating #{filename} since #{since}" if $options[:debug]
        unless File.exists? "#{filename}tmp"  
          File.open("#{filename}tmp", "w") do |tmpfile|
            File.open(filename, 'r') do |file|
              while (line = file.gets)
                if(line.split[0] >= since rescue true)
                  tmpfile.write(line)
                end
              end
              file.close
            end
            tmpfile.close
          end
          FileUtils.cp("#{filename}tmp", filename) rescue nil
        end
      rescue Exception => e
        StatsdServer.logger "Encountered an error trying to truncate #{filename}: #{e}"
      ensure 
        FileUtils.rm("#{filename}tmp")
      end

    end
  end
end
