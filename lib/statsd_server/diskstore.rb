require 'digest'
require 'fileutils'

module StatsdServer
  class Diskstore
    class << self

      def calc_filename(statistic)
        return unless statistic
        file_hash = Digest::MD5.hexdigest(statistic)
        File.join($config["coalmine_data_path"], file_hash[0,2], file_hash[2,2], file_hash)
      end

      def enqueue(type, statistic, *args)
        filename = calc_filename(statistic)
        value = args.join(" ")
        $redis.lpush "diskstoreQueue", "#{type}\x0#{filename}\x0#{value}"
      end

      def enqueue_gauge(type, statistic, *args)
        filename = calc_filename(statistic)
        value = args.join(" ")
        $redis.lpush "gaugeQueue", "#{type}<X>#{filename}<X>#{value}"
      end

      def store!(filename, value)
        FileUtils.mkdir_p filename.split("/")[0..-2].join("/")
        File.open(filename, 'a+') do |file|
          file.write("#{value}\n")
          file.close
        end
      rescue Exception => e
        StatsdServer.logger "Encountered an error trying to store to #{filename}: #{e}"
      end

      def read(statistic, start_ts, end_ts)
        datapoints = []
        filename = calc_filename(statistic)
        begin
          File.open(filename, 'r') do |file| 
            while (line = file.gets)
              ts, value = line.split
              if ts >= start_ts && ts <= end_ts
                datapoints << {:time => ts.to_i, :data => value}
              end
            end
            file.close
          end
        rescue Errno::ENOENT => e
          StatsdServer.logger "Encountered an error trying to read #{filename}: #{e}" if $options[:debug]
        rescue Exception => e
          StatsdServer.logger "Encountered an error trying to read #{filename}: #{e}"
        end
        datapoints
      end

      def truncate!(filename, since)
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
      rescue Errno::ENOENT
        StatsdServer.logger "Encountered an error trying to truncate #{filename}: #{e}" if $options[:debug]
      rescue Exception => e
        StatsdServer.logger "Encountered an error trying to truncate #{filename}: #{e}"
      ensure 
        FileUtils.rm("#{filename}tmp") rescue nil
      end

    end
  end
end
