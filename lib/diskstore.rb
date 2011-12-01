require 'digest'
require 'fileutils'
class Diskstore
  class << self

    def calc_filename(statistic)
      file_hash = Digest::MD5.hexdigest(statistic)
      FileUtils.mkdir_p File.join(ENV["coalmine_data_path"], file_hash[0,2], file_hash[2,2])
      File.join(ENV["coalmine_data_path"], file_hash[0,2], file_hash[2,2], file_hash)
    end

    def store(statistic, ts, value)
      filename = calc_filename(statistic)
      File.open(filename, 'a+') do |file|
        file.write("#{ts} #{value}\n")
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

    def truncate(statistic, since)
      filename = calc_filename(statistic)
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
        FileUtils.mv("#{filename}tmp", filename) rescue nil
      end
    rescue
      nil
    end

  end
end
