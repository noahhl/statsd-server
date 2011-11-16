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
      newLineWritten=false
      if File.exists? filename
        File.open("#{filename}tmp#{ts}", "w") do |tmpfile|
          File.open(filename, 'r') do |file|
            while (line = file.gets)
              if ts == line.split[0]
                tmpfile.write("#{ts} #{value}\n")
                newLineWritten=true
              else
                tmpfile.write(line)
              end
            end
            file.close
          end
          unless newLineWritten
            tmpfile.write("#{ts} #{value}\n")
          end
          tmpfile.close
        end
        FileUtils.mv("#{filename}tmp#{ts}", filename) rescue nil 
      else
        File.open(filename, 'w') do |file|
          file.write("#{ts} #{value}\n")
          file.close
        end
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
      File.open("#{filename}tmp#{since}", "w") do |tmpfile|
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
      FileUtils.mv("#{filename}tmp#{since}", filename) rescue nil
    rescue
      nil
    end

  end
end
