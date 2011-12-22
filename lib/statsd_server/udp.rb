module StatsdServer
  class UDP
    class << self

      def parse_incoming_message(row)
        StatsdServer.logger "received #{row}" if $options[:debug]
        bits = row.split(':')
        key = bits.shift.gsub(/\s+/, '_').gsub(/\//, '-').gsub(/[^a-zA-Z_\-0-9\.]/, '')
        bits.each do |record|
          sample_rate = 1
          fields = record.split("|")    
          if (fields[1].strip == "ms") 
            $timers[key] ||= []
            $timers[key].push(fields[0].to_f)
          elsif(fields[1].strip == "g")
            $gauges[key] ||= [] 
            $gauges[key].push([Time.now.to_i, fields[0].to_f])
          else
            if (fields[2] && fields[2].match(/^@([\d\.]+)/)) 
              sample_rate = fields[2].match(/^@([\d\.]+)/)[1]
            end
            $counters[key] ||= 0
            $counters[key] += (fields[0].to_f || 1) * (1.0 / sample_rate.to_f)
          end
        end
      rescue
        nil
      end

    end
  end
end
