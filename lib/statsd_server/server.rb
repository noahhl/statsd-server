require 'rubygems'
require 'eventmachine'
require 'yaml'
require 'erb'
module StatsdServer
  module Server #< EM::Connection  
    
    COUNTERS = {}
    TIMERS = {}

    def post_init
      puts "#{Time.now} statsd server started!"
    end

    def self.get_and_clear_stats!
      counters = COUNTERS.dup
      timers = TIMERS.dup
      COUNTERS.clear
      TIMERS.clear
      [counters,timers]
    end

    def receive_data(msg)    
      msg.split("\n").each do |row|
        puts "#{Time.now} got #{row}" if OPTIONS[:debug]
        bits = row.split(':')
        key = bits.shift.gsub(/\s+/, '_').gsub(/\//, '-').gsub(/[^a-zA-Z_\-0-9\.]/, '')
        bits.each do |record|
          sample_rate = 1
          fields = record.split("|")    
          if (fields[1].strip == "ms") 
            TIMERS[key] ||= []
            TIMERS[key].push(fields[0].to_f)
          else
            if (fields[2] && fields[2].match(/^@([\d\.]+)/)) 
              sample_rate = fields[2].match(/^@([\d\.]+)/)[1]
            end
            COUNTERS[key] ||= 0
            COUNTERS[key] += (fields[0].to_f || 1) * (1.0 / sample_rate.to_f)
          end
        end
      end
    end    

    class Daemon
      def run(options)
        config = YAML::load(ERB.new(IO.read(options[:config])).result)
      
        require 'statsd_server/redis_store'
        ENV["coalmine_data_path"] = config['coalmine_data_path']
        StatsdServer::RedisStore.host = config["redis_host"]
        StatsdServer::RedisStore.port = config["redis_port"]
        StatsdServer::RedisStore.flush_interval = config['flush_interval']
        StatsdServer::RedisStore.retentions = config['redis_retention'].split(',')

        # Start the server
        EventMachine::run do
          #Bind to the socket and gather the incoming datapoints
          EventMachine::open_datagram_socket(config['bind'], config['port'], StatsdServer::Server)  
          
          #On the flush interval, do the primary aggregation and flush it to
          #a redis zset
          EventMachine::add_periodic_timer(config['flush_interval']) do
            counters,timers = StatsdServer::Server.get_and_clear_stats!
            EM.defer { StatsdServer::RedisStore.flush_stats(counters,timers) } 
          end

          #At every retention that's longer than the flush interval, 
          #perform an aggregation and store it to disk
          StatsdServer::RedisStore.retentions.each do |retention|
            unless retention.split(":")[0].to_i == config["flush_interval"] 
              EventMachine::add_periodic_timer(retention.split(":")[0].to_i) do
                EM.defer {StatsdServer::RedisStore.aggregate(retention)}
              end
            end
          end

          #Every n flush intervals, clean up those values that are past their
          #retention limit
          EventMachine::add_periodic_timer(config['flush_interval'] * 200) do
            EM.defer {StatsdServer::RedisStore.cleanup }
          end

        end
      end
    end
  end 
end
