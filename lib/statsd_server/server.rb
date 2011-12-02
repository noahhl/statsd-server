require 'rubygems'
require 'eventmachine'
require 'yaml'
require 'erb'
require 'benchmark'
require 'em-redis'
require 'base64'

require 'statsd_server/diskstore'
require 'statsd_server/redis_store'
require 'statsd_server/redis-timeseries'

module StatsdServer
  module Server #< EM::Connection  
    
    COUNTERS = {}
    TIMERS = {}

    def post_init
      $redis = EM::Protocols::Redis.connect $config["redis_host"], $config["redis_port"]
      $redis.errback do |code|
        puts "#{Time.now} Error code: #{code}"
      end
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
        $config = YAML::load(ERB.new(IO.read(options[:config])).result)

        StatsdServer::RedisStore.retentions = $config['retention'].split(',')
        $config["retention"] = $config["retention"].split(",").collect{|r| retention = {}; retention[:interval], retention[:count] = r.split(":").map(&:to_i); retention }

        # Start the server
        EventMachine::run do
          #Bind to the socket and gather the incoming datapoints
          EventMachine::open_datagram_socket($config['bind'], $config['port'], StatsdServer::Server)  
          
          #On the flush interval, do the primary aggregation and flush it to
          #a redis zset
          EventMachine::add_periodic_timer($config['flush_interval']) do
            counters,timers = StatsdServer::Server.get_and_clear_stats!
            EM.defer do 
              StatsdServer::RedisStore.flush!(counters,timers) 
            end
          end

          #At every retention that's longer than the flush interval, 
          #perform an aggregation and store it to disk
          StatsdServer::RedisStore.retentions.each do |retention|
            unless retention.split(":")[0].to_i == $config["flush_interval"] 
              EventMachine::add_periodic_timer(retention.split(":")[0].to_i) do
                EM.defer do
                  StatsdServer::RedisStore.aggregate(retention)
                end
              end
            end
          end

          #Every n flush intervals, clean up those values that are past their
          #retention limit
          EventMachine::add_periodic_timer($config['cleanup_interval']) do
            EM.defer do 
              StatsdServer::RedisStore.cleanup!
            end
          end

        end
      end
    end
  end 
end
