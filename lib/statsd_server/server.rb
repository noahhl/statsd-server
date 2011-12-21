require 'eventmachine'
require 'benchmark'
require 'em-redis'
require 'base64'
require 'statsd_server/udp'
require 'statsd_server/queue'
require 'statsd_server/aggregation'
require 'statsd_server/diskstore'
require 'statsd_server/redis_store'
require 'statsd_server/info_server'

$options = {}

module StatsdServer
  module Server #< EM::Connection  
    
    $counters = {}
    $gauges = {}
    $timers = {}
    $num_stats = 0

    def post_init
      $started = Time.now
      $last_cleanup = Time.now
      $redis = EM::Protocols::Redis.connect $config["redis_host"], $config["redis_port"]
      $redis.errback do |code|
        StatsdServer.logger "Error code: #{code}"
      end
      StatsdServer.logger "statsd server started!"
    end

    def self.get_and_clear_stats!
      counters = $counters.dup
      gauges = $gauges.dup
      timers = $timers.dup
      $counters.clear
      $gauges.clear
      $timers.clear
      [counters,gauges,timers]
    end

    def receive_data(msg)    
      msg.split("\n").each do |row|
        StatsdServer::UDP.parse_incoming_message(row) 
      end
    end

    class Daemon
      def run(options)
        $options = options
        $config = YAML::load(ERB.new(IO.read($options[:config])).result)
        $config["retention"] = $config["retention"].split(",").collect{|r| retention = {}; retention[:interval], retention[:count] = r.split(":").map(&:to_i); retention }

        # Start the server
        EventMachine::run do
          EventMachine.threadpool_size = 500 
          #Bind to the socket and gather the incoming datapoints
          EventMachine::open_datagram_socket($config['bind'], $config['port'], StatsdServer::Server)  
          EventMachine::start_server($config['bind'], ($config['info_port'] || $config['port']+1), StatsdServer::Server::InfoServer)  

          # On the flush interval, do the primary aggregation and flush it to
          # a redis zset
          EventMachine::add_periodic_timer($config['flush_interval']) do
            counters,gauges,timers = StatsdServer::Server.get_and_clear_stats!
            StatsdServer::RedisStore.flush!(counters,gauges,timers) 
          end

          # At every retention that's longer than the flush interval, 
          # perform an aggregation and store it to disk
          $config['retention'].each_with_index do |retention, index|
            unless index.zero?
              EventMachine::add_periodic_timer(retention[:interval]) do
                StatsdServer::Aggregation.aggregate_pending!(retention[:interval])
              end
            end
          end

          # On the cleanup interval, clean up those values that are past their
          # retention limit
          EventMachine::add_periodic_timer($config['cleanup_interval']) do
            $last_cleanup = Time.now
            StatsdServer::RedisStore.cleanup!
            StatsdServer::Diskstore.cleanup!
          end

        end
      end
    end
  end 
end
