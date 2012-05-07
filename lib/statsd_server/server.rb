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
    $needsAggregated = {}
    $datapoints = {}
    $num_stats = 0

    def post_init
      $redis = EM::Protocols::Redis.connect $config["redis_host"], $config["redis_port"]
      $redis.errback do |code|
        StatsdServer.logger "Error code: #{code}"
      end
      $redis.set "server:started", Time.now.to_i
      $redis.set "server:cleanup", Time.now.to_i
      $redis.set "server:num_processed", 0
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
        $config = YAML.load_file($options[:config])
        $config["retention"] = $config["retention"].split(",").collect do |r| 
          retention = {}
          retention[:interval], retention[:count] = r.split(":").map(&:to_i)
          retention 
        end

        # Start the server
        EventMachine::run do
          EventMachine.threadpool_size = 500 

          #Bind to the socket and gather the incoming datapoints
          EventMachine::open_datagram_socket($config['bind'], $config['port'], StatsdServer::Server)

          #Bind to the info_port socket OR the next socket up
          EventMachine::start_server($config['bind'], ($config['info_port'] || $config['port']+1), 
                                     StatsdServer::Server::InfoServer)

          # On the flush interval, do the primary aggregation and flush it to
          # a redis zset
          EventMachine::add_periodic_timer($config['flush_interval']) do
            counters,gauges,timers = StatsdServer::Server.get_and_clear_stats!
            StatsdServer::RedisStore.flush!(counters,gauges,timers) 
          end

          # At every retention that's longer than the flush interval, 
          # enqeue a task to aggregate it and store it to disk
          $config['retention'].each_with_index do |retention, index|
            $needsAggregated[retention[:interval]] = {}
            unless index.zero?
              EventMachine::add_periodic_timer(retention[:interval]) do
                needsAggregated = $needsAggregated[retention[:interval]].dup
                $needsAggregated[retention[:interval]] = {}
                EM.defer do 
                  StatsdServer::Aggregation.aggregate_pending!(retention[:interval], needsAggregated )
                end
                if index == 1
                  StatsdServer::RedisStore.update_datapoint_list!($datapoints)
                  $datapoints = {} 
                  $redis.incrby "server:num_processed", $num_stats
                  $num_stats = 0
                end

              end
            end
          end

          # On the cleanup interval, clean up those values that are past their
          # retention limit in redis only.
          EventMachine::add_periodic_timer($config['cleanup_interval']) do
            $redis.set "server:cleanup", Time.now.to_i
            EM.defer { StatsdServer::RedisStore.cleanup! }
          end

        end
      end
    end
  end 
end
