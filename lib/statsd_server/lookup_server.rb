require 'eventmachine'
require 'benchmark'
require 'em-redis'
require 'base64'
require 'json'
require 'statsd_server/diskstore'
require 'statsd_server/timeseries'
$options = {}

module StatsdServer
  module LookupServer #< EM::Connection  
    
    $counters = {}
    $gauges = {}
    $timers = {}
    $num_stats = 0

    def post_init
      $started = Time.now
      $redis = EM::Protocols::Redis.connect $config["redis_host"], $config["redis_port"]
      $redis.errback do |code|
        StatsdServer.logger "Error code: #{code}"
      end
      StatsdServer.logger "Lookup server server started!"
    end

    def receive_data(msg)    
      msg.split("\n").each do |row|
        command = row.split(" ")[0]
        return unless command 
        case
          when command.match(/available/i)
            $redis.smembers("datapoints") {|datapoints| send_data "#{JSON(datapoints)}\n" }
          when command.match(/values/i)
            command, metric, begin_time, end_time = row.split(" ")
            StatsdServer::Timeseries.fetch(metric, begin_time.to_i, end_time.to_i, $config['retention']) do |values|
              send_data "#{JSON(values)}\n"
            end
          when command.match(/ping/i)
            send_data "pong\n"
          when command.match(/quit|exit/i)
            send_data "BYE\n"
            close_connection
        end

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
          EventMachine::start_server($config['bind'], ($config['lookup_port'] || $config['port']+2), StatsdServer::LookupServer)  
        end
      end
    end
  end 
end
