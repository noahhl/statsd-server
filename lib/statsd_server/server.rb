require 'rubygems'
require 'eventmachine'
require 'yaml'
require 'erb'
require 'statsd_server/graphite'
module StatsdServer
  module Server #< EM::Connection  
    
    FLUSH_INTERVAL = 10
    COUNTERS = {}
    TIMERS = {}

    def post_init
      puts "statsd server started!"
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
        # puts row
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

        if options[:mongo]
          require 'statsd_server/mongo'
          # Setup retention store
          db = ::Mongo::Connection.new(config['mongo_host']).db(config['mongo_database'])
          config['retentions'].each do |retention|
            collection_name = retention['name']
            unless db.collection_names.include?(collection_name)
              db.create_collection(collection_name, :capped => retention['capped'], :size => retention['cap_bytes']) 
            end
            db.collection(collection_name).ensure_index([['ts', ::Mongo::ASCENDING]])
          end        
          StatsdServer::Mongo.hostname = config['mongo_host']
          StatsdServer::Mongo.database = config['mongo_database']
          StatsdServer::Mongo.retentions = config['retentions']
          StatsdServer::Mongo.flush_interval = config['flush_interval']
        end
      
        if options[:graphite]
          require 'statsd_server/graphite' 
        end
        
        if options[:redis]
          require 'statsd_server/redis_store'
          ENV["coalmine_data_path"] = config['coalmine_data_path']
          StatsdServer::RedisStore.host = config["redis_host"]
          StatsdServer::RedisStore.port = config["redis_port"]
          StatsdServer::RedisStore.flush_interval = config['flush_interval']
          StatsdServer::RedisStore.retentions = config['redis_retention'].split(',')
        end

        if options[:simpledb]
          require 'statsd_server/simpledb_store'
          Statsd::SimpleDBStore.timestep = config["key_size"].to_i
          ENV['AMAZON_ACCESS_KEY_ID'] = config["aws_access_key"] if ENV["AMAZON_ACCESS_KEY_ID"].nil?
          ENV['AMAZON_SECRET_ACCESS_KEY'] = config["aws_access_key_secret"] if ENV["AMAZON_SECRET_ACCESS_KEY"].nil?
        end


        # Start the server
        EventMachine::run do
          EventMachine::open_datagram_socket(config['bind'], config['port'], StatsdServer::Server)  
          

          # Periodically Flush
          EventMachine::add_periodic_timer(config['flush_interval']) do
            counters,timers = StatsdServer::Server.get_and_clear_stats!

             # Flush Adapters
            if options[:mongo]
              EM.defer { StatsdServer::Mongo.flush_stats(counters,timers) } 
            end

            if options[:redis]
              EM.defer { StatsdServer::RedisStore.flush_stats(counters,timers) } 
            end

            if options[:simpledb]
              EM.defer { StatsdServer::SimpleDBStore.flush_stats(counters,timers) rescue nil } 
            end

            if options[:graphite]
              EventMachine.connect config['graphite_host'], config['graphite_port'], StatsdServer::Graphite do |conn|
                conn.counters = counters
                conn.timers = timers
                conn.flush_interval = config['flush_interval']
                conn.flush_stats
              end     
            end
          end

          #Clean up redis zsets
            if options[:redis]
              EventMachine::add_periodic_timer(config['flush_interval'] * 200) do
                EM.defer {StatsdServer::RedisStore.cleanup }
              end
              StatsdServer::RedisStore.retentions.each_with_index do |retention, index|
                unless index.zero?
                  EventMachine::add_periodic_timer(retention.split(":")[0].to_i) do
                    EM.defer {StatsdServer::RedisStore.aggregate(retention)}
                  end
                end
              end
            end

        end
      end
    end
  end 
end
