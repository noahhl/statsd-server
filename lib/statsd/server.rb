require 'rubygems'
gem 'eventmachine'
require 'eventmachine'
require 'yaml'
require 'erb'
module Statsd
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
          require 'statsd/mongo'
          # Setup retention store
          db = ::Mongo::Connection.new(config['mongo_host']).db(config['mongo_database'])
          config['retentions'].each do |retention|
            collection_name = retention['name']
            unless db.collection_names.include?(collection_name)
              db.create_collection(collection_name, :capped => retention['capped'], :size => retention['cap_bytes']) 
            end
            db.collection(collection_name).ensure_index([['ts', ::Mongo::ASCENDING]])
          end        
          Statsd::Mongo.hostname = config['mongo_host']
          Statsd::Mongo.database = config['mongo_database']
          Statsd::Mongo.retentions = config['retentions']
          Statsd::Mongo.flush_interval = config['flush_interval']
        end
      
        if options[:graphite]
          require 'statsd/graphite' 
        end
        
        if options[:redis]
          require 'statsd/redis_store'
          Statsd::RedisStore.host = config["redis_host"]
          Statsd::RedisStore.port = config["redis_port"]
          Statsd::RedisStore.flush_interval = config['flush_interval']
          Statsd::RedisStore.retentions = config['redis_retention'].split(',')
        end

        if options[:simpledb]
          require 'statsd/simpledb_store'
          Statsd::SimpleDBStore.timestep = config["key_size"].to_i
#          ENV['AMAZON_ACCESS_KEY_ID'] = config["aws_access_key"] if ENV["AMAZON_ACCESS_KEY_ID"].nil?
#          ENV['AMAZON_SECRET_ACCESS_KEY'] = config["aws_access_key_secret"] if ENV["AMAZON_SECRET_ACCESS_SKEY"].nil?
        end


        # Start the server
        EventMachine::run do
          EventMachine::open_datagram_socket(config['bind'], config['port'], Statsd::Server)  

          # Periodically Flush
          EventMachine::add_periodic_timer(config['flush_interval']) do
            counters,timers = Statsd::Server.get_and_clear_stats!

             # Flush Adapters
            if options[:mongo]
              EM.defer { Statsd::Mongo.flush_stats(counters,timers) } 
            end

            if options[:redis]
              EM.defer { Statsd::RedisStore.flush_stats(counters,timers) } 
            end

            if options[:simpledb]
              EM.defer { Statsd::SimpleDBStore.flush_stats(counters,timers) rescue nil } 
            end

            if options[:graphite]
              EventMachine.connect config['graphite_host'], config['graphite_port'], Statsd::Graphite do |conn|
                conn.counters = counters
                conn.timers = timers
                conn.flush_interval = config['flush_interval']
                conn.flush_stats
              end     
            end
          
          end

          #Clean up redis zsets
          EventMachine::add_periodic_timer(config['flush_interval'] * 100) do
            if options[:redis]
              EM.defer {Statsd::RedisStore.cleanup }
            end
          end

        end
      
      end
    end
  end 
end



require 'statsd/graphite'



