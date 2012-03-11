require 'rubygems'
require 'yaml'
require 'erb'
require 'benchmark'
require 'redis'
require 'base64'
require 'statsd_server/diskstore'

module StatsdServer
  class Queue
    class << self

      def work!(options)
        StatsdServer.logger "[WORKER] Starting to monitor queue for jobs to to aggregate or write to disk." 
        $options = options
        $config = YAML.load_file($options[:config])
        $redis = Redis.new({:host => $config["redis_host"], :port => $config["redis_port"]})
        while true
          $redis.brpop("aggregationQueue", "diskstoreQueue", "truncateQueue", "gaugeQueue", 30).tap do |job|
            if job
              perform(job[1])
            else 
              StatsdServer.logger "[WORKER] Waiting for an aggregation or diskstore job." if $options[:debug]
            end
          end
        end
      end

      def perform(job)
        StatsdServer.logger "[WORKER] Performing #{job} job." if $options[:debug]
        args = job.split(/\x0|<X>/)
        if args[0]== "store!"
          StatsdServer::Diskstore.store!(args[1], args[2])
        elsif args[0]== "truncate!"
          StatsdServer::Diskstore.truncate!(args[1], args[2])
        elsif args[0] == "aggregate!"
          StatsdServer::Aggregation.new(args[3], args[2].to_i, args[4], args[1].to_i).store!
        end
      end

      def enqueue(job)
       $redis.lpush("diskstoreQueue", job)
      end

    end
  end
end
