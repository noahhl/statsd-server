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

      def work_aggregation!(options)
        StatsdServer.logger "[WORKER] Starting to monitor queue for jobs to to aggregate." 
        $options = options
        $config = YAML::load(ERB.new(IO.read($options[:config])).result)
        $redis = Redis.new({:host => $config["redis_host"], :port => $config["redis_port"]})
        while true
          $redis.rpop("aggregationQueue").tap do |job|
            if job
              perform_aggregation(job)
            else 
              StatsdServer.logger "[WORKER] Waiting for an aggregation job." if $options[:debug]
              sleep 1
            end
          end
        end
      end

      def work_diskstore!(options)
        StatsdServer.logger "[WORKER] Starting to monitor queue for jobs to write to disk." 
        $options = options
        $config = YAML::load(ERB.new(IO.read($options[:config])).result)
        $redis = Redis.new({:host => $config["redis_host"], :port => $config["redis_port"]})
        while true
          $redis.rpop("diskstoreQueue").tap do |job|
            if job
              perform_diskstore(job)
            else 
              StatsdServer.logger "[WORKER] Waiting for a diskstore job." if $options[:debug]
              sleep 1
            end
          end
        end
      end

      def perform_aggregation(job)
        StatsdServer.logger "[WORKER] Performing #{job} job." if $options[:debug]
        now,interval,key,aggregation = job.split("\x0")
        StatsdServer::Aggregation.new(key, interval.to_i, aggregation, now.to_i).store!
      end

      def perform_diskstore(job)
        StatsdServer.logger "[WORKER] Performing #{job} job." if $options[:debug]
        type, filename, value = job.split("\x0")
        if type == "store!"
          StatsdServer::Diskstore.store!(filename, value)
        elsif type == "truncate!"
          StatsdServer::Diskstore.truncate!(filename, value)
        end
      end

      def enqueue(job)
       $redis.lpush("diskstoreQueue", job)
      end

    end
  end
end
