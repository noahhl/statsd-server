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
        StatsdServer.logger "[WORKER] Starting to monitor queue for jobs to write to disk." 
        $options = options
        $config = YAML::load(ERB.new(IO.read($options[:config])).result)
        $redis = Redis.new({:host => $config["redis_host"], :port => $config["redis_port"]})
        while true
          $redis.rpop("diskstoreQueue").tap do |job|
            if job
              perform(job)
            else 
              StatsdServer.logger "[WORKER] Waiting for a job." if $options[:debug]
              sleep 1
            end
          end
        end
      end

      def perform(job)
        StatsdServer.logger "[WORKER] Performing #{job} job." if $options[:debug]
        filename, value = job.split("\x0")
        StatsdServer::Diskstore.store!(filename, value)
      end

      def enqueue(job)
       $redis.lpush("diskstoreQueue", job)
      end

    end
  end
end
