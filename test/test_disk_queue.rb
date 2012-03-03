require 'rubygems'
require 'test/unit'
require 'mocha'
require 'statsd_server'
require 'statsd_server/server'

class DiskQueueTest < Test::Unit::TestCase
  
  def setup
    options = {:config => "test/config.yml"}
    ENV["silent"] = "true"
    $config = YAML.load_file(options[:config])
    $config["retention"] = $config["retention"].split(",").collect{|r| retention = {}; retention[:interval], retention[:count] = r.split(":").map(&:to_i); retention }
    $redis = RedisCustom.new({:host => $config["redis_host"], :port => $config["redis_port"]})
  end

  def teardown
    $redis.keys.each{|k| $redis.del k}
    $counters = {}
    $timers = {}
  end

  def test_queue_worker_sends_job_to_diskstore
    StatsdServer::UDP.parse_incoming_message("test_counter:1|c")
    StatsdServer::RedisStore.flush!($counters, {}, {})
    StatsdServer::Aggregation.aggregate_pending!(60)
    assert_equal 1, $redis.llen("aggregationQueue")
    StatsdServer::Aggregation.any_instance.expects(:store!)
    $redis.rpop("aggregationQueue") do |job|
      StatsdServer::Queue.perform(job)
    end
  end

end
