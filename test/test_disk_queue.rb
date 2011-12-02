require 'rubygems'
require 'test/unit'
require 'mocha'
require 'statsd_server'
require 'statsd_server/server'

class DiskQueueTest < Test::Unit::TestCase
  
  def setup
    options = {:config => "test/config.yml"}
  #  ENV["silent"] = "true"
    $config = YAML::load(ERB.new(IO.read(options[:config])).result)
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
    StatsdServer::RedisStore.flush!($counters, {})
    StatsdServer::Aggregation.aggregate_pending!(60)
    assert_equal 1, $redis.llen("diskstoreQueue")
    StatsdServer::Diskstore.expects(:store!)
    $redis.rpop("diskstoreQueue") do |job|
      StatsdServer::Queue.perform(job)
    end
  end

end
