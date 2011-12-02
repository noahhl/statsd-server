require 'rubygems'
require 'test/unit'
require 'redis'
require 'mocha'
require 'statsd_server/server'

class CleanupTest < Test::Unit::TestCase

  def setup
    options = {:config => "test/config.yml"}
    #ENV["silent"] = "true"
    $config = YAML::load(ERB.new(IO.read(options[:config])).result)
    $config["retention"] = $config["retention"].split(",").collect{|r| retention = {}; retention[:interval], retention[:count] = r.split(":").map(&:to_i); retention }
    $redis = RedisCustom.new({:host => $config["redis_host"], :port => $config["redis_port"]})
    StatsdServer::UDP.parse_incoming_message("test_counter:1|c")
    StatsdServer::UDP.parse_incoming_message("test_timer:1|ms")
  end
  
  def teardown
    $redis.keys.each{|k| $redis.del k}
    $counters = {}
    $timers = {}
  end

  def test_cleanup_truncates_redis_zsets  
    StatsdServer::RedisStore.flush!($counters, {})
    $redis.expects(:zremrangebyscore).with('counters:test_counter', 0, (Time.now.to_i - 21600))
    StatsdServer::RedisStore.cleanup!
  end
  
  def test_cleanup_truncates_files_on_disk
    StatsdServer::RedisStore.flush!($counters, {})
    StatsdServer::Diskstore.expects(:truncate!).with('counters:test_counter:60', (Time.now.to_i - 604800))
    StatsdServer::Diskstore.cleanup!
  end

end
