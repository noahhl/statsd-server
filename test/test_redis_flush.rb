require 'rubygems'
require 'test/unit'
require 'redis'
require 'statsd_server'
require 'statsd_server/server'
require 'timecop'

class RedisFlushTest < Test::Unit::TestCase

  def setup
    options = {:config => "test/config.yml"}
    ENV["silent"] = "true"
    $config = YAML.load_file(options[:config])
    $config["retention"] = $config["retention"].split(",").collect{|r| retention = {}; retention[:interval], retention[:count] = r.split(":").map(&:to_i); retention }
    $redis = Redis.new({:host => $config["redis_host"], :port => $config["redis_port"]})
    StatsdServer::UDP.parse_incoming_message("test_counter:1|c")
    StatsdServer::UDP.parse_incoming_message("test_timer:1|ms")
    StatsdServer::UDP.parse_incoming_message("test_gauge:1|g")
  end

  def teardown
    $redis.keys.each{|k| $redis.del k}
    $counters = {}
    $timers = {}
  end

  def test_redis_values_are_encoded_properly
    now = Time.now.to_i.to_s
    key = StatsdServer::RedisStore.compute_value_for_key("2", now)
    assert_equal now, key.split("\x01")[0]
    assert_equal "R2", key.split("\x01")[1]
  end

  def test_getting_and_clearing_stats
    counters,gauges,timers = StatsdServer::Server.get_and_clear_stats!
    assert_equal 1, counters.size
    assert_equal 1, gauges.size
    assert_equal 1, timers.size
    assert_empty $counters
    assert_empty $timers
    assert_empty $gauges
  end

  def test_storing_a_gauge_directly_queues_to_disk
    assert_empty $redis.smembers "datapoints"
    assert_equal 0, $redis.llen("gaugeQueue")
    StatsdServer::RedisStore.flush!({}, $gauges, {})
    assert  $redis.llen("gaugeQueue") >= 1
  end

  def test_storing_counter_initially_adds_key_to_redis
    assert_empty $redis.smembers "datapoints"
    assert_equal 0, $redis.zcard("counters:test_counter")
    StatsdServer::RedisStore.flush!($counters, {}, {})
    assert_equal ["counters:test_counter"], $redis.smembers("datapoints")
    assert_equal 1, $redis.zcard("counters:test_counter")
  end

  def test_adding_to_redis_queues_aggregations
    assert_empty $redis.smembers "needsAggregated:60"
    assert_empty $redis.smembers "needsAggregated:600"
    StatsdServer::RedisStore.flush!($counters, {}, {})
    assert_equal ["counters:test_counter"], $redis.smembers("needsAggregated:60")
    assert_equal ["counters:test_counter"], $redis.smembers("needsAggregated:600")
  end

  def test_updating_counter_adds_datapoints_but_not_keys
    StatsdServer::RedisStore.flush!($counters, {}, {})
    Timecop.freeze(Time.now + 30) do 
      StatsdServer::RedisStore.flush!($counters, {}, {})
      assert_equal ["counters:test_counter"], $redis.smembers("datapoints")
      assert_equal 2, $redis.zcard("counters:test_counter")
    end
  end
  
  def test_storing_timer_initially_adds_keys_to_redis
    assert_empty $redis.smembers "datapoints"
    StatsdServer::RedisStore.flush!({}, {}, $timers)
    assert_equal 6, $redis.scard("datapoints")
    assert_equal ["timers:test_timer:mean"], $redis.keys("*test_timer:mean")
  end

  def test_updating_timer_adds_datapoints_but_not_keys
    StatsdServer::RedisStore.flush!({}, {}, $timers)
    assert_equal 6, $redis.scard("datapoints")
    Timecop.freeze(Time.now + 30) do 
      StatsdServer::RedisStore.flush!({}, {}, $timers)
      assert_equal 6, $redis.scard("datapoints")
      assert_equal 2, $redis.zcard("timers:test_timer:mean")
    end
  end

end
