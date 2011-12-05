require 'rubygems'
require 'test/unit'
require 'redis'
require 'timecop'
require 'mocha'
require 'statsd_server'
require 'statsd_server/server'

class RedisCustom < Redis
  alias :normal_smembers :smembers
  def smembers(key, &f)
    normal_smembers(key).tap(&f)
  end
  alias :normal_zrangebyscore :zrangebyscore
  def zrangebyscore(key, low, high, &f)
    normal_zrangebyscore(key, low, high).tap(&f)
  end
  alias :normal_rpop :rpop
  def rpop(key, &f)
    normal_rpop(key).tap(&f)
  end

end

class AggregationTest < Test::Unit::TestCase
  
  def setup
    options = {:config => "test/config.yml"}
    ENV["silent"] = "true"
    $config = YAML::load(ERB.new(IO.read(options[:config])).result)
    $config["retention"] = $config["retention"].split(",").collect{|r| retention = {}; retention[:interval], retention[:count] = r.split(":").map(&:to_i); retention }
    $redis = RedisCustom.new({:host => $config["redis_host"], :port => $config["redis_port"]})
    $redis_nonem = Redis.new({:host =>$config["redis_host"], :port => $config["redis_port"]})
  end

  def teardown
    $redis.keys.each{|k| $redis.del k}
    $counters = {}
    $timers = {}
  end

  def test_mean_aggregation
    values = [1,2,3,4]
    assert_equal 2.5, StatsdServer::Aggregation.new("", 10, "mean").calculate_aggregation(values)
  end
  
  def test_max_aggregation
    values = [1,2,3,4]
    assert_equal 4, StatsdServer::Aggregation.new("", 10, "max").calculate_aggregation(values)
  end

  def test_min_aggregation
    values = [1,2,3,4]
    assert_equal 1, StatsdServer::Aggregation.new("", 10, "min").calculate_aggregation(values)
  end

  def test_sum_aggregation
    values = [1,2,3,4]
    assert_equal 10, StatsdServer::Aggregation.new("", 10, "sum").calculate_aggregation(values)
  end

  def test_percentile_aggregation
    values = (1..20).to_a
    assert_equal 19, StatsdServer::Aggregation.new("", 10, "percentile_90").calculate_aggregation(values)
    assert_equal 11, StatsdServer::Aggregation.new("", 10, "percentile_50").calculate_aggregation(values)
  end
  
  def test_mean_squared_aggregation
    values = (1..20).to_a
    assert_equal 665, StatsdServer::Aggregation.new("", 10, "mean_squared").calculate_aggregation(values)
  end

  def test_standard_deviation
    values = (1..20).to_a
    assert_equal 5.91608, StatsdServer::Aggregation.new("", 10, "standard_dev").calculate_aggregation(values).round(5)
  end

  def test_aggregating_pending_aggregates_and_clears_all_pending_metrics
    $redis.sadd "needsAggregated:60", "test1"
    $redis.sadd "needsAggregated:600", "test1"
    StatsdServer::Diskstore.stubs(:"store!").returns(true)
    StatsdServer::Aggregation.aggregate_pending!(60)
    assert_equal 0, $redis.llen("needsAggregated:60")
    StatsdServer::Aggregation.aggregate_pending!(600)
    assert_equal 0, $redis.scard("needsAggregated:600")
  end

  def test_calculating_aggregates_over_time_periods_captures_full_dataset
    StatsdServer::UDP.parse_incoming_message("test_counter:1|c")
    StatsdServer::RedisStore.flush!($counters, {})
    Timecop.freeze(Time.now + 15) do
      StatsdServer::UDP.parse_incoming_message("test_counter:1|c")
      StatsdServer::RedisStore.flush!($counters, {})
      Timecop.freeze(Time.now + 15) do
        StatsdServer::UDP.parse_incoming_message("test_counter:1|c")
        StatsdServer::RedisStore.flush!($counters, {})
        aggregation = StatsdServer::Aggregation.new("counters:test_counter", 60, "sum")
        aggregation.calculate_aggregation do |result|
          assert_equal 3, result 
        end
      end
    end
  end

  def test_storing_an_aggregation_queues_it_to_worker
    assert_equal 0, $redis.llen("aggregationQueue")
    StatsdServer::UDP.parse_incoming_message("test_counter:1|c")
    StatsdServer::RedisStore.flush!($counters, {})
    StatsdServer::Aggregation.aggregate_pending!(60)
    assert_equal 1, $redis.llen("aggregationQueue")
  end

end
