require 'rubygems'
require 'test/unit'
require 'redis'
require 'statsd/server'
require 'socket'
require 'fileutils'

class RedisStoreTest < Test::Unit::TestCase
  def setup
    @thread = Thread.new do
      Statsd::Server::Daemon.new.run({:redis => true, :config => "test/config.yml"})
    end
    @thread.run
    @redis = Redis.new
    @socket = UDPSocket.new
  end

  def teardown
    @thread.kill
  end

  def test_creating_a_counter_datapoint_creates_a_single_redis_key
    @redis.keys("*statsd_test*").each{|k| @redis.del k}
    assert_equal 0, @redis.keys("*statsd_test*").length
    @socket.send("statsd_test:1|c", 0, "127.0.0.1", 8125)
    10.times { @thread.run; sleep 1  }
    assert_equal 1, @redis.keys("*statsd_test*").length
  end
  
  def test_creating_a_counter_datapoint_writes_twice_to_file
    Dir.glob("test/data/**/**/**").each {|f| FileUtils.rm_rf(f)}
    assert_equal 0,  Dir.glob("test/data/**/**/**").length
    @socket.send("statsd_test:1|c", 0, "127.0.0.1", 8125)
    20.times { @thread.run; sleep 0.5 }
    assert_equal 6, Dir.glob("test/data/**/**/**").length
  end

end
