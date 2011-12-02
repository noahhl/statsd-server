require 'rubygems'
require 'test/unit'
require 'statsd_server/server'

class UDPParsingTest < Test::Unit::TestCase

  def test_sending_a_counter_message_adds_to_counters
    assert_empty $counters
    StatsdServer::UDP.parse_incoming_message("test_counter:1|c")
    assert_equal 1, $counters.size
    StatsdServer::UDP.parse_incoming_message("test_counter2:1|c")
    assert_equal 2, $counters.size
    StatsdServer::UDP.parse_incoming_message("test_counter:2|c")
    assert_equal 2, $counters.size
    assert_equal 3, $counters["test_counter"]
  end
  
  def test_sending_a_timer_message_adds_to_timers
    assert_empty $timers
    StatsdServer::UDP.parse_incoming_message("test_timer:1|ms")
    assert_equal 1, $timers.size
    StatsdServer::UDP.parse_incoming_message("test_timer2:1|ms")
    assert_equal 2, $timers.size
    StatsdServer::UDP.parse_incoming_message("test_timer:2|ms")
    assert_equal 2, $timers.size
    assert_equal 2, $timers["test_timer"].length
  end

  def test_requesting_info_returns_details_about_the_server
    skip "not implemented yet"
  end

end
