require 'rubygems'
require 'test/unit'
require 'statsd_server'
require 'statsd_server/server'

class DiskQueueTest < Test::Unit::TestCase

  def test_writer_gets_job_from_queue_and_sends_to_diskstore
    assert false
  end

  def test_writer_sleeps_when_there_are_no_jobs_for_it
    assert false
  end

end
