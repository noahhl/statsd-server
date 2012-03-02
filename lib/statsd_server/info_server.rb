module StatsdServer
  module Server
    module InfoServer
      def receive_data(msg)
        msg.split("\n").each do |row|
          if row.match(/stats/i)
            $redis.llen("diskstoreQueue") do |disk_queue_size|
              $redis.llen("aggregationQueue") do |aggregation_queue_size|
                $redis.llen("gaugeQueue") do |gauge_queue_size|
                  send_data <<-info
Uptime: #{(Time.now - $started).to_i}
Total statistics since restart: #{$num_stats}
Pending truncations: #{disk_queue_size}
Pending gauge writes: #{gauge_queue_size}
Pending aggregations: #{aggregation_queue_size}
Time since last cleanup: #{(Time.now - $last_cleanup).to_i}
Number of workers: #{$workers.count}
EM threadpool size: #{EM.threadpool_size}
EM connection count: #{EM.connection_count}
EM max timers: #{EM.get_max_timers}
EM heartbeat interval: #{EM.heartbeat_interval}
                  info
                end
              end
            end
          elsif row.match(/counters/i)
            send_data "#{$counters.to_s}\n"
          elsif row.match(/gauges/i)
            send_data "#{$gauges.to_s}\n"
          elsif row.match(/timers/i)
            send_data "#{$timers.to_s}\n"
          elsif row.match(/add_worker/i)
            $workers << fork { StatsdServer::Queue.work!($options) }
            send_data "OK\n"
          elsif row.match(/remove_worker/i)
            $workers.pop.tap{|pid| Process.kill "SIGKILL", pid}
            send_data "OK\n"
          elsif row.match(/quit|exit/i)
            send_data "BYE\n"
            close_connection
          end
        end
      end
    end
  end
end
