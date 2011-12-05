module StatsdServer
  module Server
    module InfoServer
      def receive_data(msg)
        msg.split("\n").each do |row|
          if row.match(/stats/i)
            $redis.llen("diskstoreQueue") do |disk_queue_size|
              $redis.llen("aggregationQueue") do |aggregation_queue_size|
              send_data <<-info
Uptime: #{(Time.now - $started).to_i}
Total statistics since restart: #{$num_stats}
Pending writes: #{disk_queue_size}
Pending aggregations: #{aggregation_queue_size}
Time since last cleanup: #{(Time.now - $last_cleanup).to_i}
EM threadpool size: #{EM.threadpool_size}
EM connection count: #{EM.connection_count}
EM max timers: #{EM.get_max_timers}
EM heartbeat interval: #{EM.heartbeat_interval}
              info
              end
            end
          elsif row.match(/counters/i)
            send_data "#{$counters.to_s}\n"
          elsif row.match(/timers/i)
            send_data "#{$timers.to_s}\n"
          elsif row.match(/quit/i)
            send_data "BYE"
            close_connection
          end
        end
      end
    end
  end
end
