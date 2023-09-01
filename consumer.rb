require 'redis'

QUEUE_NAME = 'message_queue'

redis = Redis.new

loop do
  message = redis.blpop(QUEUE_NAME)[1]
  puts "Received message: #{message}"
end

redis.quit
