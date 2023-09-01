require 'redis'
require 'sqlite3'

QUEUE_NAME = 'message_queue'

redis = Redis.new

def tableCreation(dbPool)
    sqlFile = 'sql/producer_messages_table.sql'

    sqlScript = File.read(sqlFile)

    dbPool.execute(sqlScript)
end

def insertMessage(dbPool, sendingTime, message)
    puts "#{sendingTime}: #{message}"
    dbPool.execute("INSERT INTO messages (sending_time, content, is_sent) VALUES ('#{sendingTime}', '#{message}', FALSE);")
end

db = SQLite3::Database.new "producer.db"
tableCreation(db)


loop do
  puts "Enter a message (or 'exit' to quit):"
  message = gets.chomp
  break if message == 'exit'

  sendingTime = Time.now.to_s

  insertMessage(db, sendingTime, message)

  key = sendingTime + message

  result = redis.set(key.hash, message)
  
  if result == "OK" then
    puts "Message sent to the broker"
  end
end

redis.quit