class ConsumerMessageTable
    def initialize(logger)
        @logger = logger
    end

    def createTableIfDoesntExist(dbPool)
        begin
            dbPool.execute <<-SQL
              CREATE TABLE IF NOT EXISTS messages (
                  id TEXT PRIMARY KEY NOT NULL,
                  sending_time TEXT NOT NULL,
                  receiving_time TEXT NOT NULL,
                  content TEXT
              );
            SQL
        rescue SQLite3::Exception => e
            @logger.error "Error during table creation: #{e.message}"
            return false
        end
    end

    def insertMessage(dbPool, id, sendingTime, receivingTime, message)
        begin
            dbPool.execute <<-SQL
              INSERT INTO messages (id, sending_time, receiving_time, content)
                   VALUES ('#{id}', '#{sendingTime}', '#{receivingTime}', '#{message}');
            SQL
            @logger.info "Message #{id} correctly inserted into messages table"
            return true
        rescue SQLite3::Exception => e
            @logger.error "Error during message insertion: #{e.message}"
            return false
        end
    end
end
