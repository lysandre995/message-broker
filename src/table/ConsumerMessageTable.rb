class ConsumerMessageTable
    def initialize(logger)
        @logger = logger
    end
    def createTableIfDoesntExist(dbPool)
        begin
            dbPool.execute <<-SQL
              CREATE TABLE IF NOT EXISTS messages (
                  id TEXT PRIMARY KEY NOT NULL,
                  receiving_time TEXT NOT NULL,
                  content TEXT
              );
            SQL
        rescue SQLite3::Exception => e
            @logger.error "Error during table creation: #{e.message}"
            return false
        end
    end

    def insertMessage(dbPool, id, receivingTime, message)
        begin
            dbPool.execute <<-SQL
              INSERT INTO messages (id, receiving_time, content)
                   VALUES ('#{id}', '#{receivingTime}', '#{message}');
            SQL
            @logger.info "Message correctly loaded into the database"
            return true
        rescue SQLite3::Exception => e
            @logger.error "Error during insertion into database: #{e.message}"
            return false
        end
    end
end