class ProducerMessageTable
    def initialize(logger)
        @logger = logger
    end

    def createTableIfDoesntExist(dbPool)
        begin
            dbPool.execute <<-SQL
                CREATE TABLE IF NOT EXISTS messages (
                  id TEXT PRIMARY KEY NOT NULL,
                  sending_time TEXT NOT NULL,
                  content TEXT NOT NULL,
                  is_sent BOOLEAN NOT NULL DEFAULT FALSE
                );
            SQL
        rescue SQLite3::Exception => e
            @logger.error "Error during table creation: #{e.message}"
        end
    end

    def insertMessage(dbPool, id, sendingTime, content)
        begin
            dbPool.execute <<-SQL
              INSERT INTO messages (id, sending_time, content, is_sent)
                   VALUES ('#{id}', '#{sendingTime}', '#{content}', FALSE);
            SQL
            @logger.info "Message #{id} correctly inserted into messages table"
            return true
        rescue SQLite3::Exception => e
            @logger.error "Error during message insertion: #{e.message}"
            return false
        end
    end

    def markAsSent(dbPool, id)
        begin
            checkIdExistence = dbPool.execute("SELECT * FROM messages WHERE id = '#{id}';")
            if !(checkIdExistence.size == 0) then
                result = dbPool.execute <<-SQL
                UPDATE messages
                    SET is_sent = TRUE
                WHERE id = '#{id}';
                SQL
                @logger.info "Message marked as sent (id: #{id})"
                return true
            else
                raise SQLite3::Exception, "the specified id (#{id}) doesn't exist in the table messages"
            end
        rescue SQLite3::Exception => e
            @logger.error "Error during table updating: #{e.message}"
            return false
        end
    end

    def getMessagesToSend(dbPool)
        begin
            return dbPool.execute <<-SQL
              SELECT *
                FROM messages
               WHERE is_sent = FALSE
            ORDER BY sending_time;
            SQL
        rescue SQLite3::Exception => e
            @logger.error "Error during message fetching: #{e.message}"
        end
    end
end
