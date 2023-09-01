require "sqlite3"

class ConsumerDatabaseService
    def initialize(messagesConsumerTable)
        @messagesConsumerTable = messagesConsumerTable
    end

    def databaseConnection()
        db = SQLite3::Database.new "consumer.db"
        @messagesConsumerTable.createTableIfDoesntExist(db)
        return db
    end
end