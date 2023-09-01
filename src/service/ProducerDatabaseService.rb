require "sqlite3"

class ProducerDatabaseService
    def initialize(messagesProducerTable)
        @messagesProducerTable = messagesProducerTable
    end

    def databaseConnection()
        db = SQLite3::Database.new "producer.db"
        @messagesProducerTable.createTableIfDoesntExist(db)
        return db
    end
end