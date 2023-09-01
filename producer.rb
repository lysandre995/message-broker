require "redis"
require "logger"
require_relative "src/table/ProducerMessageTable"
require_relative "src/service/ProducerDatabaseService"
require_relative 'src/Producer'

logger = Logger.new("log/producer.log")
producerMessageTable = ProducerMessageTable.new(logger)
databaseService = ProducerDatabaseService.new(producerMessageTable)
broker = Redis.new
broker.config("SET", "appendonly", "yes")

producer = Producer.new(producerMessageTable, broker, logger)

loop do
    begin
        dbPool = databaseService.databaseConnection
        producer.manageUnsentMessages(dbPool)
        producer.manageNewMessageInsertion(dbPool)
    rescue => e
        logger.error "Error during program execution: #{e.message}"
    ensure
        dbPool.close
    end
end

broker.quit
