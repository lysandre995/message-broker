require "redis"
require "logger"
require "fileutils"
require_relative "src/table/ProducerMessageTable"
require_relative "src/service/ProducerDatabaseService"
require_relative "src/Producer"
require_relative "src/service/BrokerService"

FileUtils.mkdir_p("log")
logger = Logger.new("log/producer.log")
producerMessageTable = ProducerMessageTable.new(logger)
databaseService = ProducerDatabaseService.new(producerMessageTable)
broker = Redis.new
brokerService = BrokerService.new(broker, logger)

producer = Producer.new(producerMessageTable, broker, logger)

loop do
    begin
        dbPool = databaseService.databaseConnection
        brokerService.ensureBrokerPersistence()
        break if !producer.manageNewMessageInsertion(dbPool, false)
        producer.manageUnsentMessages(dbPool)
    rescue => e
        logger.error "Error during program execution: #{e.message}"
    ensure
        dbPool.close
    end
end

broker.quit
