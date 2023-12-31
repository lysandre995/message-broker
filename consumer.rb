require "redis"
require "logger"
require "fileutils"
require_relative "src/constants"
require_relative "src/service/ConsumerDatabaseService"
require_relative "src/table/ConsumerMessageTable"
require_relative "src/Consumer"
require_relative "src/service/BrokerService"

FileUtils.mkdir_p("log")
logger = Logger.new("log/consumer.log")
consumerMessageTable = ConsumerMessageTable.new(logger)
databaseService = ConsumerDatabaseService.new(consumerMessageTable)
broker = Redis.new
brokerService = BrokerService.new(broker, logger)

consumer = Consumer.new(consumerMessageTable, broker, logger)

loop do
    begin
        dbPool = databaseService.databaseConnection
        brokerService.ensureBrokerPersistence()
        consumer.manageMessageArrival(dbPool)
    rescue => e
        logger.error "Error during program execution: #{e.message}"
    ensure
        dbPool.close
    end
end

broker.quit
