require "json"
require_relative 'constants'

class Consumer
    def initialize(messagesTable, broker, logger)
        @messagesTable = messagesTable
        @broker = broker
        @logger = logger
    end

    def manageMessageArrival(dbPool)
        message = consumeMessageFromQueue
        receivingTime = Time.now.to_s
        if message then
            jsonObject = JSON.parse(message)
            id = jsonObject["id"]
            message = jsonObject["message"]
            @logger.info "Received: id => #{id}, message => #{message}"
            if insertMessageIntoDb(dbPool, id, receivingTime, message) then
                puts "Received message: #{message}"
            end
        end
    end

    def consumeMessageFromQueue
        begin
            return @broker.blpop(QUEUE_NAME)[1]
        rescue Redis::CannotConnectError => e
            @logger.error "The broker is unreachable at the moment"
        end
    end

    def insertMessageIntoDb(dbPool, id, receivingTime, message)
        @messagesTable.insertMessage(dbPool, id, receivingTime, message)
    end
end
