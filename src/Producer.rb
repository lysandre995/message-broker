require_relative "constants"

class Producer
    def initialize(messageTable, broker, logger)
        @messageTable = messageTable
        @broker = broker
        @logger = logger
    end

    def manageUnsentMessages(dbPool)
        getUnsentMessages(dbPool).each do |row|
            id, _, message = row
            sendToBrokerQueue(dbPool, id, message)
        end
    end

    def getUnsentMessages(dbPool)
        return @messageTable.getMessagesToSend(dbPool)
    end

    def sendToBrokerQueue(dbPool, id, message)
        messageToSend = "{\"id\":\"#{id}\",\"message\":\"#{message}\"}"
        if @broker.rpush(QUEUE_NAME, messageToSend) == 1 then
            @messageTable.markAsSent(dbPool, id)
            @logger.info "The message was correctly delivered to the broker"
        end
    end

    def manageNewMessageInsertion(dbPool)
        puts "Enter a message (or 'exit' to quit):"
        message = gets.chomp

        sendingTime = Time.now.to_s
        key = (sendingTime + message).hash
        if insertNewMessageToDb(dbPool, key, sendingTime, message) then
            sendToBrokerQueue(dbPool, key, message)
        end
    end

    def insertNewMessageToDb(dbPool, key, sendingTime, message)
        return @messageTable.insertMessage(dbPool, key, sendingTime, message)
    end
end