require_relative "constants"

class Producer
    def initialize(messageTable, broker, logger)
        @messageTable = messageTable
        @broker = broker
        @logger = logger
    end

    def manageUnsentMessages(dbPool)
        getUnsentMessages(dbPool).each do |row|
            id, sendingTime, message = row
            sendToBrokerQueue(dbPool, id, sendingTime, message)
        end
    end

    def getUnsentMessages(dbPool)
        return @messageTable.getMessagesToSend(dbPool)
    end

    def sendToBrokerQueue(dbPool, id, sendingTime, message)
        messageToSend = "{\"id\":\"#{id}\", \"sendingTime\":\"#{sendingTime}\", \"message\":\"#{message}\"}"
        begin
            if @broker.rpush(QUEUE_NAME, messageToSend) == 1 then
                @messageTable.markAsSent(dbPool, id)
                @logger.info "The message was correctly delivered to the broker"
            end
        rescue Redis::CannotConnectError => e
            @logger.error "The broker is unreachable at the moment"
        end
    end

    def manageNewMessageInsertion(dbPool, isRetry)
        if isRetry then
            puts "Error during message management, please retype the message (or type 'exit' to quit):"
        else
            puts "Enter a message (or 'exit' to quit):"
        end
        message = gets.chomp
        return false if message == "exit"

        sendingTime = Time.now.to_s
        key = (sendingTime + message).hash
        if !insertNewMessageToDb(dbPool, key, sendingTime, message) then
            manageNewMessageInsertion(dbPool, true)
        end
        return true
    end

    def insertNewMessageToDb(dbPool, key, sendingTime, message)
        return @messageTable.insertMessage(dbPool, key, sendingTime, message)
    end
end
