require "redis"
require "logger"
require_relative "../../src/constants"
require_relative "../../src/Producer"
require_relative "../../src/table/ProducerMessageTable"

describe Producer do
    subject!(:messageTable) { instance_double(ProducerMessageTable) }
    subject!(:broker) { instance_double(Redis) }
    subject!(:logger) { instance_double(Logger) }
    subject!(:dbPool) { instance_double(SQLite3::Database) }

    subject!(:producer) { Producer.new(messageTable, broker, logger) }

    describe "#manageUnsentMessages" do
        it "successfully receives three messages and send them to the queue" do
            unsentMessages = [
                ["1", "sendingTime1", "message1"],
                ["2", "sendingTime2", "message2"],
                ["3", "sendingTime3", "message3"],
            ]
            allow(producer).to receive(:getUnsentMessages).with(dbPool).and_return(unsentMessages)
            allow(producer).to receive(:sendToBrokerQueue).with(dbPool, "1", "sendingTime1", "message1")
            allow(producer).to receive(:sendToBrokerQueue).with(dbPool, "2", "sendingTime2", "message2")
            allow(producer).to receive(:sendToBrokerQueue).with(dbPool, "3", "sendingTime3", "message3")

            subject.manageUnsentMessages(dbPool)
        end
    end

    describe "#getUnsentMessage" do
        it "succesfully returns messages from message table" do
            expectedMessages = [
                ["1", "sendingTime1", "message1"],
                ["2", "sendingTime2", "message2"],
                ["3", "sendingTime3", "message3"],
            ]
            allow(messageTable).to receive(:getMessagesToSend).with(dbPool).and_return(expectedMessages)

            actualMessages = subject.getUnsentMessages(dbPool)

            expect(actualMessages).to eq(expectedMessages)
        end
    end

    describe "sendToBrokerQueue" do
        id, sendingTime, message = "1", "sendingTime", "message"
        messageToSend = "{\"id\":\"#{id}\", \"sendingTime\":\"#{sendingTime}\", \"message\":\"#{message}\"}"

        it "successfully send message to the broker" do
            allow(broker).to receive(:rpush).with(QUEUE_NAME, messageToSend).and_return(1)
            allow(messageTable).to receive(:markAsSent).with(dbPool, id)
            allow(logger).to receive(:info).with("The message was correctly delivered to the broker")

            subject.sendToBrokerQueue(dbPool, id, sendingTime, message)
        end

        it "push rejected, doesn't throw an error" do
            allow(broker).to receive(:rpush).with(QUEUE_NAME, messageToSend).and_return(0)

            subject.sendToBrokerQueue(dbPool, id, sendingTime, message)

            expect(messageTable).not_to receive(:markAsSent)
            expect(logger).not_to receive(:info)
            expect(logger).not_to receive(:error)
        end

        it "push rejected, throws a connection error" do
            allow(broker).to receive(:rpush).with(QUEUE_NAME, messageToSend).and_raise(Redis::CannotConnectError)
            allow(logger).to receive(:error).with("The broker is unreachable at the moment")

            subject.sendToBrokerQueue(dbPool, id, sendingTime, message)

            expect(messageTable).not_to receive(:markAsSent)
            expect(logger).not_to receive(:info)
        end
    end

    describe "#manageNewMessageInsertion" do
        it "not a retry, message different from exit, correct insertion into the db" do
            isRetry = false

            allow(producer).to receive(:getUserInput).and_return("message")
            allow(producer).to receive(:insertNewMessageToDb).with(dbPool, anything, anything, "message").and_return(true)

            result = subject.manageNewMessageInsertion(dbPool, isRetry)

            expect(result).to eq(true)
        end

        it "not a retry, message equals exit" do
            isRetry = false

            allow(producer).to receive(:getUserInput).and_return("exit")

            result = subject.manageNewMessageInsertion(dbPool, isRetry)

            expect(result).to eq(false)
        end

        it "not a retry, message different from exit, first insertion to the db fails, the second goes well" do
            isRetry = false

            allow(producer).to receive(:getUserInput).and_return("first input trial")
            allow(producer).to receive(:getUserInput).and_return("second input trial")
            allow(producer).to receive(:insertNewMessageToDb).with(dbPool, anything, anything, "first input trial").and_return(false)
            allow(producer).to receive(:insertNewMessageToDb).with(dbPool, anything, anything, "second input trial").and_return(true)

            result = subject.manageNewMessageInsertion(dbPool, isRetry)

            expect(result).to eq(true)
        end
    end

    describe "#insertNewMessageToDb" do
        it "successfully inserted to the db" do
        key, sendingTime, message = "key", "sendingTime", "message"
            allow(messageTable).to receive(:insertMessage).with(dbPool, key, sendingTime, message).and_return(true)

            result = subject.insertNewMessageToDb(dbPool, key, sendingTime, message)

            expect(result).to eq(true)
        end
    end
end
