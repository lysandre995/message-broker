require "json"
require "logger"
require "redis"
require "sqlite3"
require_relative '../../src/Consumer'
require_relative '../../src/table/ConsumerMessageTable'
require_relative "../../src/constants"

describe Consumer do
    subject!(:messagesTable) { instance_double(ConsumerMessageTable) }
    subject!(:broker) { instance_double(Redis) }
    subject!(:logger) { instance_double(Logger) }
    subject!(:dbPool) { instance_double(SQLite3::Database) }

    subject!(:consumer) { Consumer.new(messagesTable, broker, logger) }

    describe "#manageMessageArrival" do
        it "successful execution" do
            message = '{"id": "1", "sendingTime": "2023-09-03T10:00:00Z", "message": "Test message"}'
            allow(consumer).to receive(:consumeMessageFromQueue).with(no_args).and_return(message)
            allow(logger).to receive(:info).with("Received: id => 1, sendingTime => 2023-09-03T10:00:00Z, message => Test message")
            allow(consumer).to receive(:insertMessageIntoDb).with(dbPool, "1", "2023-09-03T10:00:00Z", anything, "Test message").and_return(true)

            subject.manageMessageArrival(dbPool)
        end
    end

    describe "#consumerMessageArrival" do
        it "broker successful pop message from the queue" do
            expectedMessage = "Test message"
            messageArray = [QUEUE_NAME, expectedMessage]
            allow(broker).to receive(:blpop).with(QUEUE_NAME).and_return(messageArray)

            actualMessage = subject.consumeMessageFromQueue
            expect(actualMessage).to eq(expectedMessage)
        end

        it "broker returns connection error" do
            allow(broker).to receive(:blpop).and_raise(Redis::CannotConnectError)
            allow(logger).to receive(:error).with("The broker is unreachable at the moment")

            subject.consumeMessageFromQueue
        end
    end

    describe "#insertMessageIntoDb" do
        it "succesful execution" do
            id, sendingTime, receivingTime, message = "1", "2023-09-03T10:00:00Z", "2023-09-03T10:01:00Z", "Test message"
            allow(messagesTable).to receive(:insertMessage).with(dbPool, id, sendingTime, receivingTime, message)

            subject.insertMessageIntoDb(dbPool, id, sendingTime, receivingTime, message)
        end
    end
end
