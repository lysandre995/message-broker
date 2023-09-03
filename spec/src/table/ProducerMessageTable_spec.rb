require "logger"
require "sqlite3"
require_relative "../../test_utils"
require_relative "../../../src/table/ProducerMessageTable"

describe ProducerMessageTable do
    testUtils = TestUtils.new
    subject!(:logger) { instance_double(Logger) }
    subject!(:dbPool) { testUtils.createTestDatabase }

    subject!(:producerMessageTable) { ProducerMessageTable.new(logger) }

    after(:each) do
        dbPool.close
        testUtils.deleteTestDatabase
    end

    describe "#createTableIfDoesntExist" do
        it "correctly create the table" do
            subject.createTableIfDoesntExist(dbPool)
            tableName = "messages"
            tableExists = dbPool.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", tableName)

            expect(tableExists.empty?).to eq(false)
        end

        it "error during query execution" do
            allow(dbPool).to receive(:execute).and_raise(SQLite3::Exception)
            allow(logger).to receive(:error).with("Error during table creation: SQLite3::Exception")

            subject.createTableIfDoesntExist(dbPool)
        end
    end

    describe "#insertMessage" do
        it "correctly inserts the data" do
            id, sendingTime, content = "1", "sendingTime", "content"

            allow(logger).to receive(:info).with("Message #{id} correctly inserted into messages table")

            subject.createTableIfDoesntExist(dbPool)
            result = subject.insertMessage(dbPool, id, sendingTime, content)

            expect(result).to eq(true)

            queryResult = dbPool.execute("SELECT * FROM messages;")

            expect(queryResult.size).to eq(1)
            expect(queryResult[0][0]).to eq(id)
            expect(queryResult[0][1]).to eq(sendingTime)
            expect(queryResult[0][2]).to eq(content)
            expect(queryResult[0][3]).to eq(0)
        end

        it "reinsert the same record" do
            id, sendingTime, content = "1", "sendingTime", "content"

            allow(logger).to receive(:info).with("Message #{id} correctly inserted into messages table")
            allow(logger).to receive(:error).with("Error during message insertion: UNIQUE constraint failed: messages.id")

            subject.createTableIfDoesntExist(dbPool)
            result = subject.insertMessage(dbPool, id, sendingTime, content)
            expect(result).to eq(true)
            result = subject.insertMessage(dbPool, id, sendingTime, content)
            expect(result).to eq(false)

            queryResult = dbPool.execute("SELECT * FROM messages;")

            expect(queryResult.size).to eq(1)
            expect(queryResult[0][0]).to eq(id)
            expect(queryResult[0][1]).to eq(sendingTime)
            expect(queryResult[0][2]).to eq(content)
            expect(queryResult[0][3]).to eq(0)
        end

        it "query error" do
            id, sendingTime, content = "\'", 'sendingTime', "content"

            allow(logger).to receive(:error).with("Error during message insertion: near \"sendingTime\": syntax error")

            subject.createTableIfDoesntExist(dbPool)
            result = subject.insertMessage(dbPool, id, sendingTime, content)

            expect(result).to eq(false)

            queryResult = dbPool.execute("SELECT * FROM messages;")

            expect(queryResult.size).to eq(0)
        end
    end

    describe "#markAsSent" do
        it "correctly updates the data" do
            id, sendingTime, content = "1", 'sendingTime', "content"

            allow(logger).to receive(:info).with("Message 1 correctly inserted into messages table")
            allow(logger).to receive(:info).with("Message marked as sent (id: 1)")

            subject.createTableIfDoesntExist(dbPool)
            subject.insertMessage(dbPool, id, sendingTime, content)
            result = subject.markAsSent(dbPool, id)

            expect(result).to eq(true)

            queryResult = dbPool.execute("SELECT * FROM messages;")

            expect(queryResult.size).to eq(1)
            expect(queryResult[0][0]).to eq(id)
            expect(queryResult[0][1]).to eq(sendingTime)
            expect(queryResult[0][2]).to eq(content)
            expect(queryResult[0][3]).to eq(1)
        end

        it "update a non existent id" do
            id, sendingTime, content = "1", 'sendingTime', "content"

            allow(logger).to receive(:info).with("Message 1 correctly inserted into messages table")
            allow(logger).to receive(:error).with("Error during table updating: the specified id (10) doesn't exist in the table messages")

            subject.createTableIfDoesntExist(dbPool)
            subject.insertMessage(dbPool, id, sendingTime, content)
            result = subject.markAsSent(dbPool, "10")

            expect(result).to eq(false)
        end

        it "query error" do
            id, sendingTime, content = "'", 'sendingTime', "content"

            allow(logger).to receive(:error).with("Error during table updating: unrecognized token: \"''';\"")

            subject.createTableIfDoesntExist(dbPool)
            result = subject.markAsSent(dbPool, id)

            expect(result).to eq(false)
        end
    end

    describe "#getMessagesToSend" do
        it "correctly get the message" do
            id, sendingTime, content = "1", 'sendingTime', "content"

            allow(logger).to receive(:info).with("Message 1 correctly inserted into messages table")

            subject.createTableIfDoesntExist(dbPool)
            subject.insertMessage(dbPool, id, sendingTime, content)

            result = subject.getMessagesToSend(dbPool)

            expect(result.size).to eq(1)
            expect(result[0][0]).to eq("1")
            expect(result[0][1]).to eq("sendingTime")
            expect(result[0][2]).to eq("content")
            expect(result[0][3]).to eq(0)
        end

        it "error during query execution" do
            allow(dbPool).to receive(:execute).and_raise(SQLite3::Exception)
            allow(logger).to receive(:error).with("Error during message fetching: SQLite3::Exception")

            subject.getMessagesToSend(dbPool)
        end
    end
end
