require "logger"
require "sqlite3"
require_relative "../../test_utils"
require_relative "../../../src/table/ConsumerMessageTable"

describe ConsumerMessageTable do
    testUtils = TestUtils.new
    subject!(:logger) { instance_double(Logger) }
    subject!(:dbPool) { testUtils.createTestDatabase }

    subject!(:producerMessageTable) { ConsumerMessageTable.new(logger) }

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
            id, sendingTime, receivingTime, content = "1", "sendingTime", "receivingTime", "content"

            allow(logger).to receive(:info).with("Message #{id} correctly inserted into messages table")

            subject.createTableIfDoesntExist(dbPool)
            result = subject.insertMessage(dbPool, id, sendingTime, receivingTime, content)

            expect(result).to eq(true)

            queryResult = dbPool.execute("SELECT * FROM messages;")

            expect(queryResult.size).to eq(1)
            expect(queryResult[0][0]).to eq(id)
            expect(queryResult[0][1]).to eq(sendingTime)
            expect(queryResult[0][2]).to eq(receivingTime)
            expect(queryResult[0][3]).to eq(content)
        end

        it "reinsert the same record" do
            id, sendingTime, receivingTime, content = "1", "sendingTime", "receivingTime", "content"

            allow(logger).to receive(:info).with("Message #{id} correctly inserted into messages table")
            allow(logger).to receive(:error).with("Error during message insertion: UNIQUE constraint failed: messages.id")

            subject.createTableIfDoesntExist(dbPool)
            result = subject.insertMessage(dbPool, id, sendingTime, receivingTime, content)
            expect(result).to eq(true)
            result = subject.insertMessage(dbPool, id, sendingTime, receivingTime, content)
            expect(result).to eq(false)

            queryResult = dbPool.execute("SELECT * FROM messages;")

            expect(queryResult.size).to eq(1)
            expect(queryResult[0][0]).to eq(id)
            expect(queryResult[0][1]).to eq(sendingTime)
            expect(queryResult[0][2]).to eq(receivingTime)
            expect(queryResult[0][3]).to eq(content)
        end

        it "query error" do
            id, sendingTime, receivingTime, content = "'", "sendingTime", "receivingTime", "content"

            allow(logger).to receive(:error).with("Error during message insertion: near \"sendingTime\": syntax error")

            subject.createTableIfDoesntExist(dbPool)
            result = subject.insertMessage(dbPool, id, sendingTime, receivingTime, content)

            expect(result).to eq(false)

            queryResult = dbPool.execute("SELECT * FROM messages;")

            expect(queryResult.size).to eq(0)
        end
    end
end
