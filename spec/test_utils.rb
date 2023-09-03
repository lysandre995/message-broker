require "sqlite3"

class TestUtils
    def createTestDatabase
        return SQLite3::Database.new("test.db")
    end

    def deleteTestDatabase
        filePath = "test.db"
        if File.exist?(filePath) then
            File.delete(filePath)
        end
    end
end
