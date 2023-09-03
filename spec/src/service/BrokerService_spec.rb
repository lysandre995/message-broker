require "redis"
require "logger"
require_relative "../../../src/service/BrokerService"

describe BrokerService do
    subject!(:broker) { instance_double(Redis) }
    subject!(:logger) { instance_double(Logger) }

    subject!(:brokerService) { BrokerService.new(broker, logger) }

    describe "#ensureBrokerPersistence" do
        it "broker service is running and appendonly is set to no" do
            cfg = ["appendonly", "no"]

            allow(broker).to receive(:ping).and_return("PONG")
            allow(broker).to receive(:config).with("GET", "appendonly").and_return(cfg)
            allow(broker).to receive(:config).with("SET", "appendonly", "yes")

            subject.ensureBrokerPersistence
        end

        it "broker service is running and appendonly is set to yes" do
            cfg = ["appendonly", "yes"]

            allow(broker).to receive(:ping).and_return("PONG")
            allow(broker).to receive(:config).with("GET", "appendonly").and_return(cfg)

            subject.ensureBrokerPersistence

            expect(broker).not_to receive(:config).with("SET", "appendonly", "yes")
        end

        it "broker service is not running, doesn't throw a connection error" do
            allow(broker).to receive(:ping).and_return(nil)

            subject.ensureBrokerPersistence
            expect(broker).not_to receive(:config).with("SET", "appendonly", "yes")
        end

        it "broker service is not running, throws a connection error" do
            allow(broker).to receive(:ping).and_raise(Redis::CannotConnectError)
            allow(logger).to receive(:error).with("The broker is unreachable at the moment")

            subject.ensureBrokerPersistence
        end
    end
end
