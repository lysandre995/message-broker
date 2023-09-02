class BrokerService

    def initialize(broker, logger)
        @broker = broker
        @logger = logger
    end

    def ensureBrokerPersistence()
        begin
            if @broker.ping == "PONG" and !(@broker.config('GET', 'appendonly')[1] == "yes") then
                @broker.config("SET", "appendonly", "yes")
            end
        rescue Redis::CannotConnectError => e
            @logger.error "The broker is unreachable at the moment"
        end
    end
end
