CREATE TABLE IF NOT EXISTS messages (
    sending_time TIMESTAMP NOT NULL,
    content TEXT,
    is_sent BOOLEAN DEFAULT FALSE
);
