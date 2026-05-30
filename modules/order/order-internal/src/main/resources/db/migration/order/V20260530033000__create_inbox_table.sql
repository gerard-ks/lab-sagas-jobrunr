CREATE TABLE IF NOT EXISTS order_schema.inbox (
    message_id UUID NOT NULL,
    handler_name VARCHAR(255) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (message_id, handler_name)
);