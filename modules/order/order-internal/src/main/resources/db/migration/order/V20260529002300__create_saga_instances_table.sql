CREATE TABLE IF NOT EXISTS order_schema.saga_instances (
    saga_id UUID PRIMARY KEY,                -- C'est le correlation_id qui traverse tout le réseau NATS
    saga_type VARCHAR(100) NOT NULL,         -- ex: 'OrderCreationSaga'

    status VARCHAR(20) NOT NULL,             -- RUNNING, COMPLETED, FAILED, TIMED_OUT
    CONSTRAINT chk_saga_status CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED', 'TIMED_OUT')),

    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    timeout_at TIMESTAMPTZ NOT NULL,         -- Le garde-fou temporel

    last_step VARCHAR(100),                  -- Pour savoir jusqu'où on est allé et quoi annuler (ex: 'StockReserved')

    context JSONB NOT NULL                   -- Le Contexte : contient l'order_id, les retries, etc.
);

-- Index vital pour le Cron Job qui cherche les Sagas bloquées dans le temps !
CREATE INDEX IF NOT EXISTS idx_saga_timeout ON order_schema.saga_instances(timeout_at) WHERE status = 'RUNNING';