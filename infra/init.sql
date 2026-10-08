CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE TABLE IF NOT EXISTS trades (
	event_id UUID NOT NULL, 
	exchange VARCHAR(32) NOT NULL, 
	symbol VARCHAR(32) NOT NULL, 
	price DOUBLE PRECISION NOT NULL, 
	quantity DOUBLE PRECISION NOT NULL, 
	quote_quantity DOUBLE PRECISION NOT NULL, 
	side VARCHAR(8) NOT NULL,
	is_buyer_maker BOOLEAN NOT NULL, 
	trade_time BIGINT NOT NULL, 
	ingest_time TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

SELECT create_hypertable('trades','ingest_time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_trades_symbol_time ON trades(symbol, ingest_time DESC);

CREATE TABLE IF NOT EXISTS alerts (
	id BIGSERIAL PRIMARY KEY, 
	event_id UUID NOT NULL,
	symbol VARCHAR(32) NOT NULL,
	exchange VARCHAR(32) NOT NULL, 
	z_score DOUBLE PRECISION NOT NULL,
	ewma DOUBLE PRECISION NOT NULL,
	ewma_var DOUBLE PRECISION NOT NULL, 
	signed_ofi DOUBLE PRECISION NOT NULL,
	side VARCHAR(8) NOT NULL,
	quantity DOUBLE PRECISION NOT NULL, 
	price DOUBLE PRECISION NOT NULL, 
	trade_time_ms BIGINT NOT NULL, 
	alerted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alerts_symbol_time ON alerts(symbol, alerted_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_zscore ON alerts (z_score DESC);

CREATE TABLE IF NOT EXISTS symbol_configs(
	symbol VARCHAR(32) PRIMARY KEY,
	z_threshold DOUBLE PRECISION NOT NULL DEFAULT 3.0,
	ewma_alpha DOUBLE PRECISION NOT NULL DEFAULT 0.1,
	min_trades INT NOT NULL DEFAULT 10,
	update_time TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO symbol_configs (symbol, z_threshold, ewma_alpha, min_trades)
VALUES ('BTCUSDT', 3.0, 0.1, 10), ('ETHUSDT', 3.0, 0.1, 10), ('DOGEUSDT', 2.5, 0.1, 10)
ON CONFLICT (symbol) DO NOTHING;

CREATE EXTENSION IF NOT EXISTS pgcrypto;



CREATE TABLE IF NOT EXISTS audit_logs (
    id BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(32) NOT NULL,
    actor VARCHAR(128) NOT NULL DEFAULT 'system',
    symbol VARCHAR(32),
    event_id UUID,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    previous_hash CHAR(64),
    entry_hash CHAR(64) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_audit_logs_created_at
    ON audit_logs (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_audit_logs_event_type
    ON audit_logs (event_type);

CREATE INDEX IF NOT EXISTS idx_audit_logs_symbol
    ON audit_logs (symbol);

CREATE OR REPLACE FUNCTION audit_log_hash_chain()
RETURNS TRIGGER AS $$
DECLARE
    last_hash CHAR(64);
    canonical TEXT;
BEGIN
    SELECT entry_hash
    INTO last_hash
    FROM audit_logs
    ORDER BY id DESC
    LIMIT 1;

    NEW.previous_hash := last_hash;

    canonical :=
        COALESCE(NEW.event_type, '') || '|' ||
        COALESCE(NEW.actor, '') || '|' ||
        COALESCE(NEW.symbol, '') || '|' ||
        COALESCE(NEW.event_id::TEXT, '') || '|' ||
        NEW.details::TEXT || '|' ||
        NEW.created_at::TEXT || '|' ||
        COALESCE(NEW.previous_hash, '');

    NEW.entry_hash := encode(digest(canonical, 'sha256'), 'hex');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_audit_log_hash_chain ON audit_logs;

CREATE TRIGGER trg_audit_log_hash_chain
BEFORE INSERT ON audit_logs
FOR EACH ROW
EXECUTE FUNCTION audit_log_hash_chain();
