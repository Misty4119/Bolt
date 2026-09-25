ALTER TABLE ${prefix}bolt_worlds ADD COLUMN status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE';
ALTER TABLE ${prefix}bolt_outbox ADD COLUMN status VARCHAR(32) NOT NULL DEFAULT 'PENDING';
ALTER TABLE ${prefix}bolt_outbox ADD COLUMN next_attempt_at BIGINT NOT NULL DEFAULT 0;
ALTER TABLE ${prefix}bolt_outbox ADD COLUMN lease_owner VARCHAR(128);
ALTER TABLE ${prefix}bolt_outbox ADD COLUMN lease_until BIGINT;
ALTER TABLE ${prefix}bolt_outbox ADD COLUMN last_error TEXT;
CREATE TABLE IF NOT EXISTS ${prefix}bolt_watermarks (aggregate_type VARCHAR(64) NOT NULL, aggregate_id VARCHAR(255) NOT NULL, aggregate_version BIGINT NOT NULL, updated_at BIGINT NOT NULL, PRIMARY KEY (aggregate_type, aggregate_id));
DROP INDEX IF EXISTS ${prefix}bolt_outbox_pending;
CREATE INDEX IF NOT EXISTS ${prefix}bolt_outbox_pending ON ${prefix}bolt_outbox(status, next_attempt_at, created_at);
