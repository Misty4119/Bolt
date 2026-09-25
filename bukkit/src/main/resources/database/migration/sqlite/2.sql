ALTER TABLE ${prefix}bolt_worlds ADD COLUMN status TEXT NOT NULL DEFAULT 'ACTIVE';
ALTER TABLE ${prefix}bolt_outbox ADD COLUMN status TEXT NOT NULL DEFAULT 'PENDING';
ALTER TABLE ${prefix}bolt_outbox ADD COLUMN next_attempt_at INTEGER NOT NULL DEFAULT 0;
ALTER TABLE ${prefix}bolt_outbox ADD COLUMN lease_owner TEXT;
ALTER TABLE ${prefix}bolt_outbox ADD COLUMN lease_until INTEGER;
ALTER TABLE ${prefix}bolt_outbox ADD COLUMN last_error TEXT;
CREATE TABLE IF NOT EXISTS ${prefix}bolt_watermarks (aggregate_type TEXT NOT NULL, aggregate_id TEXT NOT NULL, aggregate_version INTEGER NOT NULL, updated_at INTEGER NOT NULL, PRIMARY KEY (aggregate_type, aggregate_id));
DROP INDEX IF EXISTS ${prefix}bolt_outbox_pending;
CREATE INDEX IF NOT EXISTS ${prefix}bolt_outbox_pending ON ${prefix}bolt_outbox(status, next_attempt_at, created_at);
