UPDATE ${prefix}bolt_outbox
SET status = 'PUBLISHED', lease_owner = NULL, lease_until = NULL, last_error = NULL
WHERE published_at IS NOT NULL AND status <> 'PUBLISHED';
