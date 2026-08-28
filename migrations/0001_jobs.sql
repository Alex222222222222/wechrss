-- Durable job queue used by every application replica.
-- The application passes job_type and status as their stable snake_case text
-- representation. A later migration may add new values, but changing or
-- removing values requires an explicit data migration.

CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY,
    job_type TEXT NOT NULL CHECK (
        job_type IN (
            'source_sync',
            'feed_rebuild',
            'article_backfill',
            'credential_refresh'
        )
    ),
    source_id UUID,
    status TEXT NOT NULL CHECK (
        status IN ('queued', 'running', 'retry_wait', 'deferred', 'succeeded', 'failed')
    ),
    priority INTEGER NOT NULL,
    run_after TIMESTAMPTZ NOT NULL,
    claim_count BIGINT NOT NULL CHECK (claim_count >= 0),
    failure_count BIGINT NOT NULL CHECK (failure_count >= 0),
    max_attempts BIGINT NOT NULL CHECK (max_attempts > 0),
    lease_owner TEXT,
    lease_token UUID,
    lease_until TIMESTAMPTZ,
    heartbeat_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    last_error TEXT,
    payload_json JSONB NOT NULL,
    dedupe_key TEXT NOT NULL CHECK (btrim(dedupe_key) <> ''),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CHECK (
        (
            status = 'running'
            AND claim_count > 0
            AND failure_count < max_attempts
            AND lease_owner IS NOT NULL
            AND btrim(lease_owner) <> ''
            AND lease_token IS NOT NULL
            AND lease_until IS NOT NULL
            AND heartbeat_at IS NOT NULL
            AND started_at IS NOT NULL
            AND finished_at IS NULL
        )
        OR (
            status IN ('queued', 'retry_wait', 'deferred')
            AND failure_count < max_attempts
            AND lease_owner IS NULL
            AND lease_token IS NULL
            AND lease_until IS NULL
            AND finished_at IS NULL
        )
        OR (
            status = 'succeeded'
            AND claim_count > 0
            AND failure_count < max_attempts
            AND lease_owner IS NULL
            AND lease_token IS NULL
            AND lease_until IS NULL
            AND started_at IS NOT NULL
            AND finished_at IS NOT NULL
        )
        OR (
            status = 'failed'
            AND failure_count <= max_attempts
            AND lease_owner IS NULL
            AND lease_token IS NULL
            AND lease_until IS NULL
            AND finished_at IS NOT NULL
        )
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS jobs_active_dedupe_key_idx
    ON jobs (dedupe_key)
    WHERE status IN ('queued', 'running', 'retry_wait', 'deferred');

CREATE INDEX IF NOT EXISTS jobs_claim_idx
    ON jobs (priority DESC, run_after ASC, created_at ASC, id ASC)
    WHERE status IN ('queued', 'retry_wait', 'deferred');

CREATE INDEX IF NOT EXISTS jobs_expired_lease_idx
    ON jobs (lease_until ASC, id ASC)
    WHERE status = 'running';

-- A lease row is the cross-replica mutex for one authenticated WeRead account.
-- Credential material is intentionally stored by a separate future account
-- table and never belongs in this coordination table.
CREATE TABLE IF NOT EXISTS account_leases (
    account_id UUID PRIMARY KEY,
    lease_owner TEXT NOT NULL CHECK (btrim(lease_owner) <> ''),
    lease_token UUID NOT NULL,
    lease_until TIMESTAMPTZ NOT NULL,
    heartbeat_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS account_leases_expiry_idx
    ON account_leases (lease_until ASC, account_id ASC);
