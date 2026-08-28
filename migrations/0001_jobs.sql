-- Durable job queue used by every application replica.
-- The application passes job_type and status as their stable snake_case text
-- representation. A later migration may add new values, but changing or
-- removing values requires an explicit data migration.

-- Allocated before upstream article acquisition starts. Gaps are expected when
-- a worker reserves a version but fails before persisting its observation.
CREATE SEQUENCE IF NOT EXISTS article_observation_version_seq
    AS BIGINT
    START WITH 1
    INCREMENT BY 1
    MINVALUE 1;

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

-- Source scheduling state is durable so every application replica observes
-- the same due time, gate, cooldown, and short enqueue reservation. The
-- source identity and configuration live in this same initial aggregate so
-- source creation and scheduling can share one transaction boundary.
CREATE TABLE IF NOT EXISTS sources (
    id UUID PRIMARY KEY,
    book_id TEXT NOT NULL CHECK (btrim(book_id) <> ''),
    display_name TEXT NOT NULL CHECK (btrim(display_name) <> ''),
    article_url TEXT NOT NULL CHECK (btrim(article_url) <> ''),
    account_id UUID,
    feed_revision BIGINT NOT NULL DEFAULT 0 CHECK (feed_revision >= 0),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    scheduling_gate TEXT NOT NULL DEFAULT 'ready' CHECK (
        scheduling_gate IN ('ready', 'authentication_required', 'risk_controlled')
    ),
    sync_interval_seconds BIGINT NOT NULL DEFAULT 3600 CHECK (sync_interval_seconds > 0),
    rss_item_limit BIGINT NOT NULL DEFAULT 50 CHECK (rss_item_limit > 0),
    next_fetch_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    failure_cooldown_until TIMESTAMPTZ,
    schedule_reserved_until TIMESTAMPTZ,
    priority INTEGER NOT NULL DEFAULT 0,
    max_attempts BIGINT NOT NULL DEFAULT 3 CHECK (max_attempts > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS sources_book_id_idx
    ON sources (book_id);

CREATE INDEX IF NOT EXISTS sources_due_idx
    ON sources (next_fetch_at ASC, priority DESC, id ASC)
    WHERE enabled AND scheduling_gate = 'ready';

-- Normalized article state is keyed by source and the stable upstream
-- review_id. Content is already sanitized before insertion. Version one keeps
-- asset bytes outside this table; optional asset archiving can add separate
-- metadata and relationships later.
CREATE TABLE IF NOT EXISTS articles (
    source_id UUID NOT NULL REFERENCES sources (id) ON DELETE CASCADE,
    review_id TEXT NOT NULL CHECK (btrim(review_id) <> ''),
    title TEXT NOT NULL CHECK (btrim(title) <> ''),
    author TEXT,
    summary TEXT,
    cover_url TEXT,
    original_url TEXT,
    published_at TIMESTAMPTZ NOT NULL,
    content_html TEXT NOT NULL DEFAULT '',
    content_hash TEXT CHECK (content_hash IS NULL OR btrim(content_hash) <> ''),
    observation_version BIGINT NOT NULL DEFAULT nextval('article_observation_version_seq')
        CHECK (observation_version > 0),
    fetched_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_id, review_id),
    CHECK (cover_url IS NULL OR btrim(cover_url) <> ''),
    CHECK (original_url IS NULL OR btrim(original_url) <> '')
);

CREATE INDEX IF NOT EXISTS articles_feed_order_idx
    ON articles (source_id, published_at DESC, review_id ASC);

-- One current rendered document per source. XML is stored as bytes so the
-- HTTP layer can return it without another serialization pass.
CREATE TABLE IF NOT EXISTS feed_cache (
    source_id UUID PRIMARY KEY REFERENCES sources (id) ON DELETE CASCADE,
    xml_bytes BYTEA NOT NULL,
    etag TEXT NOT NULL CHECK (btrim(etag) <> ''),
    generated_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    feed_revision BIGINT NOT NULL CHECK (feed_revision >= 0),
    content_hash TEXT NOT NULL CHECK (btrim(content_hash) <> ''),
    updated_at TIMESTAMPTZ NOT NULL,
    CHECK (expires_at > generated_at)
);

CREATE INDEX IF NOT EXISTS feed_cache_expiry_idx
    ON feed_cache (expires_at ASC, source_id ASC);

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

-- A build lease prevents concurrent RSS cache-miss requests from all rendering
-- the same source.
CREATE TABLE IF NOT EXISTS feed_build_leases (
    source_id UUID PRIMARY KEY REFERENCES sources (id) ON DELETE CASCADE,
    lease_owner TEXT NOT NULL CHECK (btrim(lease_owner) <> ''),
    lease_token UUID NOT NULL,
    lease_until TIMESTAMPTZ NOT NULL,
    heartbeat_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS feed_build_leases_expiry_idx
    ON feed_build_leases (lease_until ASC, source_id ASC);
