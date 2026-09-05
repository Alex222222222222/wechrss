-- PostgreSQL-backed binary asset cache.  URL/version metadata survives a
-- binary eviction so a later repair can still identify the upstream asset.
CREATE TABLE IF NOT EXISTS asset_blobs (
    id UUID PRIMARY KEY,
    checksum_algorithm TEXT NOT NULL CHECK (checksum_algorithm = 'sha256'),
    checksum TEXT NOT NULL CHECK (length(checksum) = 64),
    byte_size BIGINT NOT NULL CHECK (byte_size >= 0),
    media_type TEXT NOT NULL CHECK (btrim(media_type) <> ''),
    data BYTEA,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_fetched_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_accessed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS asset_blobs_checksum_idx
    ON asset_blobs (checksum, byte_size, id)
    WHERE data IS NOT NULL;

CREATE TABLE IF NOT EXISTS asset_records (
    id UUID PRIMARY KEY,
    source_url TEXT NOT NULL CHECK (btrim(source_url) <> ''),
    version BIGINT NOT NULL CHECK (version > 0),
    final_url TEXT NOT NULL CHECK (btrim(final_url) <> ''),
    blob_id UUID REFERENCES asset_blobs (id) ON DELETE SET NULL,
    fetch_status TEXT NOT NULL DEFAULT 'available' CHECK (
        fetch_status IN ('available', 'missing', 'failed')
    ),
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (source_url, version)
);

CREATE INDEX IF NOT EXISTS asset_records_source_url_idx
    ON asset_records (source_url, version DESC);

CREATE TABLE IF NOT EXISTS article_assets (
    source_id UUID NOT NULL,
    review_id TEXT NOT NULL CHECK (btrim(review_id) <> ''),
    asset_record_id UUID NOT NULL REFERENCES asset_records (id) ON DELETE CASCADE,
    occurrence INTEGER NOT NULL CHECK (occurrence >= 0),
    role TEXT NOT NULL DEFAULT 'body' CHECK (btrim(role) <> ''),
    referer_url TEXT NOT NULL CHECK (btrim(referer_url) <> ''),
    origin TEXT,
    user_agent TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_id, review_id, occurrence),
    FOREIGN KEY (source_id, review_id)
        REFERENCES articles (source_id, review_id)
        ON DELETE CASCADE
        DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX IF NOT EXISTS article_assets_record_idx
    ON article_assets (asset_record_id);
