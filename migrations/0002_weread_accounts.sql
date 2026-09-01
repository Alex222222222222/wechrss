-- Encrypted WeRead account credentials. The account lease remains a separate
-- coordination table and never contains credential material.
CREATE TABLE IF NOT EXISTS weread_accounts (
    account_id UUID PRIMARY KEY,
    display_name TEXT NOT NULL CHECK (btrim(display_name) <> ''),
    credentials_ciphertext BYTEA NOT NULL CHECK (octet_length(credentials_ciphertext) >= 1),
    access_expires_at TIMESTAMPTZ NOT NULL,
    credential_version BIGINT NOT NULL DEFAULT 1 CHECK (credential_version > 0),
    disabled BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS weread_accounts_expiry_idx
    ON weread_accounts (access_expires_at ASC, account_id ASC)
    WHERE NOT disabled;
