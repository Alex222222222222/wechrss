//! Opaque public-feed token domain values.
//!
//! A feed token is the capability embedded in a public RSS URL. It is
//! intentionally unrelated to administrative sessions or upstream WeRead
//! credentials. The raw value is generated with 256 bits of randomness and is
//! returned only at issuance/rotation time; persistence receives only its
//! SHA-256 digest. This lets the service resolve a token without keeping a
//! bearer secret in PostgreSQL, logs, or error messages.
//!
//! Responsibilities: generate and strictly parse the URL-safe token format,
//! provide a redacted debug representation, and calculate the storage lookup
//! digest. Non-responsibilities include token persistence, source ownership,
//! HTTP path extraction, authentication sessions, and authorization policy.
//!
//! The canonical representation is 32 random bytes encoded with unpadded
//! base64url. Parsing rejects empty values, whitespace, invalid characters,
//! wrong byte lengths, and alternate encodings. Strict parsing prevents two
//! textual representations from accidentally becoming different public URLs
//! for the same capability. A token is not meaningful after revocation or
//! rotation; the repository owns those lifecycle decisions.
//!
//! PostgreSQL/high-availability considerations: only [`FeedTokenHash`] crosses
//! the persistence boundary, so replicas can resolve the same token from the
//! shared database without a process-local secret map. Hash equality is
//! deterministic and safe to index. RSS-cache behavior is intentionally
//! separate: token lookup selects a source, then the feed service reads or
//! rebuilds that source's cache.

use std::fmt;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use rand::Rng as _;
use sha2::{Digest, Sha256};
use thiserror::Error;

const TOKEN_BYTES: usize = 32;

/// Errors raised while parsing a public feed token.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum FeedTokenError {
    /// A URL or request did not contain a token.
    #[error("feed token must not be empty")]
    Empty,
    /// The token was not valid unpadded base64url.
    #[error("feed token has invalid base64url encoding")]
    InvalidEncoding,
    /// The decoded capability did not have the required entropy size.
    #[error("feed token has invalid length")]
    InvalidLength,
}

/// Opaque bearer capability for one public RSS feed.
pub struct FeedToken(String);

impl FeedToken {
    /// Generates a new 256-bit token in canonical URL-safe form.
    pub fn generate() -> Self {
        let mut bytes = [0_u8; TOKEN_BYTES];
        rand::rng().fill(&mut bytes);
        Self(URL_SAFE_NO_PAD.encode(bytes))
    }

    /// Parses a canonical unpadded base64url token from a request or URL.
    pub fn parse(value: &str) -> Result<Self, FeedTokenError> {
        if value.is_empty() {
            return Err(FeedTokenError::Empty);
        }
        if value.trim() != value {
            return Err(FeedTokenError::InvalidEncoding);
        }

        let bytes = URL_SAFE_NO_PAD
            .decode(value)
            .map_err(|_| FeedTokenError::InvalidEncoding)?;
        if bytes.len() != TOKEN_BYTES {
            return Err(FeedTokenError::InvalidLength);
        }
        if URL_SAFE_NO_PAD.encode(&bytes) != value {
            return Err(FeedTokenError::InvalidEncoding);
        }
        Ok(Self(value.to_owned()))
    }

    /// Returns the raw token for one-time issuance or URL construction.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the digest that may cross into persistence.
    pub(crate) fn hash(&self) -> FeedTokenHash {
        let digest = Sha256::digest(self.0.as_bytes());
        let mut hash = [0_u8; 32];
        hash.copy_from_slice(&digest);
        FeedTokenHash(hash)
    }
}

impl Clone for FeedToken {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl PartialEq for FeedToken {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for FeedToken {}

impl fmt::Debug for FeedToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted feed token>")
    }
}

/// SHA-256 digest of a feed token, suitable for indexed storage.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct FeedTokenHash([u8; 32]);

impl FeedTokenHash {
    /// Returns the digest bytes for a PostgreSQL `BYTEA` parameter.
    pub(crate) const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl fmt::Debug for FeedTokenHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<feed token hash>")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_token_is_canonical_and_hashes_deterministically() {
        let token = FeedToken::generate();
        let reparsed = FeedToken::parse(token.as_str()).expect("generated token should parse");

        assert_eq!(token, reparsed);
        assert_eq!(token.as_str().len(), 43);
        assert_eq!(token.hash(), reparsed.hash());
        assert_eq!(format!("{token:?}"), "<redacted feed token>");
        assert_eq!(format!("{:?}", token.hash()), "<feed token hash>");
    }

    #[test]
    fn parser_rejects_empty_whitespace_padding_wrong_length_and_bad_alphabet() {
        assert_eq!(FeedToken::parse(""), Err(FeedTokenError::Empty));
        assert_eq!(
            FeedToken::parse("  AQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA "),
            Err(FeedTokenError::InvalidEncoding)
        );
        assert_eq!(
            FeedToken::parse("AQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
            Err(FeedTokenError::InvalidEncoding)
        );
        assert_eq!(FeedToken::parse("AQ"), Err(FeedTokenError::InvalidLength));
        assert_eq!(
            FeedToken::parse("!QAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
            Err(FeedTokenError::InvalidEncoding)
        );
    }

    #[test]
    fn parser_rejects_non_canonical_encoding_with_the_right_decoded_length() {
        let canonical = URL_SAFE_NO_PAD.encode([0_u8; TOKEN_BYTES]);
        let mut non_canonical = canonical.clone();
        non_canonical.replace_range(42.., "B");
        assert_eq!(
            FeedToken::parse(&non_canonical),
            Err(FeedTokenError::InvalidEncoding)
        );
    }
}
