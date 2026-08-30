//! Application orchestration for public feed-token lifecycle.
//!
//! This service is the only application boundary that turns a raw public RSS
//! bearer token into a persistence lookup hash or returns a newly generated
//! token to an administrative caller. It provides issue/rotate, resolve, and
//! revoke operations while keeping raw values out of repository interfaces,
//! logs, error strings, and feed-cache records.
//!
//! Responsibilities: generate 256-bit opaque capabilities, validate incoming
//! token syntax before lookup, delegate hash-only storage, and preserve the
//! distinction between an invalid token and a valid-but-unknown/revoked token.
//! Non-responsibilities: administrative authentication, source CRUD, HTTP
//! path parsing, RSS rendering, cache invalidation, browser work, or upstream
//! credentials. A web handler should resolve the token here and then pass the
//! returned source id to [`FeedService`](super::feed_service::FeedService).
//!
//! PostgreSQL/high-availability behavior comes from the repository's unique
//! digest and one-current-token-per-source row. Rotating on one replica makes
//! the old token invalid on all replicas after the write commits. Revocation is
//! idempotent, which makes retries safe. Issuance is not coupled to feed-cache
//! revision: changing who can access a feed does not change its XML bytes.
//! The raw token is returned only as the successful result of issue/rotate;
//! callers must not persist or log it.
//!
//! TODO(implementation): compose this service into the protected source
//! administration API and the tokenized `/feeds/{feed_token}.xml` route.

use thiserror::Error;

use crate::{
    domain::{feed_token::FeedToken, source::SourceId},
    persistence::repositories::feed_token_repository::{
        FeedTokenRepository, FeedTokenRepositoryError, PostgresFeedTokenRepository,
    },
};

/// Errors raised by feed-token lifecycle operations.
#[derive(Debug, Error)]
pub enum FeedTokenServiceError {
    /// The request token failed strict canonical parsing.
    #[error("invalid feed token")]
    InvalidToken,
    /// The token repository could not complete the operation.
    #[error(transparent)]
    Repository(#[from] FeedTokenRepositoryError),
}

/// Public capability lifecycle service.
#[derive(Clone)]
pub struct FeedTokenService<R = PostgresFeedTokenRepository> {
    repository: R,
}

impl<R> FeedTokenService<R>
where
    R: FeedTokenRepository,
{
    /// Creates a service over a feed-token repository.
    pub fn new(repository: R) -> Self {
        Self { repository }
    }

    /// Issues or rotates a source token and returns the raw value once.
    pub async fn issue(&self, source_id: SourceId) -> Result<FeedToken, FeedTokenServiceError> {
        let token = FeedToken::generate();
        self.repository
            .replace(source_id, token.hash())
            .await
            .map_err(FeedTokenServiceError::Repository)?;
        Ok(token)
    }

    /// Resolves a canonical raw token to an active source.
    ///
    /// A syntactically invalid, unknown, or revoked token produces no source
    /// while retaining a distinct error only for malformed input. The HTTP
    /// layer can map both cases to the same non-enumerating public response.
    pub async fn resolve(
        &self,
        raw_token: &str,
    ) -> Result<Option<SourceId>, FeedTokenServiceError> {
        let token = FeedToken::parse(raw_token).map_err(|_| FeedTokenServiceError::InvalidToken)?;
        self.repository
            .find_source(token.hash())
            .await
            .map_err(FeedTokenServiceError::Repository)
    }

    /// Revokes the active token for a source, returning whether it existed.
    pub async fn revoke(&self, source_id: SourceId) -> Result<bool, FeedTokenServiceError> {
        self.repository
            .revoke(source_id)
            .await
            .map_err(FeedTokenServiceError::Repository)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::Mutex;
    use uuid::Uuid;

    use super::*;
    use crate::domain::feed_token::FeedTokenHash;

    #[derive(Clone, Default)]
    struct FakeRepository {
        state: Arc<Mutex<FakeState>>,
    }

    #[derive(Default)]
    struct FakeState {
        source_id: Option<SourceId>,
        token_hash: Option<FeedTokenHash>,
        revoked: bool,
        replace_calls: usize,
    }

    impl FeedTokenRepository for FakeRepository {
        async fn find_source(
            &self,
            token_hash: FeedTokenHash,
        ) -> Result<Option<SourceId>, FeedTokenRepositoryError> {
            let state = self.state.lock().await;
            Ok((!state.revoked && state.token_hash == Some(token_hash))
                .then_some(state.source_id)
                .flatten())
        }

        async fn replace(
            &self,
            source_id: SourceId,
            token_hash: FeedTokenHash,
        ) -> Result<(), FeedTokenRepositoryError> {
            let mut state = self.state.lock().await;
            state.source_id = Some(source_id);
            state.token_hash = Some(token_hash);
            state.revoked = false;
            state.replace_calls += 1;
            Ok(())
        }

        async fn revoke(&self, source_id: SourceId) -> Result<bool, FeedTokenRepositoryError> {
            let mut state = self.state.lock().await;
            if state.source_id == Some(source_id) && !state.revoked {
                state.revoked = true;
                Ok(true)
            } else {
                Ok(false)
            }
        }
    }

    fn source_id() -> SourceId {
        SourceId::from_uuid(Uuid::from_u128(1))
    }

    #[tokio::test]
    async fn issued_token_resolves_then_rotation_invalidates_the_old_value() {
        let repository = FakeRepository::default();
        let service = FeedTokenService::new(repository.clone());

        let first = service
            .issue(source_id())
            .await
            .expect("issue should succeed");
        assert_eq!(
            service.resolve(first.as_str()).await.unwrap(),
            Some(source_id())
        );

        let second = service
            .issue(source_id())
            .await
            .expect("rotation should succeed");
        assert_ne!(first, second);
        assert_eq!(service.resolve(first.as_str()).await.unwrap(), None);
        assert_eq!(
            service.resolve(second.as_str()).await.unwrap(),
            Some(source_id())
        );
        assert_eq!(repository.state.lock().await.replace_calls, 2);
    }

    #[tokio::test]
    async fn malformed_and_revoked_tokens_do_not_resolve() {
        let repository = FakeRepository::default();
        let service = FeedTokenService::new(repository);
        let token = service
            .issue(source_id())
            .await
            .expect("issue should succeed");
        let unknown = FeedToken::generate();

        assert!(matches!(
            service.resolve("not-a-token").await,
            Err(FeedTokenServiceError::InvalidToken)
        ));
        assert_eq!(service.resolve(unknown.as_str()).await.unwrap(), None);
        assert!(service.revoke(source_id()).await.unwrap());
        assert_eq!(service.resolve(token.as_str()).await.unwrap(), None);
        assert!(!service.revoke(source_id()).await.unwrap());
    }

    #[tokio::test]
    async fn repository_errors_are_not_converted_to_unknown_token() {
        #[derive(Clone)]
        struct FailingRepository;

        impl FeedTokenRepository for FailingRepository {
            async fn find_source(
                &self,
                _token_hash: FeedTokenHash,
            ) -> Result<Option<SourceId>, FeedTokenRepositoryError> {
                Err(FeedTokenRepositoryError::Storage("offline".to_owned()))
            }

            async fn replace(
                &self,
                _source_id: SourceId,
                _token_hash: FeedTokenHash,
            ) -> Result<(), FeedTokenRepositoryError> {
                Err(FeedTokenRepositoryError::Storage("offline".to_owned()))
            }

            async fn revoke(&self, _source_id: SourceId) -> Result<bool, FeedTokenRepositoryError> {
                Err(FeedTokenRepositoryError::Storage("offline".to_owned()))
            }
        }

        let service = FeedTokenService::new(FailingRepository);
        let token = FeedToken::generate();
        assert!(matches!(
            service.resolve(token.as_str()).await,
            Err(FeedTokenServiceError::Repository(
                FeedTokenRepositoryError::Storage(_)
            ))
        ));
    }
}
