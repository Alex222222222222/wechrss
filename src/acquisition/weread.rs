//! WeRead account and article-list protocol adapter boundary.
//!
//! This module defines the authenticated protocol port for QR/login state,
//! refresh-token lifecycle, article-list responses, detail-URL recovery, and
//! current/legacy response-shape parsing. It does not fetch rendered article
//! content: that is the separate public operation in [`super::article_page`]
//! and intentionally needs no credentials.
//!
//! A caller must obtain the one-request capability from
//! [`super::webdriver::AuthenticatedBrowserSession::prepare_request`] before
//! issuing protocol requests. That capability performs a server-clock lease
//! heartbeat, so an expired lease cannot reach the adapter. Lease loss is
//! terminal for the current operation and must not trigger token rotation.
//! Authentication expiry may be retried once by the application orchestration
//! layer, while risk-control responses remain terminal.
//!
//! TODO(implementation): add the concrete Thirtyfour/browser protocol adapter,
//! QR exchange, response-shape parsing, pacing hooks, credential refresh, and
//! typed classification of upstream responses.

use thiserror::Error;

use crate::domain::{credentials::WeReadAccountId, source::VerifiedWechatArticleUrl};

use super::{
    browser_pool::{AccountLeaseError, AccountLeaseStore},
    webdriver::AuthenticatedRequest,
};

/// One normalized article-list entry returned by the WeRead adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WeReadArticleReference {
    /// Stable upstream review ID used for article idempotency.
    pub review_id: String,
    /// Recovered public article URL, when the list response includes one.
    pub article_url: Option<VerifiedWechatArticleUrl>,
    /// Optional upstream title hint.
    pub title: Option<String>,
}

impl WeReadArticleReference {
    /// Constructs a normalized reference and rejects an empty stable identity.
    pub fn new(
        review_id: impl Into<String>,
        article_url: Option<VerifiedWechatArticleUrl>,
        title: Option<String>,
    ) -> Result<Self, WeReadAdapterError> {
        let review_id = review_id.into().trim().to_owned();
        if review_id.is_empty() {
            return Err(WeReadAdapterError::InvalidReviewId);
        }
        Ok(Self {
            review_id,
            article_url,
            title,
        })
    }
}

/// Errors exposed by authenticated WeRead protocol adapters.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum WeReadAdapterError {
    /// The account lease was lost before a request could be issued.
    #[error("WeRead account lease lost for {account_id}")]
    LeaseLost { account_id: WeReadAccountId },
    /// The account lease backend could not prove request ownership.
    #[error("WeRead account lease backend error: {0}")]
    LeaseBackend(String),
    /// The upstream response did not match a supported shape.
    #[error("WeRead protocol error: {0}")]
    Protocol(String),
    /// A response omitted the stable identity needed for idempotent storage.
    #[error("WeRead article review_id must not be empty")]
    InvalidReviewId,
}

impl From<AccountLeaseError> for WeReadAdapterError {
    fn from(error: AccountLeaseError) -> Self {
        match error {
            AccountLeaseError::LeaseLost { account_id } => Self::LeaseLost { account_id },
            other => Self::LeaseBackend(other.to_string()),
        }
    }
}

/// Port for authenticated WeRead account/list operations.
#[allow(async_fn_in_trait)]
pub trait WeReadAdapter<R>: Send + Sync
where
    R: AccountLeaseStore,
{
    /// Lists normalized article references using a freshly heartbeated request.
    async fn list_articles(
        &self,
        request: AuthenticatedRequest<'_, R>,
    ) -> Result<Vec<WeReadArticleReference>, WeReadAdapterError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use uuid::Uuid;

    use crate::{
        acquisition::browser_pool::BrowserPool,
        persistence::repositories::account_lease_repository::MemoryAccountLeaseRepository,
    };

    fn account_id() -> WeReadAccountId {
        WeReadAccountId::from_uuid(Uuid::from_u128(1))
    }

    struct FakeAdapter;

    impl<R> WeReadAdapter<R> for FakeAdapter
    where
        R: AccountLeaseStore,
    {
        async fn list_articles(
            &self,
            request: AuthenticatedRequest<'_, R>,
        ) -> Result<Vec<WeReadArticleReference>, WeReadAdapterError> {
            let _account_id = request.account_id();
            Ok(vec![
                WeReadArticleReference::new("review-1", None, None).unwrap()
            ])
        }
    }

    #[test]
    fn rejects_missing_stable_article_identity() {
        assert_eq!(
            WeReadArticleReference::new("  ", None, None),
            Err(WeReadAdapterError::InvalidReviewId)
        );
    }

    #[test]
    fn trims_stable_article_identity() {
        let reference = WeReadArticleReference::new(" review-1 ", None, None).unwrap();
        assert_eq!(reference.review_id, "review-1");
    }

    #[tokio::test]
    async fn adapter_receives_only_an_authenticated_session() {
        let repository = MemoryAccountLeaseRepository::new(Utc::now());
        let pool = BrowserPool::new(1).unwrap();
        let mut session = pool
            .open_authenticated(
                repository,
                account_id(),
                "worker-a",
                chrono::Duration::seconds(30),
            )
            .await
            .unwrap()
            .unwrap();

        let request = session
            .prepare_request(chrono::Duration::seconds(30))
            .await
            .unwrap();
        let entries = FakeAdapter.list_articles(request).await.unwrap();
        assert_eq!(entries[0].review_id, "review-1");
    }
}
