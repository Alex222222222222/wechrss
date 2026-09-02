//! Runtime composition for authenticated WeRead listing and public article
//! acquisition.
//!
//! The source-sync handler depends on [`super::source_sync_handler::SourceSyncAcquirer`]
//! rather than on browser details. This adapter is the executable bridge: it
//! acquires one local browser permit and one durable account lease for the
//! authenticated list request, then releases both before each public article
//! is fetched with a clean browser session.
//!
//! Authentication uses an encrypted cookie header injected by the application
//! authentication boundary. The account is selected at job time from the
//! source relationship or the enabled account records, so admin-panel
//! enrollment does not require a process restart. Login and QR exchange remain
//! outside this first runtime slice. Lease contention is reported as a
//! retryable acquisition failure, and a lost lease is never followed by
//! another upstream request.

use chrono::Duration;

use crate::{
    acquisition::{
        article_page::{ArticlePageError, ArticlePageFetcher, ExtractedArticlePage},
        browser_pool::{AccountLeaseError, AccountLeaseStore, BrowserPool},
        webdriver::{WebDriverError, WebDriverFactory},
        weread::{WeReadAdapter, WeReadAdapterError, WeReadArticleReference},
    },
    domain::{credentials::WeReadAccountId, source::Source},
    persistence::repositories::credential_repository::CredentialRepository,
};

use super::{source_sync_handler::SourceSyncAcquirer, sync_service::SyncAcquisitionError};

/// Resolves the account to use for one source-sync request.
///
/// A requested account comes from the source relationship when present. When
/// it is absent, implementations may select any enabled, usable account from
/// their durable store. Selection happens immediately before browser work so
/// accounts enrolled through the admin panel become available without a
/// process restart.
#[async_trait::async_trait]
pub trait WeReadAccountSelector: Send + Sync {
    /// Returns a usable account, or `None` when no account is currently
    /// enrolled and enabled.
    async fn select_account(
        &self,
        requested: Option<WeReadAccountId>,
    ) -> Result<Option<WeReadAccountId>, WeReadAdapterError>;
}

/// PostgreSQL- or memory-backed account selector for runtime source jobs.
#[derive(Clone)]
pub struct CredentialRepositoryAccountSelector<R> {
    accounts: R,
}

impl<R> CredentialRepositoryAccountSelector<R> {
    /// Creates a selector backed by the supplied credential repository.
    pub const fn new(accounts: R) -> Self {
        Self { accounts }
    }
}

#[async_trait::async_trait]
impl<R> WeReadAccountSelector for CredentialRepositoryAccountSelector<R>
where
    R: CredentialRepository + Clone,
{
    async fn select_account(
        &self,
        requested: Option<WeReadAccountId>,
    ) -> Result<Option<WeReadAccountId>, WeReadAdapterError> {
        let now = self
            .accounts
            .database_now()
            .await
            .map_err(|error| WeReadAdapterError::LeaseBackend(error.to_string()))?;
        let record = match requested {
            Some(account_id) => self
                .accounts
                .find(account_id)
                .await
                .map_err(|error| WeReadAdapterError::LeaseBackend(error.to_string()))?,
            None => self
                .accounts
                .list()
                .await
                .map_err(|error| WeReadAdapterError::LeaseBackend(error.to_string()))?
                .into_iter()
                .find(|record| {
                    !record.account().disabled() && record.account().access_expires_at() > now
                }),
        };

        Ok(record
            .filter(|record| {
                !record.account().disabled() && record.account().access_expires_at() > now
            })
            .map(|record| record.account().account_id()))
    }
}

/// Dependencies for the concrete source-sync acquisition adapter.
#[derive(Clone)]
pub struct BrowserSourceSyncAcquirerDependencies<R, W, A, S> {
    /// Process-local browser capacity shared by worker tasks.
    pub browser_pool: BrowserPool,
    /// WebDriver connection and browser-profile policy.
    pub webdriver: WebDriverFactory,
    /// Durable account lease store.
    pub account_leases: R,
    /// Authenticated WeRead article-list adapter.
    pub weread: W,
    /// Credential-free public article-page fetcher.
    pub article_pages: A,
    /// Durable account selector used when the source has no fixed account.
    pub account_selector: S,
}

/// Runtime policy for authenticated source synchronization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrowserSourceSyncAcquirerConfig {
    account_id: Option<WeReadAccountId>,
    owner: String,
    lease_for: Duration,
    heartbeat_for: Duration,
}

impl BrowserSourceSyncAcquirerConfig {
    /// Creates a policy with an optional environment-configured default
    /// account. When it is absent, the account selector is consulted for each
    /// source-sync job.
    pub fn new(
        account_id: Option<WeReadAccountId>,
        owner: impl Into<String>,
        lease_for: Duration,
        heartbeat_for: Duration,
    ) -> Result<Self, BrowserSourceSyncAcquirerConfigError> {
        if account_id.is_some_and(|account_id| account_id.as_uuid().is_nil()) {
            return Err(BrowserSourceSyncAcquirerConfigError::InvalidAccountId);
        }
        let owner = owner.into();
        if owner.trim().is_empty() {
            return Err(BrowserSourceSyncAcquirerConfigError::EmptyOwner);
        }
        if lease_for <= Duration::zero() || heartbeat_for <= Duration::zero() {
            return Err(BrowserSourceSyncAcquirerConfigError::InvalidLeaseTiming);
        }
        if heartbeat_for >= lease_for {
            return Err(BrowserSourceSyncAcquirerConfigError::HeartbeatNotShorterThanLease);
        }
        Ok(Self {
            account_id,
            owner,
            lease_for,
            heartbeat_for,
        })
    }

    /// Returns the default account used when a source has no account override.
    pub const fn account_id(&self) -> Option<WeReadAccountId> {
        self.account_id
    }

    /// Returns the account lease duration.
    pub const fn lease_for(&self) -> Duration {
        self.lease_for
    }

    /// Returns the account heartbeat duration passed to the authenticated
    /// request preparation boundary.
    pub const fn heartbeat_for(&self) -> Duration {
        self.heartbeat_for
    }

    fn owner_for(&self, worker_index: u32) -> String {
        format!("{}-{worker_index}", self.owner)
    }
}

/// Invalid concrete source-sync acquisition policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BrowserSourceSyncAcquirerConfigError {
    /// A nil UUID cannot identify an account.
    #[error("WeRead account id must not be nil")]
    InvalidAccountId,
    /// Lease ownership must identify a worker instance.
    #[error("source-sync account lease owner must not be empty")]
    EmptyOwner,
    /// Both lease durations must be positive.
    #[error("source-sync account lease timings must be positive")]
    InvalidLeaseTiming,
    /// Heartbeats must leave time to reach the database before expiry.
    #[error("source-sync account heartbeat must be shorter than its lease")]
    HeartbeatNotShorterThanLease,
}

/// Thirtyfour-backed source-sync acquisition implementation.
#[derive(Clone)]
pub struct BrowserSourceSyncAcquirer<R, W, A, S> {
    dependencies: BrowserSourceSyncAcquirerDependencies<R, W, A, S>,
    config: BrowserSourceSyncAcquirerConfig,
    worker_index: u32,
}

impl<R, W, A, S> BrowserSourceSyncAcquirer<R, W, A, S> {
    /// Creates one worker-local adapter. The browser pool and lease store may
    /// be cloned into other workers; the durable account lease serializes use
    /// of the selected account across them.
    pub fn new(
        dependencies: BrowserSourceSyncAcquirerDependencies<R, W, A, S>,
        config: BrowserSourceSyncAcquirerConfig,
        worker_index: u32,
    ) -> Self {
        Self {
            dependencies,
            config,
            worker_index,
        }
    }
}

impl<R, W, A, S> SourceSyncAcquirer for BrowserSourceSyncAcquirer<R, W, A, S>
where
    R: AccountLeaseStore + Clone + 'static,
    W: WeReadAdapter<R>,
    A: ArticlePageFetcher,
    S: WeReadAccountSelector,
{
    async fn list_article_references(
        &self,
        source: &Source,
    ) -> Result<Vec<WeReadArticleReference>, SyncAcquisitionError> {
        let account_id = match self.config.account_id() {
            Some(configured) => {
                if source
                    .account_id()
                    .is_some_and(|source_account| source_account != configured)
                {
                    return Err(SyncAcquisitionError::WeRead(
                        WeReadAdapterError::LeaseBackend(
                            "source account is not available in the configured account".to_owned(),
                        ),
                    ));
                }
                configured
            }
            None => self
                .dependencies
                .account_selector
                .select_account(source.account_id())
                .await
                .map_err(SyncAcquisitionError::WeRead)?
                .ok_or(SyncAcquisitionError::NoAccountEnrolled)?,
        };
        let owner = self.config.owner_for(self.worker_index);
        let session = self
            .dependencies
            .webdriver
            .open_authenticated(
                &self.dependencies.browser_pool,
                self.dependencies.account_leases.clone(),
                account_id,
                &owner,
                self.config.lease_for(),
            )
            .await
            .map_err(map_authenticated_webdriver_error)?;
        let Some(mut session) = session else {
            return Err(SyncAcquisitionError::WeRead(
                WeReadAdapterError::LeaseBackend(
                    "the configured WeRead account is already in use".to_owned(),
                ),
            ));
        };

        if let Err(error) =
            session.start_lease_heartbeat(self.config.heartbeat_for(), self.config.lease_for())
        {
            let _ = session.release().await;
            return Err(SyncAcquisitionError::WeRead(error.into()));
        }
        let result = async {
            let references = {
                let request = session
                    .prepare_request(self.config.lease_for())
                    .await
                    .map_err(WeReadAdapterError::from)?;
                self.dependencies
                    .weread
                    .list_articles(source.book_id(), request)
                    .await?
            };
            session.ensure_usable().map_err(WeReadAdapterError::from)?;
            Ok(references)
        }
        .await;
        let heartbeat = session.stop_lease_heartbeat().await;
        let release = session.release().await;
        match (result, heartbeat, release) {
            (Err(error), _, _) => Err(SyncAcquisitionError::WeRead(error)),
            (Ok(_), Err(error), _) => Err(SyncAcquisitionError::WeRead(error.into())),
            (Ok(_), Ok(()), Err(error)) => Err(SyncAcquisitionError::WeRead(error.into())),
            (Ok(references), Ok(()), Ok(())) => Ok(references),
        }
    }

    async fn fetch_article(
        &self,
        reference: &WeReadArticleReference,
    ) -> Result<ExtractedArticlePage, SyncAcquisitionError> {
        let url = reference.article_url.clone().ok_or_else(|| {
            SyncAcquisitionError::ArticlePage(ArticlePageError::InvalidExtraction(
                "WeRead article reference has no public URL".to_owned(),
            ))
        })?;
        let session = self
            .dependencies
            .webdriver
            .open_public(&self.dependencies.browser_pool)
            .await
            .map_err(map_public_webdriver_error)?;
        self.dependencies
            .article_pages
            .fetch(url, session)
            .await
            .map_err(SyncAcquisitionError::ArticlePage)
    }
}

fn map_authenticated_webdriver_error(error: WebDriverError) -> SyncAcquisitionError {
    SyncAcquisitionError::WeRead(WeReadAdapterError::Browser(error.to_string()))
}

fn map_public_webdriver_error(error: WebDriverError) -> SyncAcquisitionError {
    SyncAcquisitionError::ArticlePage(ArticlePageError::Browser(error.to_string()))
}

impl From<AccountLeaseError> for SyncAcquisitionError {
    fn from(error: AccountLeaseError) -> Self {
        SyncAcquisitionError::WeRead(WeReadAdapterError::from(error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        acquisition::{
            article_page::{ArticlePageError, ArticlePageFetcher},
            browser_pool::{AccountLeaseStore, BrowserPool},
            webdriver::{AuthenticatedRequest, PublicBrowserSession},
        },
        config::BrowserEngine,
        domain::source::{NewSource, Source},
        persistence::repositories::{
            account_lease_repository::MemoryAccountLeaseRepository,
            credential_repository::{CredentialRepository, MemoryCredentialRepository},
        },
    };
    use uuid::Uuid;

    fn account_id() -> WeReadAccountId {
        WeReadAccountId::from_uuid(Uuid::from_u128(1))
    }

    fn source(account_id: Option<WeReadAccountId>) -> Source {
        let mut spec = NewSource::test_default();
        spec.account_id = account_id;
        Source::new(spec).expect("test source should be valid")
    }

    #[derive(Clone)]
    struct UnusedWeReadAdapter;

    impl<R> WeReadAdapter<R> for UnusedWeReadAdapter
    where
        R: AccountLeaseStore,
    {
        async fn list_articles(
            &self,
            _book_id: &str,
            _request: AuthenticatedRequest<'_, R>,
        ) -> Result<Vec<WeReadArticleReference>, WeReadAdapterError> {
            unreachable!("the test must stop before browser acquisition")
        }
    }

    #[derive(Clone)]
    struct UnusedArticlePageFetcher;

    impl ArticlePageFetcher for UnusedArticlePageFetcher {
        async fn fetch(
            &self,
            _url: crate::domain::source::VerifiedWechatArticleUrl,
            _session: PublicBrowserSession,
        ) -> Result<ExtractedArticlePage, ArticlePageError> {
            unreachable!("the test must stop before browser acquisition")
        }
    }

    #[derive(Clone)]
    struct StaticAccountSelector {
        account_id: Option<WeReadAccountId>,
    }

    #[async_trait::async_trait]
    impl WeReadAccountSelector for StaticAccountSelector {
        async fn select_account(
            &self,
            requested: Option<WeReadAccountId>,
        ) -> Result<Option<WeReadAccountId>, WeReadAdapterError> {
            Ok(requested.or(self.account_id))
        }
    }

    fn acquirer_with_account_and_selector(
        repository: MemoryAccountLeaseRepository,
        account_id: Option<WeReadAccountId>,
        selected_account_id: Option<WeReadAccountId>,
    ) -> BrowserSourceSyncAcquirer<
        MemoryAccountLeaseRepository,
        UnusedWeReadAdapter,
        UnusedArticlePageFetcher,
        StaticAccountSelector,
    > {
        let config = BrowserSourceSyncAcquirerConfig::new(
            account_id,
            "source-sync-test",
            Duration::seconds(30),
            Duration::seconds(5),
        )
        .expect("test acquisition policy should be valid");
        BrowserSourceSyncAcquirer::new(
            BrowserSourceSyncAcquirerDependencies {
                browser_pool: BrowserPool::new(1).expect("test pool should be valid"),
                webdriver: WebDriverFactory::new(
                    "http://127.0.0.1:9"
                        .parse()
                        .expect("test URL should be valid"),
                    BrowserEngine::Firefox,
                ),
                account_leases: repository,
                weread: UnusedWeReadAdapter,
                article_pages: UnusedArticlePageFetcher,
                account_selector: StaticAccountSelector {
                    account_id: selected_account_id,
                },
            },
            config,
            0,
        )
    }

    fn acquirer(
        repository: MemoryAccountLeaseRepository,
        account_id: WeReadAccountId,
    ) -> BrowserSourceSyncAcquirer<
        MemoryAccountLeaseRepository,
        UnusedWeReadAdapter,
        UnusedArticlePageFetcher,
        StaticAccountSelector,
    > {
        acquirer_with_account_and_selector(repository, Some(account_id), Some(account_id))
    }

    #[test]
    fn rejects_invalid_lease_policies_before_browser_work() {
        assert_eq!(
            BrowserSourceSyncAcquirerConfig::new(
                Some(WeReadAccountId::from_uuid(Uuid::nil())),
                "worker",
                Duration::seconds(30),
                Duration::seconds(5),
            ),
            Err(BrowserSourceSyncAcquirerConfigError::InvalidAccountId)
        );
        assert_eq!(
            BrowserSourceSyncAcquirerConfig::new(
                Some(account_id()),
                " ",
                Duration::seconds(30),
                Duration::seconds(5),
            ),
            Err(BrowserSourceSyncAcquirerConfigError::EmptyOwner)
        );
        assert_eq!(
            BrowserSourceSyncAcquirerConfig::new(
                Some(account_id()),
                "worker",
                Duration::seconds(5),
                Duration::seconds(5),
            ),
            Err(BrowserSourceSyncAcquirerConfigError::HeartbeatNotShorterThanLease)
        );
    }

    #[test]
    fn derives_distinct_worker_lease_owners() {
        let config = BrowserSourceSyncAcquirerConfig::new(
            Some(account_id()),
            "instance-source-worker",
            Duration::seconds(30),
            Duration::seconds(5),
        )
        .unwrap();
        assert_eq!(config.owner_for(0), "instance-source-worker-0");
        assert_eq!(config.owner_for(7), "instance-source-worker-7");
    }

    #[tokio::test]
    async fn lease_contention_stops_before_authenticated_browser_work() {
        let repository = MemoryAccountLeaseRepository::new(chrono::Utc::now());
        let account_id = account_id();
        let _held = repository
            .acquire(account_id, "another-worker", Duration::seconds(30))
            .await
            .unwrap()
            .expect("the competing worker should acquire the account");

        let result = acquirer(repository, account_id)
            .list_article_references(&source(None))
            .await;

        assert!(matches!(
            result,
            Err(SyncAcquisitionError::WeRead(
                WeReadAdapterError::LeaseBackend(message)
            )) if message.contains("already in use")
        ));
    }

    #[tokio::test]
    async fn no_enrolled_account_fails_before_opening_authenticated_browser_work() {
        let result = acquirer_with_account_and_selector(
            MemoryAccountLeaseRepository::new(chrono::Utc::now()),
            None,
            None,
        )
        .list_article_references(&source(None))
        .await;

        assert_eq!(result, Err(SyncAcquisitionError::NoAccountEnrolled));
    }

    #[tokio::test]
    async fn selects_the_source_account_from_the_durable_account_selector() {
        let selected = account_id();
        let repository = MemoryAccountLeaseRepository::new(chrono::Utc::now());
        let _held = repository
            .acquire(selected, "another-worker", Duration::seconds(30))
            .await
            .unwrap()
            .expect("the competing worker should acquire the selected account");

        let result = acquirer_with_account_and_selector(repository, None, Some(selected))
            .list_article_references(&source(None))
            .await;

        assert!(matches!(
            result,
            Err(SyncAcquisitionError::WeRead(
                WeReadAdapterError::LeaseBackend(message)
            )) if message.contains("already in use")
        ));
    }

    #[tokio::test]
    async fn selects_the_first_unexpired_enrolled_account_and_honors_overrides() {
        let now = chrono::Utc::now();
        let expired = WeReadAccountId::from_uuid(Uuid::from_u128(2));
        let enrolled = account_id();
        let repository = MemoryCredentialRepository::new(now);
        repository
            .insert(
                expired,
                "expired",
                b"expired-ciphertext",
                now - chrono::Duration::seconds(1),
            )
            .await
            .unwrap();
        repository
            .insert(
                enrolled,
                "enrolled",
                b"enrolled-ciphertext",
                now + chrono::Duration::hours(1),
            )
            .await
            .unwrap();

        let selector = CredentialRepositoryAccountSelector::new(repository);
        assert_eq!(selector.select_account(None).await.unwrap(), Some(enrolled));
        assert_eq!(selector.select_account(Some(expired)).await.unwrap(), None);
        assert_eq!(
            selector.select_account(Some(enrolled)).await.unwrap(),
            Some(enrolled)
        );
    }

    #[tokio::test]
    async fn source_account_override_cannot_escape_the_configured_profile() {
        let configured_account = account_id();
        let source_account = WeReadAccountId::from_uuid(Uuid::from_u128(2));
        let result = acquirer(
            MemoryAccountLeaseRepository::new(chrono::Utc::now()),
            configured_account,
        )
        .list_article_references(&source(Some(source_account)))
        .await;

        assert!(matches!(
            result,
            Err(SyncAcquisitionError::WeRead(
                WeReadAdapterError::LeaseBackend(message)
            )) if message.contains("not available")
        ));
    }

    #[tokio::test]
    async fn missing_public_url_is_rejected_before_opening_a_public_browser() {
        let reference = WeReadArticleReference::new("review-1", None, Some("title".to_owned()))
            .expect("reference should be valid");
        let result = acquirer(
            MemoryAccountLeaseRepository::new(chrono::Utc::now()),
            account_id(),
        )
        .fetch_article(&reference)
        .await;

        assert!(matches!(
            result,
            Err(SyncAcquisitionError::ArticlePage(
                ArticlePageError::InvalidExtraction(message)
            )) if message.contains("no public URL")
        ));
    }
}
