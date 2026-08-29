use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use chrono::Utc;
use uuid::Uuid;
use wechrss::{
    acquisition::{
        article_page::{ArticlePageError, ArticlePageFetcher, ExtractedArticlePage},
        browser_pool::{AccountLeaseError, AccountLeaseStore, BrowserPool},
        webdriver::{AuthenticatedRequest, PublicBrowserSession},
        weread::{WeReadAdapter, WeReadAdapterError, WeReadArticleReference},
    },
    domain::{credentials::WeReadAccountId, source::VerifiedWechatArticleUrl},
    persistence::repositories::account_lease_repository::MemoryAccountLeaseRepository,
};

fn account_id() -> WeReadAccountId {
    WeReadAccountId::from_uuid(Uuid::from_u128(1))
}

struct PublicFetcher;

impl ArticlePageFetcher for PublicFetcher {
    async fn fetch(
        &self,
        url: VerifiedWechatArticleUrl,
        _session: PublicBrowserSession,
    ) -> Result<ExtractedArticlePage, ArticlePageError> {
        Ok(ExtractedArticlePage {
            canonical_url: url,
            title: "Public article".to_owned(),
            author: None,
            summary: None,
            published_at: Utc::now(),
            content_html: "<p>content</p>".to_owned(),
            cover_url: None,
        })
    }
}

#[derive(Clone)]
struct LeaseCheckingAdapter {
    calls: Arc<AtomicUsize>,
}

impl<R> WeReadAdapter<R> for LeaseCheckingAdapter
where
    R: AccountLeaseStore,
{
    async fn list_articles(
        &self,
        request: AuthenticatedRequest<'_, R>,
    ) -> Result<Vec<WeReadArticleReference>, WeReadAdapterError> {
        let _account_id = request.account_id();
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(vec![WeReadArticleReference::new("review-1", None, None)
            .expect("test reference has a stable identity")])
    }
}

#[tokio::test]
async fn public_article_fetching_uses_no_account_lease() {
    let pool = BrowserPool::new(1).expect("positive browser capacity");
    let session = pool
        .open_public()
        .await
        .expect("public session should be available");
    let url = "https://mp.weixin.qq.com/s/public-article"
        .parse::<VerifiedWechatArticleUrl>()
        .expect("test URL should be valid");

    let page = PublicFetcher
        .fetch(url.clone(), session)
        .await
        .expect("public page should be fetched");

    assert_eq!(page.canonical_url, url);
    assert_eq!(page.title, "Public article");
}

#[tokio::test]
async fn pool_serializes_account_sessions_and_releases_after_completion() {
    let repository = MemoryAccountLeaseRepository::new(Utc::now());
    let pool = BrowserPool::new(2).expect("positive browser capacity");
    let mut first = pool
        .open_authenticated(
            repository.clone(),
            account_id(),
            "worker-a",
            chrono::Duration::seconds(30),
        )
        .await
        .expect("first lease attempt should succeed")
        .expect("first worker should own the account");
    let calls = Arc::new(AtomicUsize::new(0));
    let adapter = LeaseCheckingAdapter {
        calls: calls.clone(),
    };
    let request = first
        .prepare_request(chrono::Duration::seconds(30))
        .await
        .expect("a live account should prepare a request");
    let entries = adapter
        .list_articles(request)
        .await
        .expect("the authenticated adapter should receive a lease proof");
    assert_eq!(entries[0].review_id, "review-1");
    assert_eq!(calls.load(Ordering::Relaxed), 1);

    assert!(pool
        .open_authenticated(
            repository.clone(),
            account_id(),
            "worker-b",
            chrono::Duration::seconds(30),
        )
        .await
        .expect("second lease attempt should succeed")
        .is_none());
    let public_session = pool
        .open_public()
        .await
        .expect("a refused account lease must return local capacity");
    drop(public_session);

    first
        .release()
        .await
        .expect("first worker should release its lease");
    let second = pool
        .open_authenticated(
            repository,
            account_id(),
            "worker-b",
            chrono::Duration::seconds(30),
        )
        .await
        .expect("released account should be acquirable")
        .expect("worker should reacquire the released account");
    second
        .release()
        .await
        .expect("second worker should release its lease");
}

#[tokio::test]
async fn lost_account_lease_blocks_authenticated_adapter_requests() {
    let initial_time = chrono::DateTime::parse_from_rfc3339("2026-08-29T00:00:00Z")
        .expect("test timestamp should be valid")
        .with_timezone(&Utc);
    let repository = MemoryAccountLeaseRepository::new(initial_time);
    let pool = BrowserPool::new(1).expect("positive browser capacity");
    let mut session = pool
        .open_authenticated(
            repository.clone(),
            account_id(),
            "worker-a",
            chrono::Duration::seconds(10),
        )
        .await
        .expect("lease attempt should succeed")
        .expect("worker should own the account");
    repository
        .set_now(initial_time + chrono::Duration::seconds(11))
        .await;
    assert!(matches!(
        session.prepare_request(chrono::Duration::seconds(10)).await,
        Err(AccountLeaseError::LeaseLost { .. })
    ));
}
