//! Opt-in authenticated WeRead browser integration coverage.
//!
//! This test is ignored by default because it needs a reachable WebDriver
//! sidecar, upstream network access, and an operator-provided WeRead cookie.
//! The cookie is read only from `WEREAD_COOKIE_HEADER`; it is never stored in
//! the repository or printed by the test. The adapter deliberately opens the
//! WeRead shelf, installs the cookie in that origin, revisits the shelf, and
//! only then fetches the article endpoint as raw response text. It does not
//! navigate to the JSON endpoint, because Firefox may show a JSON viewer
//! document instead of exposing the response bytes through visible body text.
//!
//! Use a standard browser session for this diagnostic. It does not alter
//! `navigator.webdriver`, inject stealth scripts, or spoof browser
//! fingerprints to bypass upstream controls.
//!
//! ```text
//! WEBDRIVER_URL=http://127.0.0.1:4444 \
//! WEREAD_COOKIE_HEADER='read-from-your-secret-manager' \
//! WEREAD_BOOK_ID=MP_WXS_2103095721 \
//!   cargo test --locked --test real_weread -- --ignored --test-threads=1 --nocapture
//! ```

use std::{env, sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use url::Url;
use uuid::Uuid;
use werrss::{
    acquisition::{
        browser_pool::BrowserPool,
        webdriver::{BrowserProfile, BrowserViewport, WebDriverFactory},
        weread::{
            BrowserWeReadAdapter, WeReadAdapter, WeReadCredentialProvider,
            WeReadCredentialProviderError,
        },
    },
    config::BrowserEngine,
    domain::credentials::{WeReadAccountId, WeReadCredentials},
    persistence::repositories::account_lease_repository::MemoryAccountLeaseRepository,
};

const DEFAULT_BOOK_ID: &str = "MP_WXS_2103095721";
const DEFAULT_ARTICLE_LIST_URL: &str = "https://weread.qq.com/api/mp/cover";

#[derive(Debug)]
struct EnvironmentCredentialProvider {
    cookie_header: String,
}

#[async_trait]
impl WeReadCredentialProvider for EnvironmentCredentialProvider {
    async fn credentials(
        &self,
        _account_id: WeReadAccountId,
    ) -> Result<WeReadCredentials, WeReadCredentialProviderError> {
        let issued_at = Utc::now();
        let credentials = WeReadCredentials::new(
            "real-browser-diagnostic-access",
            "real-browser-diagnostic-refresh",
            issued_at + ChronoDuration::hours(1),
            issued_at,
        )
        .map_err(|_| WeReadCredentialProviderError::Unavailable)?;
        credentials
            .with_web_cookie(self.cookie_header.clone())
            .map_err(|_| WeReadCredentialProviderError::Unavailable)
    }
}

#[tokio::test]
#[ignore = "requires a standard WebDriver sidecar, upstream network, and an operator-provided cookie"]
async fn authenticated_weread_listing_uses_the_shelf_first_flow() {
    let cookie_header = env::var("WEREAD_COOKIE_HEADER")
        .expect("set WEREAD_COOKIE_HEADER from a secret manager or an ignored local environment");
    assert!(
        !cookie_header.trim().is_empty(),
        "WEREAD_COOKIE_HEADER must contain a non-empty cookie header"
    );
    let book_id = env::var("WEREAD_BOOK_ID").unwrap_or_else(|_| DEFAULT_BOOK_ID.to_owned());
    let article_list_url = env::var("WEREAD_ARTICLE_LIST_URL")
        .unwrap_or_else(|_| DEFAULT_ARTICLE_LIST_URL.to_owned())
        .parse::<Url>()
        .expect("WEREAD_ARTICLE_LIST_URL must be a valid URL");
    let webdriver_url = env::var("WEBDRIVER_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:4444".to_owned())
        .parse::<Url>()
        .expect("WEBDRIVER_URL must be a valid URL");
    let engine = env::var("BROWSER_ENGINE")
        .map(|value| {
            value
                .parse::<BrowserEngine>()
                .expect("BROWSER_ENGINE must be chromium or firefox")
        })
        .unwrap_or(BrowserEngine::Firefox);
    let profile = browser_profile_from_environment();
    let factory = WebDriverFactory::new(webdriver_url, engine).with_profile(profile);
    let pool = BrowserPool::new(1).expect("positive browser capacity");
    let repository = MemoryAccountLeaseRepository::new(Utc::now());
    let account_id = WeReadAccountId::from_uuid(Uuid::from_u128(1));
    let mut session = tokio::time::timeout(
        Duration::from_secs(90),
        factory.open_authenticated(
            &pool,
            repository,
            account_id,
            "real-weread-diagnostic",
            ChronoDuration::minutes(5),
        ),
    )
    .await
    .expect("WebDriver session creation timed out")
    .expect("WebDriver sidecar should accept an authenticated session")
    .expect("diagnostic account lease should be available");

    let adapter = BrowserWeReadAdapter::new(article_list_url)
        .expect("WEREAD_ARTICLE_LIST_URL must be a supported WeRead article endpoint")
        .with_credential_provider(Arc::new(EnvironmentCredentialProvider {
            cookie_header: cookie_header.trim().to_owned(),
        }));
    let request = session
        .prepare_request(ChronoDuration::minutes(5))
        .await
        .expect("diagnostic account lease should remain live");
    let result = tokio::time::timeout(
        Duration::from_secs(90),
        adapter.list_articles(&book_id, request),
    )
    .await
    .expect("authenticated WeRead article request timed out");
    let release_result = session.release().await;
    let articles = result.expect("authenticated WeRead article request failed");
    release_result.expect("diagnostic account lease and browser session should be released");

    eprintln!(
        "authenticated WeRead article request succeeded: {} normalized article(s)",
        articles.len()
    );
    assert!(
        articles.iter().all(|article| !article.review_id.is_empty()),
        "every normalized article must have a stable review ID"
    );
}

fn browser_profile_from_environment() -> BrowserProfile {
    let viewport_width = env::var("BROWSER_VIEWPORT_WIDTH")
        .ok()
        .map(|value| {
            value
                .parse()
                .expect("BROWSER_VIEWPORT_WIDTH must be an integer")
        })
        .unwrap_or(1_280);
    let viewport_height = env::var("BROWSER_VIEWPORT_HEIGHT")
        .ok()
        .map(|value| {
            value
                .parse()
                .expect("BROWSER_VIEWPORT_HEIGHT must be an integer")
        })
        .unwrap_or(2_000);
    let expected_timezone = env::var("APP_TIMEZONE").ok().map(|value| {
        value
            .parse()
            .expect("APP_TIMEZONE must be an IANA timezone")
    });
    BrowserProfile {
        user_agent: env::var("BROWSER_USER_AGENT")
            .ok()
            .filter(|value| !value.trim().is_empty()),
        viewport: BrowserViewport::new(viewport_width, viewport_height),
        locale: env::var("BROWSER_LOCALE").unwrap_or_else(|_| "zh-CN".to_owned()),
        expected_timezone,
        extra_args: env::var("BROWSER_EXTRA_ARGS")
            .unwrap_or_default()
            .split_whitespace()
            .map(ToOwned::to_owned)
            .collect(),
    }
}
