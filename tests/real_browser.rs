//! Opt-in integration test for the public, unauthenticated browser path.
//!
//! This test is ignored by default because it needs a reachable WebDriver
//! sidecar and upstream network access. It deliberately uses the public
//! article fetcher, never creates an account lease, and exercises bounded
//! pacing/scrolling before extraction. It does not assert on mutable article
//! text beyond the known fixture title. Run it manually after forwarding a
//! development WebDriver service:
//!
//! ```text
//! WEBDRIVER_URL=http://127.0.0.1:4444 \
//!   cargo test --locked --test real_browser -- --ignored --nocapture
//! ```

use std::{env, time::Duration};

use chrono_tz::Asia::Shanghai;
use url::Url;
use werrss::{
    acquisition::{
        article_page::{ArticlePageFetcher, WebDriverArticlePageFetcher},
        browser_pool::BrowserPool,
        pacing::PacingController,
        webdriver::{BrowserProfile, BrowserViewport, WebDriverFactory},
    },
    config::BrowserEngine,
    domain::{
        pacing::{DelayDistribution, PacingPolicy},
        source::VerifiedWechatArticleUrl,
    },
};

const PUBLIC_ARTICLE_URL: &str = "https://mp.weixin.qq.com/s/5CqpNShrLGIM93XoJD7s5g";
const PUBLIC_ARTICLE_TITLE: &str = "我们逃不掉的discouraged 时间";

#[tokio::test]
#[ignore = "requires an explicitly configured WebDriver sidecar and upstream network"]
async fn fetches_a_real_public_article_without_credentials() {
    let endpoint = env::var("WEBDRIVER_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:4444".to_owned())
        .parse::<Url>()
        .expect("WEBDRIVER_URL must be a valid URL");
    let engine = env::var("BROWSER_ENGINE")
        .map(|value| {
            value
                .parse::<BrowserEngine>()
                .expect("BROWSER_ENGINE must be chromium or firefox")
        })
        .unwrap_or(BrowserEngine::Chromium);
    let profile = browser_profile_from_environment();
    eprintln!(
        "browser diagnostic profile: engine={engine:?}, user_agent={}, viewport={}x{}, locale={}, expected_timezone={}, extra_args={:?}",
        profile.user_agent.as_deref().unwrap_or("browser-default"),
        profile.viewport.width,
        profile.viewport.height,
        profile.locale,
        profile
            .expected_timezone
            .map(|timezone| timezone.to_string())
            .unwrap_or_else(|| "sidecar-default-not-asserted".to_owned()),
        profile.extra_args,
    );
    let factory = WebDriverFactory::new(endpoint, engine).with_profile(profile.clone());
    let pool = BrowserPool::new(1).expect("positive browser capacity");
    let session = tokio::time::timeout(Duration::from_secs(90), factory.open_public(&pool))
        .await
        .expect("WebDriver session creation timed out")
        .expect("WebDriver sidecar should accept a public session");
    let url = PUBLIC_ARTICLE_URL
        .parse::<VerifiedWechatArticleUrl>()
        .expect("fixture URL should be a verified public WeChat URL");

    let environment = session
        .environment()
        .await
        .expect("browser environment diagnostic should succeed");
    eprintln!("effective browser environment: {environment:?}");
    if let Some(user_agent) = profile.user_agent.as_deref() {
        assert_eq!(environment.user_agent, user_agent);
    }
    assert_eq!(environment.language, profile.locale);
    assert!(environment.inner_width > 0);
    assert!(environment.inner_height > 0);
    assert!(environment.inner_width <= profile.viewport.width);
    assert!(environment.inner_height <= profile.viewport.height);
    if let Some(timezone) = profile.expected_timezone {
        let canonical_timezone = session
            .canonical_timezone(timezone)
            .await
            .expect("browser timezone canonicalization should succeed");
        assert_eq!(environment.timezone, canonical_timezone);
    }

    let fetch_timezone = article_fetch_timezone(&profile);
    let pacing = PacingController::from_seed(real_browser_pacing_policy(), 42);
    let fetch_result = tokio::time::timeout(
        Duration::from_secs(90),
        WebDriverArticlePageFetcher::new(fetch_timezone)
            .with_pacing(pacing)
            .fetch(url, session),
    )
    .await
    .expect("public article navigation timed out");
    match fetch_result {
        Ok(page) => {
            assert_eq!(page.title, PUBLIC_ARTICLE_TITLE);
            assert!(!page.content_html.is_empty());
            assert!(page
                .canonical_url
                .as_str()
                .starts_with("https://mp.weixin.qq.com/s/"));
        }
        Err(werrss::acquisition::article_page::ArticlePageError::VerificationRequired) => {
            eprintln!("WeChat requested environment verification for the public fixture");
        }
        Err(error) => panic!("public article fetch failed unexpectedly: {error}"),
    }
}

fn real_browser_pacing_policy() -> PacingPolicy {
    let delay = |milliseconds| {
        DelayDistribution::new(milliseconds, 0.0, milliseconds, milliseconds)
            .expect("fixed integration-test delay should be valid")
    };
    PacingPolicy::new(
        delay(5.0),
        delay(5.0),
        delay(5.0),
        delay(5.0),
        4,
        4_000,
        Duration::from_secs(30),
    )
    .expect("integration-test pacing policy should be valid")
}

fn article_fetch_timezone(profile: &BrowserProfile) -> chrono_tz::Tz {
    profile.expected_timezone.unwrap_or(Shanghai)
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

#[test]
fn article_fetch_timezone_follows_the_profile_timezone() {
    let profile = BrowserProfile {
        expected_timezone: Some(chrono_tz::UTC),
        ..BrowserProfile::default()
    };
    assert_eq!(article_fetch_timezone(&profile), chrono_tz::UTC);
    assert_eq!(article_fetch_timezone(&BrowserProfile::default()), Shanghai);
}

#[tokio::test]
#[ignore = "requires a WebDriver sidecar configured with the requested timezone"]
async fn browser_canonicalizes_an_iana_timezone_alias() {
    let endpoint = env::var("WEBDRIVER_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:4444".to_owned())
        .parse::<Url>()
        .expect("WEBDRIVER_URL must be a valid URL");
    let expected_timezone = "US/Pacific"
        .parse::<chrono_tz::Tz>()
        .expect("test timezone alias should parse");
    let profile = BrowserProfile {
        expected_timezone: Some(expected_timezone),
        ..BrowserProfile::default()
    };
    let factory = WebDriverFactory::new(endpoint, BrowserEngine::Chromium).with_profile(profile);
    let pool = BrowserPool::new(1).expect("positive browser capacity");
    let session = tokio::time::timeout(Duration::from_secs(30), factory.open_public(&pool))
        .await
        .expect("WebDriver session creation timed out")
        .expect("sidecar should accept the configured timezone");

    let environment = session
        .environment()
        .await
        .expect("browser environment diagnostic should succeed");
    let canonical_timezone = session
        .canonical_timezone(expected_timezone)
        .await
        .expect("browser timezone canonicalization should succeed");
    assert_eq!(environment.timezone, canonical_timezone);
}
