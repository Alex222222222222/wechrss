//! Bounded WeRead QR-login orchestration.
//!
//! The QR flow is deliberately split into a small application state machine
//! and an upstream transport. The state machine owns the local attempt ID,
//! account binding, expiry, cancellation, and single-use behavior. The
//! transport owns WeRead's UID and response shapes. This keeps upstream login
//! details out of the HTTP layer and makes the lifecycle deterministic to
//! test without a live browser or account.
//!
//! Login attempts are short-lived process-local capabilities. The QR payload
//! is returned only by the start operation; status responses never contain the
//! payload or authenticated credentials. A deployment with multiple API
//! replicas should route an attempt's start and polling requests to the same
//! replica until durable encrypted attempt storage is introduced.

use std::{collections::HashMap, fmt, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use qrcode::{render::svg, QrCode};
use secrecy::{ExposeSecret, SecretString};
use serde::Serialize;
use thiserror::Error;
use tokio::sync::Mutex;
use url::Url;
use uuid::Uuid;

use crate::domain::credentials::WeReadAccountId;

const DEFAULT_ATTEMPT_TTL: Duration = Duration::minutes(5);
const DEFAULT_MAX_ACTIVE_ATTEMPTS: usize = 4;

/// The upstream capability created for one QR login attempt.
///
/// The UID is intentionally private and its debug representation is redacted.
/// It is only exposed to the injected transport implementation.
#[derive(Clone)]
pub struct QrLoginChallenge {
    uid: SecretString,
    confirmation_url: Url,
    /// Opaque local key used by transports to isolate per-attempt state.
    transport_key: Uuid,
}

impl fmt::Debug for QrLoginChallenge {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QrLoginChallenge")
            .field("uid", &"<secret>")
            .field("confirmation_url", &"<secret>")
            .finish()
    }
}

impl QrLoginChallenge {
    /// Creates a challenge from the UID returned by WeRead.
    pub fn new(uid: impl Into<String>) -> Result<Self, QrLoginTransportError> {
        let uid = uid.into();
        if uid.trim().is_empty() || uid.chars().any(char::is_control) {
            return Err(QrLoginTransportError::InvalidResponse);
        }
        let mut confirmation_url =
            Url::parse("https://weread.qq.com/web/confirm").expect("constant URL is valid");
        confirmation_url
            .query_pairs_mut()
            .append_pair("pf", "2")
            .append_pair("uid", uid.trim());
        Ok(Self {
            uid: SecretString::new(uid.trim().to_owned().into_boxed_str()),
            confirmation_url,
            transport_key: Uuid::new_v4(),
        })
    }

    /// Returns the upstream UID to a transport implementation.
    pub(crate) fn uid(&self) -> &str {
        self.uid.expose_secret()
    }

    /// Returns the opaque key used to associate transport state with this
    /// challenge without using the upstream UID as a shared-session key.
    pub(crate) const fn transport_key(&self) -> Uuid {
        self.transport_key
    }

    /// Returns the URL encoded in the QR code.
    pub fn confirmation_url(&self) -> &Url {
        &self.confirmation_url
    }
}

/// Credentials produced only after WeRead confirms the QR login.
///
/// This type intentionally has no serialization implementation and redacts
/// its debug representation. The HTTP adapter consumes it immediately to
/// provision or replace an encrypted account record.
pub struct QrAuthenticatedSession {
    access_token: SecretString,
    refresh_token: SecretString,
    cookie_header: SecretString,
    access_expires_at: DateTime<Utc>,
    display_name: Option<String>,
}

impl fmt::Debug for QrAuthenticatedSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QrAuthenticatedSession")
            .field("access_token", &"<secret>")
            .field("refresh_token", &"<secret>")
            .field("cookie_header", &"<secret>")
            .field("access_expires_at", &self.access_expires_at)
            .field("display_name", &self.display_name)
            .finish()
    }
}

impl QrAuthenticatedSession {
    /// Validates an authenticated result before it reaches account storage.
    pub fn new(
        access_token: impl Into<String>,
        refresh_token: impl Into<String>,
        cookie_header: impl Into<String>,
        access_expires_at: DateTime<Utc>,
        display_name: Option<String>,
    ) -> Result<Self, QrLoginTransportError> {
        let access_token = access_token.into();
        let refresh_token = refresh_token.into();
        let cookie_header = cookie_header.into();
        if access_token.trim().is_empty()
            || refresh_token.trim().is_empty()
            || cookie_header.trim().is_empty()
            || cookie_header.chars().any(char::is_control)
            || access_expires_at <= DateTime::<Utc>::UNIX_EPOCH
        {
            return Err(QrLoginTransportError::InvalidResponse);
        }
        Ok(Self {
            access_token: SecretString::new(access_token.trim().to_owned().into_boxed_str()),
            refresh_token: SecretString::new(refresh_token.trim().to_owned().into_boxed_str()),
            cookie_header: SecretString::new(cookie_header.trim().to_owned().into_boxed_str()),
            access_expires_at,
            display_name: display_name
                .map(|name| name.trim().to_owned())
                .filter(|name| !name.is_empty()),
        })
    }

    /// Returns the access token to the account-provisioning boundary.
    pub fn access_token(&self) -> &str {
        self.access_token.expose_secret()
    }

    /// Returns the refresh token to the account-provisioning boundary.
    pub fn refresh_token(&self) -> &str {
        self.refresh_token.expose_secret()
    }

    /// Returns the authenticated browser cookie to the account-provisioning boundary.
    pub fn cookie_header(&self) -> &str {
        self.cookie_header.expose_secret()
    }

    /// Returns the access credential expiry.
    pub const fn access_expires_at(&self) -> DateTime<Utc> {
        self.access_expires_at
    }

    /// Returns the upstream display name hint, if one was supplied.
    pub fn display_name(&self) -> Option<&str> {
        self.display_name.as_deref()
    }
}

/// One response from the upstream QR status endpoint.
#[derive(Debug)]
pub enum QrLoginTransportPoll {
    /// The user has not completed the scan yet.
    Waiting,
    /// The QR code was scanned but confirmation is still pending.
    Scanned,
    /// The upstream returned a complete authenticated session.
    Authenticated(QrAuthenticatedSession),
    /// The upstream challenge can no longer be used.
    Expired,
    /// The upstream refused the login for risk-control reasons.
    RiskControlled,
}

/// Safe failures from the upstream QR transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum QrLoginTransportError {
    /// The upstream could not be reached temporarily.
    #[error("WeRead QR service is temporarily unavailable")]
    Unavailable,
    /// The upstream response did not contain a supported login shape.
    #[error("WeRead QR service returned an invalid response")]
    InvalidResponse,
    /// The upstream rejected the operation for risk-control reasons.
    #[error("WeRead QR login was risk-controlled")]
    RiskControlled,
}

/// Upstream boundary for QR creation, polling, and local cancellation.
#[async_trait]
pub trait QrLoginTransport: Send + Sync {
    /// Obtains a fresh upstream UID.
    async fn begin(&self) -> Result<QrLoginChallenge, QrLoginTransportError>;

    /// Polls the status of one challenge.
    async fn poll(
        &self,
        challenge: &QrLoginChallenge,
    ) -> Result<QrLoginTransportPoll, QrLoginTransportError>;

    /// Releases any transport-side resources for a cancelled challenge.
    async fn cancel(&self, challenge: &QrLoginChallenge) -> Result<(), QrLoginTransportError>;
}

/// Configuration for the local QR login attempt lifecycle.
#[derive(Debug, Clone, Copy)]
pub struct QrLoginConfig {
    attempt_ttl: Duration,
    max_active_attempts: usize,
}

impl Default for QrLoginConfig {
    fn default() -> Self {
        Self {
            attempt_ttl: DEFAULT_ATTEMPT_TTL,
            max_active_attempts: DEFAULT_MAX_ACTIVE_ATTEMPTS,
        }
    }
}

impl QrLoginConfig {
    /// Creates a bounded attempt policy.
    pub fn new(
        attempt_ttl: Duration,
        max_active_attempts: usize,
    ) -> Result<Self, QrLoginConfigError> {
        if attempt_ttl <= Duration::zero() {
            return Err(QrLoginConfigError::InvalidTtl);
        }
        if max_active_attempts == 0 {
            return Err(QrLoginConfigError::InvalidCapacity);
        }
        Ok(Self {
            attempt_ttl,
            max_active_attempts,
        })
    }

    /// Returns the maximum lifetime of one attempt.
    pub const fn attempt_ttl(self) -> Duration {
        self.attempt_ttl
    }
}

/// Invalid QR attempt policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum QrLoginConfigError {
    /// Attempts must expire after a positive duration.
    #[error("QR login attempt TTL must be positive")]
    InvalidTtl,
    /// At least one simultaneous attempt must be allowed.
    #[error("QR login attempt capacity must be positive")]
    InvalidCapacity,
}

/// Safe state exposed by polling and cancellation responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QrLoginState {
    /// Waiting for the operator to scan the QR code.
    WaitingForScan,
    /// Scanned and waiting for confirmation.
    Scanned,
    /// Credentials were confirmed and stored.
    Completed,
    /// The attempt exceeded its local deadline or upstream expiry.
    Expired,
    /// The administrator cancelled the attempt.
    Cancelled,
    /// WeRead refused the login for risk-control reasons.
    RiskControlled,
}

/// Safe status response for one QR attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QrLoginStatusResponse {
    /// Local single-use attempt identity.
    pub attempt_id: Uuid,
    /// Current non-secret lifecycle state.
    pub status: QrLoginState,
    /// Local deadline; no QR payload is returned here.
    pub expires_at: DateTime<Utc>,
}

/// Response returned when a QR attempt is started.
///
/// The SVG is returned only at creation time. It contains the short-lived
/// upstream confirmation capability and must not be persisted or logged.
pub struct QrLoginStarted {
    /// Local attempt identity used for subsequent polling.
    pub attempt_id: Uuid,
    /// Account identity that will receive the confirmed session.
    pub account_id: WeReadAccountId,
    /// QR image as server-generated SVG.
    pub qr_svg: String,
    /// Local attempt deadline.
    pub expires_at: DateTime<Utc>,
}

/// Result of polling a QR attempt.
#[derive(Debug)]
pub enum QrLoginPollResult {
    /// The attempt remains active or reached a safe terminal state.
    Status(QrLoginStatusResponse),
    /// The upstream confirmed a session; the HTTP layer must provision it.
    Authenticated {
        /// Local account that owns this attempt.
        account_id: WeReadAccountId,
        /// Optional name supplied when the attempt was started.
        requested_display_name: Option<String>,
        /// Confirmed upstream credentials, never serializable.
        session: QrAuthenticatedSession,
    },
}

#[derive(Debug)]
struct QrLoginAttempt {
    challenge: QrLoginChallenge,
    account_id: WeReadAccountId,
    requested_display_name: Option<String>,
    expires_at: DateTime<Utc>,
    state: QrLoginState,
}

#[derive(Debug)]
struct AttemptCell {
    state: Mutex<QrLoginAttempt>,
}

/// In-process bounded QR login lifecycle manager.
#[derive(Clone)]
pub struct QrLoginManager<T> {
    transport: Arc<T>,
    attempts: Arc<Mutex<HashMap<Uuid, Arc<AttemptCell>>>>,
    config: QrLoginConfig,
}

impl<T> fmt::Debug for QrLoginManager<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QrLoginManager")
            .field("attempts", &"<redacted>")
            .field("config", &self.config)
            .finish()
    }
}

impl<T> QrLoginManager<T>
where
    T: QrLoginTransport + 'static,
{
    /// Creates a manager with the default five-minute, bounded policy.
    pub fn new(transport: T) -> Self {
        Self::with_config(transport, QrLoginConfig::default())
    }

    /// Creates a manager with an explicit lifecycle policy.
    pub fn with_config(transport: T, config: QrLoginConfig) -> Self {
        Self {
            transport: Arc::new(transport),
            attempts: Arc::new(Mutex::new(HashMap::new())),
            config,
        }
    }

    /// Starts an account-bound attempt and returns its one-time QR SVG.
    pub async fn start(
        &self,
        account_id: Option<WeReadAccountId>,
        display_name: Option<String>,
    ) -> Result<QrLoginStarted, QrLoginError> {
        self.start_at(account_id, display_name, Utc::now()).await
    }

    /// Deterministic start boundary used by unit and integration tests.
    pub async fn start_at(
        &self,
        account_id: Option<WeReadAccountId>,
        display_name: Option<String>,
        now: DateTime<Utc>,
    ) -> Result<QrLoginStarted, QrLoginError> {
        let account_id = account_id.unwrap_or_else(|| WeReadAccountId::from_uuid(Uuid::new_v4()));
        let requested_display_name = display_name
            .map(|name| name.trim().to_owned())
            .filter(|name| !name.is_empty());

        self.prune_expired(now).await;
        {
            let attempts = self.attempts.lock().await;
            if attempts.len() >= self.config.max_active_attempts {
                return Err(QrLoginError::TooManyActiveAttempts);
            }
        }

        let challenge = self.transport.begin().await?;
        let qr_svg = match render_qr_svg(challenge.confirmation_url()) {
            Ok(qr_svg) => qr_svg,
            Err(error) => {
                self.release_transport(&challenge).await;
                return Err(error);
            }
        };
        let attempt_id = Uuid::new_v4();
        let expires_at = match now.checked_add_signed(self.config.attempt_ttl) {
            Some(expires_at) => expires_at,
            None => {
                self.release_transport(&challenge).await;
                return Err(QrLoginError::InvalidDeadline);
            }
        };
        let attempt = Arc::new(AttemptCell {
            state: Mutex::new(QrLoginAttempt {
                challenge: challenge.clone(),
                account_id,
                requested_display_name,
                expires_at,
                state: QrLoginState::WaitingForScan,
            }),
        });
        self.prune_expired(now).await;
        let mut attempts = self.attempts.lock().await;
        if attempts.len() >= self.config.max_active_attempts {
            drop(attempts);
            self.release_transport(&challenge).await;
            return Err(QrLoginError::TooManyActiveAttempts);
        }
        attempts.insert(attempt_id, attempt);
        tracing::info!(account_id = %account_id, "WeRead QR login attempt started");
        Ok(QrLoginStarted {
            attempt_id,
            account_id,
            qr_svg,
            expires_at,
        })
    }

    /// Polls upstream and returns only safe state unless login completed.
    pub async fn poll(&self, attempt_id: Uuid) -> Result<QrLoginPollResult, QrLoginError> {
        self.poll_at_with_clock(attempt_id, Utc::now(), Utc::now)
            .await
    }

    /// Deterministic polling boundary used by unit and integration tests.
    pub async fn poll_at(
        &self,
        attempt_id: Uuid,
        now: DateTime<Utc>,
    ) -> Result<QrLoginPollResult, QrLoginError> {
        self.poll_at_with_clock(attempt_id, now, move || now).await
    }

    async fn poll_at_with_clock<F>(
        &self,
        attempt_id: Uuid,
        now: DateTime<Utc>,
        clock: F,
    ) -> Result<QrLoginPollResult, QrLoginError>
    where
        F: Fn() -> DateTime<Utc> + Copy + Send + Sync,
    {
        let attempt = self
            .attempts
            .lock()
            .await
            .get(&attempt_id)
            .cloned()
            .ok_or(QrLoginError::AttemptNotFound)?;
        let mut state = attempt.state.lock().await;
        if !self.is_current_attempt(attempt_id, &attempt).await {
            return Err(QrLoginError::AttemptNotFound);
        }
        if state.expires_at <= now {
            let response = status_for(attempt_id, &state, QrLoginState::Expired);
            let challenge = state.challenge.clone();
            drop(state);
            self.remove_attempt(attempt_id, &attempt).await;
            self.release_transport(&challenge).await;
            tracing::warn!("WeRead QR login attempt expired");
            return Ok(QrLoginPollResult::Status(response));
        }

        let result = self.transport.poll(&state.challenge).await?;
        if state.expires_at <= clock() {
            let response = status_for(attempt_id, &state, QrLoginState::Expired);
            let challenge = state.challenge.clone();
            drop(state);
            self.remove_attempt(attempt_id, &attempt).await;
            self.release_transport(&challenge).await;
            tracing::warn!("WeRead QR login attempt expired while polling");
            return Ok(QrLoginPollResult::Status(response));
        }
        match result {
            QrLoginTransportPoll::Waiting => {
                state.state = QrLoginState::WaitingForScan;
                Ok(QrLoginPollResult::Status(status_for(
                    attempt_id,
                    &state,
                    state.state,
                )))
            }
            QrLoginTransportPoll::Scanned => {
                state.state = QrLoginState::Scanned;
                Ok(QrLoginPollResult::Status(status_for(
                    attempt_id,
                    &state,
                    state.state,
                )))
            }
            QrLoginTransportPoll::Expired => {
                let response = status_for(attempt_id, &state, QrLoginState::Expired);
                drop(state);
                self.remove_attempt(attempt_id, &attempt).await;
                Ok(QrLoginPollResult::Status(response))
            }
            QrLoginTransportPoll::RiskControlled => {
                let response = status_for(attempt_id, &state, QrLoginState::RiskControlled);
                drop(state);
                self.remove_attempt(attempt_id, &attempt).await;
                tracing::warn!("WeRead QR login was risk-controlled");
                Ok(QrLoginPollResult::Status(response))
            }
            QrLoginTransportPoll::Authenticated(session) => {
                // The attempt is consumed before the caller provisions the
                // account. A failed persistence operation must require a new
                // login rather than making the upstream session reusable.
                let account_id = state.account_id;
                let requested_display_name = state.requested_display_name.clone();
                drop(state);
                self.remove_attempt(attempt_id, &attempt).await;
                tracing::info!(account_id = %account_id, "WeRead QR login confirmed");
                Ok(QrLoginPollResult::Authenticated {
                    account_id,
                    requested_display_name,
                    session,
                })
            }
        }
    }

    /// Cancels and consumes one attempt.
    pub async fn cancel(&self, attempt_id: Uuid) -> Result<QrLoginStatusResponse, QrLoginError> {
        self.cancel_at(attempt_id, Utc::now()).await
    }

    /// Deterministic cancellation boundary used by unit tests.
    pub async fn cancel_at(
        &self,
        attempt_id: Uuid,
        now: DateTime<Utc>,
    ) -> Result<QrLoginStatusResponse, QrLoginError> {
        let attempt = self
            .attempts
            .lock()
            .await
            .get(&attempt_id)
            .cloned()
            .ok_or(QrLoginError::AttemptNotFound)?;
        let mut state = attempt.state.lock().await;
        if !self.is_current_attempt(attempt_id, &attempt).await {
            return Err(QrLoginError::AttemptNotFound);
        }
        let response = QrLoginStatusResponse {
            attempt_id,
            status: if state.expires_at <= now {
                QrLoginState::Expired
            } else {
                QrLoginState::Cancelled
            },
            expires_at: state.expires_at,
        };
        self.transport.cancel(&state.challenge).await?;
        state.state = response.status;
        drop(state);
        self.remove_attempt(attempt_id, &attempt).await;
        tracing::info!("WeRead QR login attempt cancelled");
        Ok(response)
    }

    async fn is_current_attempt(&self, attempt_id: Uuid, expected: &Arc<AttemptCell>) -> bool {
        self.attempts
            .lock()
            .await
            .get(&attempt_id)
            .is_some_and(|current| Arc::ptr_eq(current, expected))
    }

    async fn prune_expired(&self, now: DateTime<Utc>) {
        let expired = {
            let mut attempts = self.attempts.lock().await;
            let mut expired = Vec::new();
            attempts.retain(|_, attempt| {
                let Ok(state) = attempt.state.try_lock() else {
                    return true;
                };
                if state.expires_at <= now {
                    expired.push(state.challenge.clone());
                    false
                } else {
                    true
                }
            });
            expired
        };
        for challenge in expired {
            self.release_transport(&challenge).await;
        }
    }

    async fn remove_attempt(&self, attempt_id: Uuid, expected: &Arc<AttemptCell>) {
        let mut attempts = self.attempts.lock().await;
        if attempts
            .get(&attempt_id)
            .is_some_and(|current| Arc::ptr_eq(current, expected))
        {
            attempts.remove(&attempt_id);
        }
    }

    async fn release_transport(&self, challenge: &QrLoginChallenge) {
        if let Err(error) = self.transport.cancel(challenge).await {
            tracing::warn!(
                error = %error,
                "unable to release expired WeRead QR login transport state"
            );
        }
    }
}

/// Object-safe admin boundary for the generic QR manager.
#[async_trait]
pub trait QrLoginService: Send + Sync {
    /// Starts a QR attempt.
    async fn start(
        &self,
        account_id: Option<WeReadAccountId>,
        display_name: Option<String>,
    ) -> Result<QrLoginStarted, QrLoginError>;

    /// Polls a QR attempt.
    async fn poll(&self, attempt_id: Uuid) -> Result<QrLoginPollResult, QrLoginError>;

    /// Cancels a QR attempt.
    async fn cancel(&self, attempt_id: Uuid) -> Result<QrLoginStatusResponse, QrLoginError>;
}

#[async_trait]
impl<T> QrLoginService for QrLoginManager<T>
where
    T: QrLoginTransport + 'static,
{
    async fn start(
        &self,
        account_id: Option<WeReadAccountId>,
        display_name: Option<String>,
    ) -> Result<QrLoginStarted, QrLoginError> {
        QrLoginManager::start(self, account_id, display_name).await
    }

    async fn poll(&self, attempt_id: Uuid) -> Result<QrLoginPollResult, QrLoginError> {
        QrLoginManager::poll(self, attempt_id).await
    }

    async fn cancel(&self, attempt_id: Uuid) -> Result<QrLoginStatusResponse, QrLoginError> {
        QrLoginManager::cancel(self, attempt_id).await
    }
}

/// QR lifecycle failures safe to expose through an admin response mapper.
#[derive(Debug, Error)]
pub enum QrLoginError {
    /// The requested attempt does not exist or was already consumed.
    #[error("QR login attempt was not found or has already been consumed")]
    AttemptNotFound,
    /// The process already has its configured number of live attempts.
    #[error("too many QR login attempts are active")]
    TooManyActiveAttempts,
    /// The upstream transport failed safely.
    #[error(transparent)]
    Transport(#[from] QrLoginTransportError),
    /// The configured deadline could not be represented.
    #[error("QR login attempt deadline is invalid")]
    InvalidDeadline,
    /// QR SVG generation failed without including the payload.
    #[error("QR login image could not be generated")]
    QrCode,
}

fn status_for(
    attempt_id: Uuid,
    attempt: &QrLoginAttempt,
    state: QrLoginState,
) -> QrLoginStatusResponse {
    QrLoginStatusResponse {
        attempt_id,
        status: state,
        expires_at: attempt.expires_at,
    }
}

fn render_qr_svg(url: &Url) -> Result<String, QrLoginError> {
    QrCode::new(url.as_str().as_bytes())
        .map_err(|_| QrLoginError::QrCode)
        .map(|code| {
            code.render::<svg::Color>()
                .min_dimensions(240, 240)
                .max_dimensions(320, 320)
                .build()
        })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::sync::Notify;

    use super::*;

    #[derive(Clone)]
    struct MockTransport {
        begin_uid: String,
        polls: Arc<Mutex<Vec<Result<QrLoginTransportPoll, QrLoginTransportError>>>>,
        cancellations: Arc<AtomicUsize>,
    }

    impl MockTransport {
        fn new(results: Vec<Result<QrLoginTransportPoll, QrLoginTransportError>>) -> Self {
            Self {
                begin_uid: "uid-for-test".to_owned(),
                polls: Arc::new(Mutex::new(results)),
                cancellations: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn with_begin_uid(mut self, begin_uid: String) -> Self {
            self.begin_uid = begin_uid;
            self
        }
    }

    #[async_trait]
    impl QrLoginTransport for MockTransport {
        async fn begin(&self) -> Result<QrLoginChallenge, QrLoginTransportError> {
            QrLoginChallenge::new(self.begin_uid.clone())
        }

        async fn poll(
            &self,
            _challenge: &QrLoginChallenge,
        ) -> Result<QrLoginTransportPoll, QrLoginTransportError> {
            self.polls.lock().await.remove(0)
        }

        async fn cancel(&self, _challenge: &QrLoginChallenge) -> Result<(), QrLoginTransportError> {
            self.cancellations.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[derive(Clone)]
    struct BlockingPollTransport {
        poll_started: Arc<Notify>,
        release_poll: Arc<Notify>,
        cancellations: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl QrLoginTransport for BlockingPollTransport {
        async fn begin(&self) -> Result<QrLoginChallenge, QrLoginTransportError> {
            QrLoginChallenge::new("uid-for-test")
        }

        async fn poll(
            &self,
            _challenge: &QrLoginChallenge,
        ) -> Result<QrLoginTransportPoll, QrLoginTransportError> {
            self.poll_started.notify_one();
            self.release_poll.notified().await;
            Ok(QrLoginTransportPoll::Authenticated(session()))
        }

        async fn cancel(&self, _challenge: &QrLoginChallenge) -> Result<(), QrLoginTransportError> {
            self.cancellations.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn now() -> DateTime<Utc> {
        "2026-09-04T00:00:00Z".parse().unwrap()
    }

    fn session() -> QrAuthenticatedSession {
        QrAuthenticatedSession::new(
            "access",
            "refresh",
            "wr_vid=vid; wr_skey=access; wr_rt=refresh; wr_name=Test",
            now() + Duration::hours(1),
            Some("Test account".to_owned()),
        )
        .unwrap()
    }

    #[test]
    fn rejects_invalid_attempt_configuration() {
        assert_eq!(
            QrLoginConfig::new(Duration::zero(), 1).unwrap_err(),
            QrLoginConfigError::InvalidTtl
        );
        assert_eq!(
            QrLoginConfig::new(Duration::minutes(1), 0).unwrap_err(),
            QrLoginConfigError::InvalidCapacity
        );
    }

    #[tokio::test]
    async fn start_returns_a_local_attempt_and_scannable_svg() {
        let manager = QrLoginManager::new(MockTransport::new(vec![]));
        let started = manager
            .start_at(None, Some("  Account  ".to_owned()), now())
            .await
            .unwrap();

        assert!(!started.attempt_id.is_nil());
        assert!(!started.account_id.as_uuid().is_nil());
        assert!(started.qr_svg.contains("<svg"));
        assert_eq!(started.expires_at, now() + Duration::minutes(5));
    }

    #[tokio::test]
    async fn start_releases_transport_when_qr_rendering_fails() {
        let transport = MockTransport::new(vec![]).with_begin_uid("x".repeat(5_000));
        let cancellations = transport.cancellations.clone();
        let manager = QrLoginManager::new(transport);

        assert!(matches!(
            manager.start_at(None, None, now()).await,
            Err(QrLoginError::QrCode)
        ));
        assert_eq!(cancellations.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn start_releases_transport_when_deadline_cannot_be_represented() {
        let transport = MockTransport::new(vec![]);
        let cancellations = transport.cancellations.clone();
        let manager = QrLoginManager::new(transport);

        assert!(matches!(
            manager.start_at(None, None, DateTime::<Utc>::MAX_UTC).await,
            Err(QrLoginError::InvalidDeadline)
        ));
        assert_eq!(cancellations.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn pending_and_scanned_states_never_expose_the_qr_payload() {
        let manager = QrLoginManager::new(MockTransport::new(vec![
            Ok(QrLoginTransportPoll::Waiting),
            Ok(QrLoginTransportPoll::Scanned),
        ]));
        let started = manager.start_at(None, None, now()).await.unwrap();

        let pending = manager.poll_at(started.attempt_id, now()).await.unwrap();
        let scanned = manager.poll_at(started.attempt_id, now()).await.unwrap();
        for result in [pending, scanned] {
            let QrLoginPollResult::Status(status) = result else {
                panic!("poll should remain pending")
            };
            assert_eq!(status.attempt_id, started.attempt_id);
            assert!(
                serde_json::to_string(&status)
                    .unwrap()
                    .contains("waiting_for_scan")
                    || serde_json::to_string(&status).unwrap().contains("scanned")
            );
            assert!(!serde_json::to_string(&status)
                .unwrap()
                .contains("uid-for-test"));
        }
    }

    #[tokio::test]
    async fn confirmed_state_is_single_use_and_returns_bound_account() {
        let account_id = WeReadAccountId::from_uuid(Uuid::from_u128(7));
        let manager = QrLoginManager::new(MockTransport::new(vec![Ok(
            QrLoginTransportPoll::Authenticated(session()),
        )]));
        let started = manager
            .start_at(Some(account_id), Some("Requested".to_owned()), now())
            .await
            .unwrap();

        let result = manager.poll_at(started.attempt_id, now()).await.unwrap();
        let QrLoginPollResult::Authenticated {
            account_id: returned_id,
            requested_display_name,
            session,
        } = result
        else {
            panic!("poll should return authenticated session")
        };
        assert_eq!(returned_id, account_id);
        assert_eq!(requested_display_name.as_deref(), Some("Requested"));
        assert_eq!(session.access_token(), "access");

        assert_eq!(
            manager
                .poll_at(started.attempt_id, now())
                .await
                .unwrap_err()
                .to_string(),
            QrLoginError::AttemptNotFound.to_string()
        );
    }

    #[tokio::test]
    async fn cancellation_cannot_win_after_a_concurrent_poll_confirms_login() {
        let transport = BlockingPollTransport {
            poll_started: Arc::new(Notify::new()),
            release_poll: Arc::new(Notify::new()),
            cancellations: Arc::new(AtomicUsize::new(0)),
        };
        let manager = QrLoginManager::new(transport.clone());
        let started = manager.start_at(None, None, now()).await.unwrap();

        let poll_manager = manager.clone();
        let attempt_id = started.attempt_id;
        let poll_task = tokio::spawn(async move { poll_manager.poll_at(attempt_id, now()).await });
        transport.poll_started.notified().await;

        let cancel_manager = manager.clone();
        let cancel_task =
            tokio::spawn(async move { cancel_manager.cancel_at(attempt_id, now()).await });
        transport.release_poll.notify_one();

        let poll_result = poll_task.await.unwrap().unwrap();
        assert!(matches!(
            poll_result,
            QrLoginPollResult::Authenticated { .. }
        ));
        assert_eq!(
            cancel_task.await.unwrap().unwrap_err().to_string(),
            QrLoginError::AttemptNotFound.to_string()
        );
        assert_eq!(transport.cancellations.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn authenticated_result_that_crosses_the_deadline_is_expired() {
        let transport = BlockingPollTransport {
            poll_started: Arc::new(Notify::new()),
            release_poll: Arc::new(Notify::new()),
            cancellations: Arc::new(AtomicUsize::new(0)),
        };
        let manager = QrLoginManager::new(transport.clone());
        let started = manager.start_at(None, None, now()).await.unwrap();

        let poll_manager = manager.clone();
        let attempt_id = started.attempt_id;
        let poll_task = tokio::spawn(async move {
            poll_manager
                .poll_at_with_clock(attempt_id, now(), || now() + Duration::minutes(5))
                .await
        });
        transport.poll_started.notified().await;
        transport.release_poll.notify_one();

        let result = poll_task.await.unwrap().unwrap();
        let QrLoginPollResult::Status(status) = result else {
            panic!("an expired authenticated result should return an expired status")
        };
        assert_eq!(status.status, QrLoginState::Expired);
        assert!(matches!(
            manager.poll_at(started.attempt_id, now()).await,
            Err(QrLoginError::AttemptNotFound)
        ));
    }

    #[tokio::test]
    async fn expired_attempt_is_consumed_without_polling_upstream() {
        let manager = QrLoginManager::with_config(
            MockTransport::new(vec![Ok(QrLoginTransportPoll::Waiting)]),
            QrLoginConfig::new(Duration::minutes(1), 1).unwrap(),
        );
        let started = manager.start_at(None, None, now()).await.unwrap();

        let result = manager
            .poll_at(started.attempt_id, now() + Duration::minutes(1))
            .await
            .unwrap();
        let QrLoginPollResult::Status(status) = result else {
            panic!("expired poll should return status")
        };
        assert_eq!(status.status, QrLoginState::Expired);
        assert!(matches!(
            manager.poll_at(started.attempt_id, now()).await,
            Err(QrLoginError::AttemptNotFound)
        ));
    }

    #[tokio::test]
    async fn cancellation_releases_transport_and_consumes_attempt() {
        let transport = MockTransport::new(vec![]);
        let cancellations = transport.cancellations.clone();
        let manager = QrLoginManager::new(transport);
        let started = manager.start_at(None, None, now()).await.unwrap();

        let response = manager.cancel_at(started.attempt_id, now()).await.unwrap();
        assert_eq!(response.status, QrLoginState::Cancelled);
        assert_eq!(cancellations.load(Ordering::SeqCst), 1);
        assert!(matches!(
            manager.poll_at(started.attempt_id, now()).await,
            Err(QrLoginError::AttemptNotFound)
        ));
    }

    #[tokio::test]
    async fn active_attempt_capacity_is_enforced() {
        let manager = QrLoginManager::with_config(
            MockTransport::new(vec![]),
            QrLoginConfig::new(Duration::minutes(5), 1).unwrap(),
        );
        manager.start_at(None, None, now()).await.unwrap();
        assert!(matches!(
            manager.start_at(None, None, now()).await,
            Err(QrLoginError::TooManyActiveAttempts)
        ));
    }

    #[test]
    fn challenge_debug_and_session_debug_redact_secrets() {
        let challenge = QrLoginChallenge::new("uid-secret").unwrap();
        let session = QrAuthenticatedSession::new(
            "access-secret",
            "refresh-secret",
            "wr_vid=vid; wr_skey=access-secret; wr_rt=refresh-secret",
            now() + Duration::hours(1),
            None,
        )
        .unwrap();
        let debug = format!("{challenge:?} {session:?}");
        assert!(!debug.contains("uid-secret"));
        assert!(!debug.contains("access-secret"));
        assert!(!debug.contains("refresh-secret"));
        assert!(!debug.contains("wr_vid=vid"));
    }
}
