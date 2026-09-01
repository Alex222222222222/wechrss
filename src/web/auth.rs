//! Single-administrator authentication for the management API and panel.
//!
//! The administrator identity is deliberately environment-backed rather than
//! persisted as a user-management record. Sessions are signed, short-lived
//! cookies; the signing key is independent from both the administrator
//! password and the credential-encryption key. Mutating API requests must also
//! present the CSRF value carried in the signed session.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, Duration, Utc};
use rand::Rng as _;
use ring::hmac;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const SESSION_COOKIE: &str = "wechrss_admin_session";
const SESSION_TTL: Duration = Duration::hours(12);
const RATE_LIMIT_WINDOW: Duration = Duration::minutes(1);
const RATE_LIMIT_FAILURES: u32 = 5;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SessionClaims {
    username: String,
    csrf_token: String,
    expires_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminSession {
    username: String,
    csrf_token: String,
    expires_at: DateTime<Utc>,
}

impl AdminSession {
    /// Returns the authenticated administrator name.
    pub fn username(&self) -> &str {
        &self.username
    }

    /// Returns the CSRF value required for state-changing requests.
    pub fn csrf_token(&self) -> &str {
        &self.csrf_token
    }

    /// Returns the session expiry time.
    pub const fn expires_at(&self) -> DateTime<Utc> {
        self.expires_at
    }
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum AuthError {
    #[error("invalid administrator credentials")]
    InvalidCredentials,
    #[error("administrator login temporarily rate limited")]
    RateLimited,
    #[error("invalid or expired administrator session")]
    InvalidSession,
    #[error("administrator session is missing CSRF protection")]
    InvalidCsrf,
}

#[derive(Debug, Clone, Copy)]
struct AttemptWindow {
    started_at: DateTime<Utc>,
    failures: u32,
}

#[derive(Debug, Default)]
struct LoginRateLimiter {
    attempts: HashMap<String, AttemptWindow>,
}

impl LoginRateLimiter {
    fn allow(&mut self, key: &str, now: DateTime<Utc>) -> bool {
        self.expire(now);
        self.attempts
            .get(key)
            .is_none_or(|attempt| attempt.failures < RATE_LIMIT_FAILURES)
    }

    fn failure(&mut self, key: &str, now: DateTime<Utc>) {
        self.expire(now);
        let attempt = self
            .attempts
            .entry(key.to_owned())
            .or_insert(AttemptWindow {
                started_at: now,
                failures: 0,
            });
        attempt.failures = attempt.failures.saturating_add(1);
    }

    fn success(&mut self, key: &str) {
        self.attempts.remove(key);
    }

    fn expire(&mut self, now: DateTime<Utc>) {
        self.attempts
            .retain(|_, attempt| now.signed_duration_since(attempt.started_at) < RATE_LIMIT_WINDOW);
    }
}

/// Signed-cookie authentication for the one configured administrator.
#[derive(Clone)]
pub struct AdminAuthenticator {
    username: String,
    password: SecretString,
    signing_key: Vec<u8>,
    limiter: Arc<Mutex<LoginRateLimiter>>,
}

impl AdminAuthenticator {
    /// Builds authentication from the already-validated application config.
    pub fn new(
        username: String,
        password: SecretString,
        signing_key: SecretString,
    ) -> Result<Self, AuthConfigError> {
        if username.trim().is_empty() {
            return Err(AuthConfigError::EmptyUsername);
        }
        if password.expose_secret().is_empty() {
            return Err(AuthConfigError::EmptyPassword);
        }
        if signing_key.expose_secret().is_empty() {
            return Err(AuthConfigError::EmptySigningKey);
        }
        Ok(Self {
            username,
            password,
            signing_key: signing_key.expose_secret().as_bytes().to_vec(),
            limiter: Arc::new(Mutex::new(LoginRateLimiter::default())),
        })
    }

    /// Attempts a login and returns a signed session cookie value.
    pub fn login(
        &self,
        username: &str,
        password: &str,
        client_key: &str,
        now: DateTime<Utc>,
    ) -> Result<(AdminSession, String), AuthError> {
        let mut limiter = self.limiter.lock().expect("login limiter mutex poisoned");
        if !limiter.allow(client_key, now) {
            return Err(AuthError::RateLimited);
        }
        let username_matches = constant_time_eq(username.as_bytes(), self.username.as_bytes());
        let password_matches = constant_time_eq(
            password.as_bytes(),
            self.password.expose_secret().as_bytes(),
        );
        if !username_matches || !password_matches {
            limiter.failure(client_key, now);
            return Err(AuthError::InvalidCredentials);
        }
        limiter.success(client_key);
        drop(limiter);

        let expires_at = now + SESSION_TTL;
        let session = AdminSession {
            username: self.username.clone(),
            csrf_token: random_token(24),
            expires_at,
        };
        Ok((session.clone(), self.encode(&session)?))
    }

    /// Verifies the signed session cookie from a request header.
    pub fn authenticate_cookie(
        &self,
        cookie_header: Option<&str>,
        now: DateTime<Utc>,
    ) -> Result<AdminSession, AuthError> {
        let value = cookie_header
            .and_then(|header| {
                header.split(';').find_map(|part| {
                    let (name, value) = part.trim().split_once('=')?;
                    (name == SESSION_COOKIE).then_some(value)
                })
            })
            .ok_or(AuthError::InvalidSession)?;
        let (payload, signature) = value.split_once('.').ok_or(AuthError::InvalidSession)?;
        let payload_bytes = URL_SAFE_NO_PAD
            .decode(payload)
            .map_err(|_| AuthError::InvalidSession)?;
        let signature_bytes = URL_SAFE_NO_PAD
            .decode(signature)
            .map_err(|_| AuthError::InvalidSession)?;
        hmac::verify(
            &hmac::Key::new(hmac::HMAC_SHA256, &self.signing_key),
            &payload_bytes,
            &signature_bytes,
        )
        .map_err(|_| AuthError::InvalidSession)?;
        let claims: SessionClaims =
            serde_json::from_slice(&payload_bytes).map_err(|_| AuthError::InvalidSession)?;
        let expires_at =
            DateTime::from_timestamp(claims.expires_at, 0).ok_or(AuthError::InvalidSession)?;
        if claims.username != self.username || expires_at <= now || claims.csrf_token.is_empty() {
            return Err(AuthError::InvalidSession);
        }
        Ok(AdminSession {
            username: claims.username,
            csrf_token: claims.csrf_token,
            expires_at,
        })
    }

    /// Checks a CSRF token using a constant-time comparison.
    pub fn verify_csrf(
        &self,
        session: &AdminSession,
        supplied: Option<&str>,
    ) -> Result<(), AuthError> {
        let supplied = supplied.ok_or(AuthError::InvalidCsrf)?;
        constant_time_eq(supplied.as_bytes(), session.csrf_token.as_bytes())
            .then_some(())
            .ok_or(AuthError::InvalidCsrf)
    }

    /// Returns a `Set-Cookie` value for an authenticated session.
    pub fn session_cookie(cookie_value: &str) -> String {
        format!("{SESSION_COOKIE}={cookie_value}; Path=/; HttpOnly; SameSite=Lax; Secure")
    }

    /// Returns a `Set-Cookie` value that removes the current session.
    pub fn clear_cookie() -> &'static str {
        "wechrss_admin_session=; Path=/; HttpOnly; SameSite=Lax; Secure; Max-Age=0"
    }

    fn encode(&self, session: &AdminSession) -> Result<String, AuthError> {
        let payload = serde_json::to_vec(&SessionClaims {
            username: session.username.clone(),
            csrf_token: session.csrf_token.clone(),
            expires_at: session.expires_at.timestamp(),
        })
        .map_err(|_| AuthError::InvalidSession)?;
        let signature = hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA256, &self.signing_key),
            &payload,
        );
        Ok(format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(payload),
            URL_SAFE_NO_PAD.encode(signature.as_ref())
        ))
    }
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum AuthConfigError {
    #[error("administrator username must not be empty")]
    EmptyUsername,
    #[error("administrator password must not be empty")]
    EmptyPassword,
    #[error("session signing key must not be empty")]
    EmptySigningKey,
}

fn random_token(length: usize) -> String {
    let mut bytes = vec![0_u8; length];
    rand::rng().fill(bytes.as_mut_slice());
    URL_SAFE_NO_PAD.encode(bytes)
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    for index in 0..left.len().max(right.len()) {
        difference |= usize::from(
            left.get(index).copied().unwrap_or_default()
                ^ right.get(index).copied().unwrap_or_default(),
        );
    }
    difference == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn authenticator() -> AdminAuthenticator {
        AdminAuthenticator::new(
            "admin".to_owned(),
            SecretString::new("correct horse".to_owned().into_boxed_str()),
            SecretString::new("independent signing key".to_owned().into_boxed_str()),
        )
        .expect("test auth should be valid")
    }

    #[test]
    fn signed_session_round_trips_and_tampering_is_rejected() {
        let auth = authenticator();
        let now = "2026-09-01T00:00:00Z".parse().unwrap();
        let (session, cookie) = auth.login("admin", "correct horse", "client", now).unwrap();
        let header = format!(
            "{}; theme=dark",
            AdminAuthenticator::session_cookie(&cookie)
        );
        let restored = auth.authenticate_cookie(Some(&header), now).unwrap();
        assert_eq!(restored, session);
        let mut tampered = cookie.clone();
        tampered.replace_range(0..1, if &tampered[0..1] == "e" { "f" } else { "e" });
        assert_eq!(
            auth.authenticate_cookie(Some(&format!("wechrss_admin_session={tampered}")), now),
            Err(AuthError::InvalidSession)
        );
    }

    #[test]
    fn expired_sessions_and_wrong_csrf_are_rejected() {
        let auth = authenticator();
        let now = "2026-09-01T00:00:00Z".parse().unwrap();
        let (session, cookie) = auth.login("admin", "correct horse", "client", now).unwrap();
        assert_eq!(
            auth.authenticate_cookie(
                Some(&format!("wechrss_admin_session={cookie}")),
                now + SESSION_TTL
            ),
            Err(AuthError::InvalidSession)
        );
        assert_eq!(
            auth.verify_csrf(&session, Some("wrong")),
            Err(AuthError::InvalidCsrf)
        );
        auth.verify_csrf(&session, Some(session.csrf_token()))
            .unwrap();
    }

    #[test]
    fn failed_logins_are_limited_per_client_and_expire() {
        let auth = authenticator();
        let now = "2026-09-01T00:00:00Z".parse().unwrap();
        for _ in 0..RATE_LIMIT_FAILURES {
            assert_eq!(
                auth.login("admin", "wrong", "one", now),
                Err(AuthError::InvalidCredentials)
            );
        }
        assert_eq!(
            auth.login("admin", "correct horse", "one", now),
            Err(AuthError::RateLimited)
        );
        assert!(auth.login("admin", "correct horse", "two", now).is_ok());
        assert!(auth
            .login("admin", "correct horse", "one", now + RATE_LIMIT_WINDOW)
            .is_ok());
    }

    #[test]
    fn admin_session_cookies_are_secure() {
        assert!(AdminAuthenticator::session_cookie("cookie").contains("; Secure"));
        assert!(AdminAuthenticator::clear_cookie().contains("; Secure"));
    }
}
