//! HTTP transport for WeRead's QR login endpoints.
//!
//! This adapter follows the flow used by the reference WeRead integration:
//! obtain a login UID from /web/login/getuid, encode
//! /web/confirm?pf=2&uid=... in a locally generated QR code, and poll
//! /web/login/getinfo. Once the scan is confirmed, the returned login info is
//! exchanged through /web/login/weblogin and /web/login/session/init, then
//! bootstraps the user cookie set through /web/user. The application layer owns
//! the attempt deadline and account provisioning; this module only translates
//! upstream response shapes into safe typed results.

use std::{collections::HashMap, sync::Arc, time::Duration as StdDuration};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use reqwest::{header, Client, Response};
use serde_json::{Map, Value};
use tokio::sync::Mutex;
use url::Url;
use uuid::Uuid;

use crate::application::qr_login::{
    QrAuthenticatedSession, QrLoginChallenge, QrLoginTransport, QrLoginTransportError,
    QrLoginTransportPoll,
};

const WEREAD_BASE_URL: &str = "https://weread.qq.com";
const LOGIN_UID_PATH: &str = "/web/login/getuid";
const LOGIN_INFO_PATH: &str = "/web/login/getinfo";
const WEB_LOGIN_PATH: &str = "/web/login/weblogin";
const SESSION_INIT_PATH: &str = "/web/login/session/init";
const USER_PATH: &str = "/web/user";
const DEFAULT_CREDENTIAL_TTL: Duration = Duration::hours(1);
const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

/// Reqwest-backed WeRead QR transport.
#[derive(Clone)]
pub struct WereadQrHttpTransport {
    client: Client,
    base_url: Url,
    credential_ttl: Duration,
    attempt_cookies: Arc<Mutex<HashMap<Uuid, AttemptCookieJar>>>,
}

impl std::fmt::Debug for WereadQrHttpTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WereadQrHttpTransport")
            .field("client", &"<http client>")
            .field("base_url", &self.base_url)
            .field("credential_ttl", &self.credential_ttl)
            .finish()
    }
}

impl Default for WereadQrHttpTransport {
    fn default() -> Self {
        let client = Client::builder()
            .build()
            .expect("default WeRead QR HTTP client should be constructible");
        Self {
            client,
            base_url: Url::parse(WEREAD_BASE_URL).expect("constant WeRead URL is valid"),
            credential_ttl: DEFAULT_CREDENTIAL_TTL,
            attempt_cookies: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl WereadQrHttpTransport {
    /// Creates the transport with an explicit WeRead HTTPS endpoint.
    ///
    /// The host is intentionally restricted to the production WeRead origin;
    /// tests should inject an application-level transport instead of weakening
    /// this credential-bearing boundary for a local HTTP fixture.
    pub fn new(
        client: Client,
        base_url: Url,
        credential_ttl: Duration,
    ) -> Result<Self, QrLoginTransportError> {
        if credential_ttl <= Duration::zero()
            || base_url.scheme() != "https"
            || base_url.host_str() != Some("weread.qq.com")
        {
            return Err(QrLoginTransportError::InvalidResponse);
        }
        Ok(Self {
            client,
            base_url,
            credential_ttl,
            attempt_cookies: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    fn endpoint(&self, path: &str) -> Result<Url, QrLoginTransportError> {
        self.base_url
            .join(path)
            .map_err(|_| QrLoginTransportError::InvalidResponse)
    }

    fn request(&self, url: Url, body: &Value) -> reqwest::RequestBuilder {
        self.client
            .post(url)
            .json(body)
            .header(header::USER_AGENT, USER_AGENT)
            .header(header::ACCEPT, "application/json, text/plain, */*")
            .header(header::ACCEPT_LANGUAGE, "zh-CN,zh;q=0.9,en;q=0.8")
            .header(header::ORIGIN, WEREAD_BASE_URL)
            .header(header::REFERER, format!("{WEREAD_BASE_URL}/"))
    }

    fn request_get(&self, url: Url) -> reqwest::RequestBuilder {
        self.client
            .get(url)
            .header(header::USER_AGENT, USER_AGENT)
            .header(header::ACCEPT, "application/json, text/plain, */*")
            .header(header::ACCEPT_LANGUAGE, "zh-CN,zh;q=0.9,en;q=0.8")
            .header(header::ORIGIN, WEREAD_BASE_URL)
            .header(header::REFERER, format!("{WEREAD_BASE_URL}/"))
    }

    async fn request_with_cookies(
        &self,
        url: Url,
        body: &Value,
        challenge: &QrLoginChallenge,
    ) -> reqwest::RequestBuilder {
        let request = self.request(url, body);
        match self.cookie_header_for(challenge).await {
            Some(cookie_header) => request.header(header::COOKIE, cookie_header),
            None => request,
        }
    }

    async fn request_get_with_cookies(
        &self,
        url: Url,
        challenge: &QrLoginChallenge,
    ) -> reqwest::RequestBuilder {
        let request = self.request_get(url);
        match self.cookie_header_for(challenge).await {
            Some(cookie_header) => request.header(header::COOKIE, cookie_header),
            None => request,
        }
    }

    async fn cookie_header_for(&self, challenge: &QrLoginChallenge) -> Option<String> {
        self.attempt_cookies
            .lock()
            .await
            .get(&challenge.transport_key())
            .and_then(AttemptCookieJar::header)
    }

    async fn update_cookies_from_response(
        &self,
        challenge: &QrLoginChallenge,
        response: &Response,
    ) -> Option<String> {
        let mut cookies = self.attempt_cookies.lock().await;
        let jar = cookies
            .entry(challenge.transport_key())
            .or_insert_with(AttemptCookieJar::default);
        jar.update_from_response(response);
        jar.header()
    }

    async fn remove_cookie_jar(&self, challenge: &QrLoginChallenge) {
        self.attempt_cookies
            .lock()
            .await
            .remove(&challenge.transport_key());
    }
}

#[async_trait]
impl QrLoginTransport for WereadQrHttpTransport {
    async fn begin(&self) -> Result<QrLoginChallenge, QrLoginTransportError> {
        let url = self.endpoint(LOGIN_UID_PATH)?;
        let response = self
            .request(url, &serde_json::json!({}))
            .timeout(StdDuration::from_secs(20))
            .send()
            .await
            .map_err(|_| QrLoginTransportError::Unavailable)?;
        ensure_success(&response)?;
        let mut attempt_cookies = AttemptCookieJar::default();
        attempt_cookies.update_from_response(&response);
        let payload = response
            .json::<Value>()
            .await
            .map_err(|_| QrLoginTransportError::InvalidResponse)?;
        let uid = find_string(&payload, &["uid", "loginUid"])
            .filter(|uid| !uid.trim().is_empty())
            .ok_or(QrLoginTransportError::InvalidResponse)?;
        let challenge = QrLoginChallenge::new(uid)?;
        self.attempt_cookies
            .lock()
            .await
            .insert(challenge.transport_key(), attempt_cookies);
        Ok(challenge)
    }

    async fn poll(
        &self,
        challenge: &QrLoginChallenge,
    ) -> Result<QrLoginTransportPoll, QrLoginTransportError> {
        let url = self.endpoint(LOGIN_INFO_PATH)?;
        let request = self
            .request_with_cookies(url, &serde_json::json!({"uid": challenge.uid()}), challenge)
            .await;
        let response = request
            .timeout(StdDuration::from_secs(70))
            .send()
            .await
            .map_err(|_| QrLoginTransportError::Unavailable)?;
        ensure_success(&response)?;
        self.update_cookies_from_response(challenge, &response)
            .await;
        let payload = response
            .json::<Value>()
            .await
            .map_err(|_| QrLoginTransportError::InvalidResponse)?;
        match parse_login_info_state(&payload) {
            LoginInfoState::Pending(result) => {
                let terminal = matches!(
                    result,
                    QrLoginTransportPoll::Expired | QrLoginTransportPoll::RiskControlled
                );
                if terminal {
                    self.remove_cookie_jar(challenge).await;
                }
                Ok(result)
            }
            LoginInfoState::Confirmed(login_info) => {
                let weblogin_url = self.endpoint(WEB_LOGIN_PATH)?;
                let weblogin_response = self
                    .request_with_cookies(
                        weblogin_url,
                        &build_weblogin_payload(&login_info),
                        challenge,
                    )
                    .await
                    .timeout(StdDuration::from_secs(20))
                    .send()
                    .await
                    .map_err(|_| QrLoginTransportError::Unavailable)?;
                ensure_success(&weblogin_response)?;
                self.update_cookies_from_response(challenge, &weblogin_response)
                    .await;
                let weblogin_payload = weblogin_response
                    .json::<Value>()
                    .await
                    .map_err(|_| QrLoginTransportError::InvalidResponse)?;
                ensure_exchange_success(&weblogin_payload)?;

                let mut exchange_objects = response_objects(&payload);
                exchange_objects.extend(response_objects(&weblogin_payload));
                let cookie_header = self.cookie_header_for(challenge).await;
                let access_token =
                    find_string_from_objects(&exchange_objects, &["accessToken", "access_token"])
                        .or_else(|| {
                            cookie_header
                                .as_deref()
                                .and_then(|value| cookie_value(value, "wr_skey"))
                        })
                        .ok_or(QrLoginTransportError::InvalidResponse)?;
                let refresh_token =
                    find_string_from_objects(&exchange_objects, &["refreshToken", "refresh_token"])
                        .or_else(|| {
                            cookie_header
                                .as_deref()
                                .and_then(|value| cookie_value(value, "wr_rt"))
                        })
                        .ok_or(QrLoginTransportError::InvalidResponse)?;
                let vid =
                    find_string_from_objects(&exchange_objects, &["vid", "webLoginVid", "userVid"])
                        .or_else(|| {
                            cookie_header
                                .as_deref()
                                .and_then(|value| cookie_value(value, "wr_vid"))
                        })
                        .ok_or(QrLoginTransportError::InvalidResponse)?;

                let session_init_url = self.endpoint(SESSION_INIT_PATH)?;
                let session_init_response = self
                    .request_with_cookies(
                        session_init_url,
                        &serde_json::json!({
                            "vid": vid,
                            "pf": 0,
                            "skey": access_token,
                            "rt": refresh_token,
                        }),
                        challenge,
                    )
                    .await
                    .timeout(StdDuration::from_secs(20))
                    .send()
                    .await
                    .map_err(|_| QrLoginTransportError::Unavailable)?;
                ensure_success(&session_init_response)?;
                self.update_cookies_from_response(challenge, &session_init_response)
                    .await;
                let session_init_payload = session_init_response
                    .json::<Value>()
                    .await
                    .map_err(|_| QrLoginTransportError::InvalidResponse)?;
                ensure_exchange_success(&session_init_payload)?;

                let mut final_objects = exchange_objects;
                final_objects.extend(response_objects(&session_init_payload));

                // session/init establishes only the core HttpOnly cookies.
                // WeRead fills the user identity and remaining browser cookie
                // values when the authenticated user endpoint is requested.
                // Keep this request inside the same attempt jar so the
                // resulting credentials can be used by later source syncs.
                let mut user_url = self.endpoint(USER_PATH)?;
                user_url.query_pairs_mut().append_pair("userVid", &vid);
                let user_response = self
                    .request_get_with_cookies(user_url, challenge)
                    .await
                    .timeout(StdDuration::from_secs(20))
                    .send()
                    .await
                    .map_err(|_| QrLoginTransportError::Unavailable)?;
                ensure_success(&user_response)?;
                self.update_cookies_from_response(challenge, &user_response)
                    .await;
                let user_payload = user_response
                    .json::<Value>()
                    .await
                    .map_err(|_| QrLoginTransportError::InvalidResponse)?;
                ensure_exchange_success(&user_payload)?;
                final_objects.extend(response_objects(&user_payload));
                let cookie_header = self.cookie_header_for(challenge).await;
                let session = parse_authenticated_session_objects(
                    &final_objects,
                    cookie_header.as_deref(),
                    Utc::now(),
                    self.credential_ttl,
                )?;
                self.remove_cookie_jar(challenge).await;
                Ok(QrLoginTransportPoll::Authenticated(session))
            }
        }
    }

    async fn cancel(&self, challenge: &QrLoginChallenge) -> Result<(), QrLoginTransportError> {
        // The reference implementation cancels by stopping local polling and
        // deleting the QR artifact; WeRead does not expose a required close
        // endpoint. The application manager consumes the local attempt.
        self.remove_cookie_jar(challenge).await;
        Ok(())
    }
}

fn ensure_success(response: &Response) -> Result<(), QrLoginTransportError> {
    if response.status().is_success() {
        Ok(())
    } else if response.status().is_server_error() || response.status().as_u16() == 429 {
        Err(QrLoginTransportError::Unavailable)
    } else {
        Err(QrLoginTransportError::InvalidResponse)
    }
}

fn ensure_exchange_success(payload: &Value) -> Result<(), QrLoginTransportError> {
    let objects = response_objects(payload);
    let has_failure = objects.iter().any(|object| {
        ["succeed", "success", "succ"]
            .into_iter()
            .filter_map(|key| object.get(key))
            .any(|value| value_is_boolish(value) == Some(false))
    });
    if !has_failure {
        return Ok(());
    }

    let error_label = find_value(
        &objects,
        &[
            "logicCode",
            "errCode",
            "errorCode",
            "code",
            "status",
            "errMsg",
        ],
    )
    .map(value_label)
    .unwrap_or_default()
    .to_ascii_uppercase();
    if ["OTP", "RISK", "SAFE", "CONTROL"]
        .into_iter()
        .any(|marker| error_label.contains(marker))
    {
        Err(QrLoginTransportError::RiskControlled)
    } else {
        Err(QrLoginTransportError::InvalidResponse)
    }
}

#[derive(Clone, Default)]
struct AttemptCookieJar {
    values: Vec<(String, String)>,
}

impl AttemptCookieJar {
    fn update_from_response(&mut self, response: &Response) {
        for value in response.headers().get_all(header::SET_COOKIE).iter() {
            if let Ok(value) = value.to_str() {
                self.update_pair(value.split(';').next().unwrap_or_default());
            }
        }
    }

    fn update_from_header(&mut self, cookie_header: &str) {
        for pair in cookie_header.split(';') {
            self.update_pair(pair);
        }
    }

    fn update_pair(&mut self, pair: &str) {
        let Some((name, value)) = pair.trim().split_once('=') else {
            return;
        };
        let name = name.trim();
        let value = value.trim();
        if name.is_empty()
            || name.chars().any(char::is_control)
            || value.chars().any(char::is_control)
        {
            return;
        }
        let value = (name.to_owned(), value.to_owned());
        if let Some(existing) = self
            .values
            .iter_mut()
            .find(|(existing_name, _)| existing_name.eq_ignore_ascii_case(name))
        {
            *existing = value;
        } else {
            self.values.push(value);
        }
    }

    fn header(&self) -> Option<String> {
        (!self.values.is_empty()).then(|| {
            self.values
                .iter()
                .map(|(name, value)| format!("{name}={value}"))
                .collect::<Vec<_>>()
                .join("; ")
        })
    }
}

#[cfg(test)]
fn parse_login_info(
    payload: &Value,
    response_cookie_header: Option<&str>,
    now: DateTime<Utc>,
    credential_ttl: Duration,
) -> Result<QrLoginTransportPoll, QrLoginTransportError> {
    match parse_login_info_state(payload) {
        LoginInfoState::Pending(result) => Ok(result),
        LoginInfoState::Confirmed(_) => {
            let objects = response_objects(payload);
            parse_authenticated_session_objects(
                &objects,
                response_cookie_header,
                now,
                credential_ttl,
            )
            .map(QrLoginTransportPoll::Authenticated)
        }
    }
}

enum LoginInfoState {
    Pending(QrLoginTransportPoll),
    Confirmed(Value),
}

fn parse_login_info_state(payload: &Value) -> LoginInfoState {
    let objects = response_objects(payload);
    if let Some(object) = objects
        .iter()
        .find(|object| login_success_flag(object) == Some(true))
    {
        return LoginInfoState::Confirmed(Value::Object((*object).clone()));
    }

    let logic_code = objects
        .iter()
        .flat_map(|object| {
            ["logicCode", "code", "status"]
                .into_iter()
                .map(move |key| (*object, key))
        })
        .filter_map(|(object, key)| object.get(key).map(value_label))
        .find(|value| !value.is_empty() && value != "0")
        .unwrap_or_default()
        .to_ascii_uppercase();
    // WeRead uses LOGIN_TIMEOUT when the long-poll request reaches its normal
    // wait boundary. The reference flow keeps polling until the local
    // five-minute attempt deadline; only explicit expiry/timeout states
    // consume the attempt.
    LoginInfoState::Pending(match logic_code.as_str() {
        "1" | "SCANNED" | "CONFIRMING" | "WAIT_CONFIRM" => QrLoginTransportPoll::Scanned,
        "LOGIN_TIMEOUT" => QrLoginTransportPoll::Waiting,
        "EXPIRED" | "TIMEOUT" => QrLoginTransportPoll::Expired,
        "NEED_OTP" | "RISK_CONTROLLED" | "RISK" => QrLoginTransportPoll::RiskControlled,
        _ => QrLoginTransportPoll::Waiting,
    })
}

fn build_weblogin_payload(login_info: &Value) -> Value {
    let mut payload = login_info.as_object().cloned().unwrap_or_default();
    for key in ["redirect_uri", "expireMode", "pf"] {
        payload.remove(key);
    }
    payload.insert("fp".to_owned(), Value::String(String::new()));
    Value::Object(payload)
}

fn login_success_flag(object: &Map<String, Value>) -> Option<bool> {
    ["succeed", "success", "succ"]
        .into_iter()
        .find_map(|key| object.get(key).and_then(value_is_boolish))
}

fn value_is_boolish(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(value) => Some(*value),
        Value::Number(value) => value.as_i64().map(|value| value != 0),
        Value::String(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "ok" | "success" => Some(true),
            "0" | "false" | "failed" | "failure" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

fn parse_authenticated_session_objects(
    objects: &[&Map<String, Value>],
    response_cookie_header: Option<&str>,
    now: DateTime<Utc>,
    credential_ttl: Duration,
) -> Result<QrAuthenticatedSession, QrLoginTransportError> {
    let json_cookie_header = find_cookie_header(objects);
    let mut cookie_header =
        merge_cookie_headers(response_cookie_header, json_cookie_header.as_deref())
            .unwrap_or_default();
    let access_token = find_string_from_objects(objects, &["accessToken", "access_token"])
        .or_else(|| cookie_value(&cookie_header, "wr_skey"))
        .ok_or(QrLoginTransportError::InvalidResponse)?;
    let refresh_token = find_string_from_objects(objects, &["refreshToken", "refresh_token"])
        .or_else(|| cookie_value(&cookie_header, "wr_rt"))
        .ok_or(QrLoginTransportError::InvalidResponse)?;
    if cookie_value(&cookie_header, "wr_vid").is_none() {
        let vid = find_string_from_objects(objects, &["vid", "webLoginVid", "userVid"])
            .ok_or(QrLoginTransportError::InvalidResponse)?;
        append_cookie(&mut cookie_header, "wr_vid", &vid)?;
    }
    if cookie_value(&cookie_header, "wr_skey").is_none() {
        append_cookie(&mut cookie_header, "wr_skey", &access_token)?;
    }
    if cookie_value(&cookie_header, "wr_rt").is_none() {
        append_cookie(&mut cookie_header, "wr_rt", &refresh_token)?;
    }
    let display_name = find_string_from_objects(objects, &["name", "displayName"]);
    let access_expires_at = find_expiry(objects).unwrap_or_else(|| now + credential_ttl);
    QrAuthenticatedSession::new(
        access_token,
        refresh_token,
        cookie_header,
        access_expires_at,
        display_name,
    )
}

fn response_objects(payload: &Value) -> Vec<&Map<String, Value>> {
    let mut objects = Vec::new();
    if let Some(object) = payload.as_object() {
        objects.push(object);
        if let Some(data) = object.get("data").and_then(Value::as_object) {
            objects.push(data);
            if let Some(inner) = data.get("data").and_then(Value::as_object) {
                objects.push(inner);
            }
        }
    }
    objects
}

fn find_string(payload: &Value, keys: &[&str]) -> Option<String> {
    find_string_from_objects(&response_objects(payload), keys)
}

fn find_string_from_objects(objects: &[&Map<String, Value>], keys: &[&str]) -> Option<String> {
    objects.iter().find_map(|object| {
        keys.iter().find_map(|key| {
            object
                .get(*key)
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
        })
    })
}

fn find_value<'a>(objects: &[&'a Map<String, Value>], keys: &[&str]) -> Option<&'a Value> {
    objects
        .iter()
        .find_map(|object| keys.iter().find_map(|key| object.get(*key)))
}

fn value_label(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        _ => String::new(),
    }
}

fn find_cookie_header(objects: &[&Map<String, Value>]) -> Option<String> {
    objects.iter().find_map(|object| {
        object
            .get("cookie")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .or_else(|| {
                object.get("cookies").and_then(|cookies| {
                    cookies
                        .as_str()
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(ToOwned::to_owned)
                        .or_else(|| {
                            cookies
                                .as_object()
                                .map(|cookies| {
                                    cookies
                                        .iter()
                                        .filter_map(|(name, value)| {
                                            value.as_str().map(|value| format!("{name}={value}"))
                                        })
                                        .collect::<Vec<_>>()
                                        .join("; ")
                                })
                                .filter(|value| !value.is_empty())
                        })
                })
            })
    })
}

fn merge_cookie_headers(primary: Option<&str>, secondary: Option<&str>) -> Option<String> {
    let mut jar = AttemptCookieJar::default();
    for cookie_header in [primary, secondary].into_iter().flatten() {
        jar.update_from_header(cookie_header);
    }
    jar.header()
}

fn append_cookie(
    header: &mut String,
    name: &str,
    value: &str,
) -> Result<(), QrLoginTransportError> {
    if value.is_empty() || value.chars().any(char::is_control) || value.contains(';') {
        return Err(QrLoginTransportError::InvalidResponse);
    }
    if !header.is_empty() {
        header.push_str("; ");
    }
    header.push_str(name);
    header.push('=');
    header.push_str(value);
    Ok(())
}

fn cookie_value(header: &str, name: &str) -> Option<String> {
    header.split(';').find_map(|part| {
        let (key, value) = part.trim().split_once('=')?;
        (key.trim() == name && !value.trim().is_empty()).then(|| value.trim().to_owned())
    })
}

fn find_expiry(objects: &[&Map<String, Value>]) -> Option<DateTime<Utc>> {
    let value = find_value(objects, &["expiresAt", "accessExpiresAt", "expires_at"])?;
    match value {
        Value::Number(number) => {
            let value = number.as_i64()?;
            if value > 10_000_000_000 {
                DateTime::<Utc>::from_timestamp_millis(value)
            } else {
                DateTime::<Utc>::from_timestamp(value, 0)
            }
        }
        Value::String(value) => DateTime::parse_from_rfc3339(value)
            .ok()
            .map(|value| value.with_timezone(&Utc)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;

    use axum::{
        body::Bytes,
        extract::State,
        http::{HeaderMap, HeaderValue, Method, StatusCode, Uri},
        response::{IntoResponse, Response as AxumResponse},
        Router,
    };
    use tokio::net::TcpListener;

    use super::*;

    fn now() -> DateTime<Utc> {
        "2026-09-04T00:00:00Z".parse().unwrap()
    }

    #[derive(Debug)]
    struct FixtureRequest {
        method: Method,
        path: String,
        query: Option<String>,
        cookie: Option<String>,
        body: Value,
    }

    #[derive(Clone)]
    struct FixtureState {
        requests: Arc<StdMutex<Vec<FixtureRequest>>>,
    }

    async fn qr_http_fixture(
        State(state): State<FixtureState>,
        method: Method,
        uri: Uri,
        headers: HeaderMap,
        body: Bytes,
    ) -> AxumResponse {
        let body = serde_json::from_slice(&body).unwrap_or(Value::Null);
        state
            .requests
            .lock()
            .expect("fixture request state should not be poisoned")
            .push(FixtureRequest {
                method: method.clone(),
                path: uri.path().to_owned(),
                query: uri.query().map(ToOwned::to_owned),
                cookie: headers
                    .get(axum::http::header::COOKIE)
                    .and_then(|value| value.to_str().ok())
                    .map(ToOwned::to_owned),
                body,
            });

        match uri.path() {
            LOGIN_UID_PATH if method == Method::POST => qr_json_response(
                serde_json::json!({"uid": "fixture-uid"}),
                &["wr_trace=from-getuid; Path=/"],
            ),
            LOGIN_INFO_PATH if method == Method::POST => qr_json_response(
                serde_json::json!({
                    "succeed": true,
                    "vid": "fixture-vid",
                    "accessToken": "fixture-access",
                    "refreshToken": "fixture-refresh",
                    "redirect_uri": "must-not-be-forwarded",
                    "expireMode": 1,
                    "pf": 2
                }),
                &["wr_info=from-getinfo; Path=/"],
            ),
            WEB_LOGIN_PATH if method == Method::POST => qr_json_response(
                serde_json::json!({"succeed": true}),
                &["wr_login=from-weblogin; Path=/"],
            ),
            SESSION_INIT_PATH if method == Method::POST => qr_json_response(
                serde_json::json!({"succeed": true}),
                &[
                    "wr_vid=fixture-vid; Path=/",
                    "wr_skey=fixture-access; Path=/",
                    "wr_rt=fixture-refresh; Path=/",
                ],
            ),
            USER_PATH if method == Method::GET => qr_json_response(
                serde_json::json!({"vid": "fixture-vid", "name": "Fixture User"}),
                &["wr_name=Fixture%20User; Path=/", "wr_ql=1; Path=/"],
            ),
            _ => StatusCode::NOT_FOUND.into_response(),
        }
    }

    fn qr_json_response(payload: Value, cookies: &[&str]) -> AxumResponse {
        let mut response = axum::Json(payload).into_response();
        for cookie in cookies {
            response.headers_mut().append(
                axum::http::header::SET_COOKIE,
                HeaderValue::try_from(*cookie).expect("fixture cookie should be valid"),
            );
        }
        response
    }

    #[tokio::test]
    async fn http_transport_performs_the_documented_qr_exchange() {
        let state = FixtureState {
            requests: Arc::new(StdMutex::new(Vec::new())),
        };
        let app = Router::new()
            .fallback(qr_http_fixture)
            .with_state(state.clone());
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("fixture listener should bind");
        let address = listener
            .local_addr()
            .expect("fixture listener should expose an address");
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("fixture server should stay healthy");
        });
        let transport = WereadQrHttpTransport {
            client: Client::new(),
            base_url: Url::parse(&format!("http://{address}/"))
                .expect("fixture base URL should parse"),
            credential_ttl: Duration::hours(1),
            attempt_cookies: Arc::new(Mutex::new(HashMap::new())),
        };

        let challenge = transport
            .begin()
            .await
            .expect("UID request should complete");
        assert!(challenge
            .confirmation_url()
            .query_pairs()
            .any(|(key, value)| key == "pf" && value == "2"));
        assert!(challenge
            .confirmation_url()
            .query_pairs()
            .any(|(key, value)| key == "uid" && value == "fixture-uid"));

        let result = transport
            .poll(&challenge)
            .await
            .expect("confirmed QR exchange should complete");
        let QrLoginTransportPoll::Authenticated(session) = result else {
            panic!("fixture should return authenticated credentials")
        };
        assert_eq!(session.access_token(), "fixture-access");
        assert_eq!(session.refresh_token(), "fixture-refresh");
        assert_eq!(session.display_name(), Some("Fixture User"));
        for cookie in [
            "wr_trace=from-getuid",
            "wr_info=from-getinfo",
            "wr_login=from-weblogin",
            "wr_vid=fixture-vid",
            "wr_skey=fixture-access",
            "wr_rt=fixture-refresh",
            "wr_name=Fixture%20User",
            "wr_ql=1",
        ] {
            assert!(
                session.cookie_header().contains(cookie),
                "session should retain {cookie}"
            );
        }

        let requests = state
            .requests
            .lock()
            .expect("fixture request state should not be poisoned");
        assert_eq!(requests.len(), 5);
        assert!(requests[..4]
            .iter()
            .all(|request| request.method == Method::POST));
        assert_eq!(requests[0].path, LOGIN_UID_PATH);
        assert_eq!(requests[0].body, serde_json::json!({}));
        assert_eq!(requests[1].path, LOGIN_INFO_PATH);
        assert_eq!(requests[1].body, serde_json::json!({"uid": "fixture-uid"}));
        assert!(requests[1]
            .cookie
            .as_deref()
            .is_some_and(|value| value.contains("wr_trace=from-getuid")));
        assert_eq!(requests[2].path, WEB_LOGIN_PATH);
        assert_eq!(requests[2].body["fp"], "");
        assert!(requests[2].body.get("redirect_uri").is_none());
        assert!(requests[2].body.get("expireMode").is_none());
        assert!(requests[2].body.get("pf").is_none());
        assert!(requests[2]
            .cookie
            .as_deref()
            .is_some_and(|value| value.contains("wr_info=from-getinfo")));
        assert_eq!(requests[3].path, SESSION_INIT_PATH);
        assert_eq!(
            requests[3].body,
            serde_json::json!({
                "vid": "fixture-vid",
                "pf": 0,
                "skey": "fixture-access",
                "rt": "fixture-refresh"
            })
        );
        assert!(requests[3]
            .cookie
            .as_deref()
            .is_some_and(|value| value.contains("wr_login=from-weblogin")));
        assert_eq!(requests[4].method, Method::GET);
        assert_eq!(requests[4].path, USER_PATH);
        assert_eq!(requests[4].query.as_deref(), Some("userVid=fixture-vid"));
        assert!(requests[4]
            .cookie
            .as_deref()
            .is_some_and(|value| value.contains("wr_rt=fixture-refresh")));
        drop(requests);
        server.abort();
    }

    #[test]
    fn parses_uid_from_top_level_and_nested_responses() {
        for payload in [
            serde_json::json!({"uid":"top-level"}),
            serde_json::json!({"data":{"uid":"nested"}}),
        ] {
            assert!(find_string(&payload, &["uid"]).is_some());
        }
    }

    #[test]
    fn maps_waiting_scanned_and_expired_logic_codes() {
        for (payload, expected) in [
            (
                serde_json::json!({"succeed":false}),
                QrLoginStateForTest::Waiting,
            ),
            (
                serde_json::json!({"succeed":false,"logicCode":1}),
                QrLoginStateForTest::Scanned,
            ),
            (
                serde_json::json!({"succeed":false,"logicCode":"LOGIN_TIMEOUT"}),
                QrLoginStateForTest::Waiting,
            ),
            (
                serde_json::json!({"succeed":false,"logicCode":"EXPIRED"}),
                QrLoginStateForTest::Expired,
            ),
        ] {
            let result = parse_login_info(&payload, None, now(), Duration::hours(1)).unwrap();
            assert_eq!(QrLoginStateForTest::from(result), expected);
        }
    }

    #[test]
    fn maps_need_otp_to_risk_control_without_exposing_payload() {
        let result = parse_login_info(
            &serde_json::json!({"succeed":false,"logicCode":"NEED_OTP"}),
            None,
            now(),
            Duration::hours(1),
        )
        .unwrap();
        assert_eq!(
            QrLoginStateForTest::from(result),
            QrLoginStateForTest::RiskControlled
        );
    }

    #[test]
    fn parses_authenticated_tokens_from_set_cookie_and_nested_payload() {
        let result = parse_login_info(
            &serde_json::json!({"succeed":true,"data":{"vid":"vid","name":"A"}}),
            Some("wr_vid=vid; wr_skey=access; wr_rt=refresh"),
            now(),
            Duration::hours(1),
        )
        .unwrap();
        let QrLoginTransportPoll::Authenticated(session) = result else {
            panic!("successful response should produce a session")
        };
        assert_eq!(session.access_token(), "access");
        assert_eq!(session.refresh_token(), "refresh");
        assert_eq!(session.display_name(), Some("A"));
        assert_eq!(session.access_expires_at(), now() + Duration::hours(1));
    }

    #[test]
    fn accepts_success_nested_under_a_false_response_envelope() {
        let result = parse_login_info(
            &serde_json::json!({
                "succeed": false,
                "data": {
                    "succeed": true,
                    "vid": "nested-identity",
                    "accessToken": "nested-access",
                    "refreshToken": "nested-refresh"
                }
            }),
            Some("tracking=value"),
            now(),
            Duration::hours(1),
        )
        .unwrap();
        let QrLoginTransportPoll::Authenticated(session) = result else {
            panic!("nested success should produce a session")
        };
        assert_eq!(session.access_token(), "nested-access");
        assert_eq!(session.refresh_token(), "nested-refresh");
        assert!(session.cookie_header().contains("wr_vid=nested-identity"));
    }

    #[test]
    fn backfills_identity_and_auth_cookies_from_a_confirmed_json_response() {
        let result = parse_login_info(
            &serde_json::json!({
                "succeed": true,
                "vid": "json-identity",
                "accessToken": "json-access",
                "refreshToken": "json-refresh"
            }),
            Some("tracking=value"),
            now(),
            Duration::hours(1),
        )
        .unwrap();
        let QrLoginTransportPoll::Authenticated(session) = result else {
            panic!("successful response should produce a session")
        };
        assert!(session.cookie_header().contains("tracking=value"));
        assert!(session.cookie_header().contains("wr_vid=json-identity"));
        assert!(session.cookie_header().contains("wr_skey=json-access"));
        assert!(session.cookie_header().contains("wr_rt=json-refresh"));
    }

    #[test]
    fn rejects_success_without_cookie_or_identity() {
        assert!(matches!(
            parse_login_info(
                &serde_json::json!({"succeed":true,"accessToken":"access","refreshToken":"refresh"}),
                None,
                now(),
                Duration::hours(1),
            ),
            Err(QrLoginTransportError::InvalidResponse)
        ));
    }

    #[test]
    fn rejects_json_identity_that_cannot_be_added_to_the_authenticated_cookie() {
        assert!(matches!(
            parse_login_info(
                &serde_json::json!({
                    "succeed":true,
                    "vid":"json;identity",
                    "accessToken":"access",
                    "refreshToken":"refresh"
                }),
                Some("wr_skey=access; wr_rt=refresh"),
                now(),
                Duration::hours(1),
            ),
            Err(QrLoginTransportError::InvalidResponse)
        ));
    }

    #[test]
    fn keeps_cookie_state_for_one_attempt_separate_from_another_attempt() {
        let mut first = AttemptCookieJar::default();
        first.update_from_header("session=first; wr_skey=first-access");
        let mut second = AttemptCookieJar::default();
        second.update_from_header("session=second; wr_skey=second-access");

        assert_eq!(
            first.header().as_deref(),
            Some("session=first; wr_skey=first-access")
        );
        assert_eq!(
            second.header().as_deref(),
            Some("session=second; wr_skey=second-access")
        );
    }

    #[tokio::test]
    async fn cancelling_an_attempt_discards_its_cookie_state() {
        let transport = WereadQrHttpTransport::new(
            Client::new(),
            Url::parse("https://weread.qq.com").unwrap(),
            Duration::hours(1),
        )
        .unwrap();
        let challenge = QrLoginChallenge::new("uid-for-test").unwrap();
        transport
            .attempt_cookies
            .lock()
            .await
            .insert(challenge.transport_key(), AttemptCookieJar::default());

        QrLoginTransport::cancel(&transport, &challenge)
            .await
            .unwrap();

        assert!(!transport
            .attempt_cookies
            .lock()
            .await
            .contains_key(&challenge.transport_key()));
    }

    #[test]
    fn rejects_non_weread_transport_endpoints() {
        let client = Client::new();
        assert!(matches!(
            WereadQrHttpTransport::new(
                client.clone(),
                Url::parse("http://weread.qq.com").unwrap(),
                Duration::hours(1),
            ),
            Err(QrLoginTransportError::InvalidResponse)
        ));
        assert!(matches!(
            WereadQrHttpTransport::new(
                client,
                Url::parse("https://example.test").unwrap(),
                Duration::hours(1),
            ),
            Err(QrLoginTransportError::InvalidResponse)
        ));
    }

    #[derive(Debug, PartialEq, Eq)]
    enum QrLoginStateForTest {
        Waiting,
        Scanned,
        Expired,
        RiskControlled,
    }

    impl From<QrLoginTransportPoll> for QrLoginStateForTest {
        fn from(value: QrLoginTransportPoll) -> Self {
            match value {
                QrLoginTransportPoll::Waiting => Self::Waiting,
                QrLoginTransportPoll::Scanned => Self::Scanned,
                QrLoginTransportPoll::Expired => Self::Expired,
                QrLoginTransportPoll::RiskControlled => Self::RiskControlled,
                QrLoginTransportPoll::Authenticated(_) => panic!("not a state fixture"),
            }
        }
    }
}
