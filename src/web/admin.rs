//! Authenticated single-administrator API and panel.
//!
//! This module is intentionally a thin HTTP adapter. Source mutations and
//! feed-token lifecycle stay in application services; synchronization history
//! stays in its repository read port. All state-changing routes require both
//! the signed admin session and its CSRF token.

use std::{net::SocketAddr, sync::Arc};

use axum::{
    extract::{ConnectInfo, Path, Query, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Redirect, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Duration, Utc};
use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use crate::{
    acquisition::identity::{ArticleIdentityResolver, IdentityError, UrlArticleIdentityResolver},
    application::{
        auth_service::{
            AuthService, AuthServiceError, CredentialProvision, ManualCredentialRefresher,
            RingCredentialCipher,
        },
        feed_token_service::{FeedTokenService, FeedTokenServiceError},
        source_service::{SourceService, SourceServiceError},
    },
    domain::{
        credentials::WeReadAccountId,
        source::{NewSource, SchedulingGate, Source, SourceId, SourcePatch},
        sync::SyncRun,
    },
    persistence::repositories::{
        account_lease_repository::PostgresAccountLeaseRepository,
        credential_repository::PostgresCredentialRepository,
        feed_token_repository::{FeedTokenRepositoryError, PostgresFeedTokenRepository},
        source_repository::{PostgresSourceRepository, SourceRepositoryError},
        sync_run_repository::{
            PostgresSyncRunRepository, SyncRunRepository, SyncRunRepositoryError,
        },
    },
    persistence::unit_of_work::UnitOfWorkFactory,
    web::{
        auth::{AdminAuthenticator, AdminSession, AuthError},
        ui,
    },
};

type AdminSourceService = SourceService<PostgresSourceRepository, UnitOfWorkFactory>;
type AdminTokenService = FeedTokenService<PostgresFeedTokenRepository>;
type AdminAuthService = AuthService<
    PostgresCredentialRepository,
    PostgresAccountLeaseRepository,
    ManualCredentialRefresher,
    RingCredentialCipher,
>;

/// Builds the authenticated administration API and HTML panel routes.
pub fn admin_router(
    auth: AdminAuthenticator,
    sources: AdminSourceService,
    feed_tokens: AdminTokenService,
    sync_runs: PostgresSyncRunRepository,
    weread_auth: AdminAuthService,
) -> Router {
    admin_router_with_identity_resolver(
        auth,
        sources,
        feed_tokens,
        sync_runs,
        weread_auth,
        Arc::new(UrlArticleIdentityResolver),
    )
}

/// Builds the administration routes with a configured article identity
/// resolver. Runtime deployments should provide the browser-backed resolver;
/// the compatibility constructor above remains useful for long URLs and tests.
pub fn admin_router_with_identity_resolver(
    auth: AdminAuthenticator,
    sources: AdminSourceService,
    feed_tokens: AdminTokenService,
    sync_runs: PostgresSyncRunRepository,
    weread_auth: AdminAuthService,
    identity_resolver: Arc<dyn ArticleIdentityResolver>,
) -> Router {
    let state = Arc::new(AdminApiState {
        auth,
        sources,
        feed_tokens,
        sync_runs,
        weread_auth,
        identity_resolver,
    });
    Router::new()
        .route("/", get(root_redirect))
        .route("/admin/login", get(login_page))
        .route("/admin", get(admin_page))
        .route("/admin/", get(admin_page))
        .route("/admin/sources/{source_id}", get(source_page))
        .route("/admin/weread/accounts", get(weread_accounts_page))
        .route(
            "/admin/weread/accounts/{account_id}",
            get(weread_account_page),
        )
        .route("/api/admin/login", post(login))
        .route("/api/admin/logout", post(logout))
        .route(
            "/api/admin/weread/accounts",
            get(list_weread_accounts).post(provision_weread_account),
        )
        .route(
            "/api/admin/weread/accounts/{account_id}",
            get(get_weread_account)
                .put(replace_weread_account)
                .delete(delete_weread_account),
        )
        .route(
            "/api/admin/weread/accounts/{account_id}/enabled",
            post(set_weread_account_enabled),
        )
        .route("/api/admin/sources", get(list_sources).post(create_source))
        .route(
            "/api/admin/sources/{source_id}",
            get(get_source).put(update_source).delete(delete_source),
        )
        .route("/api/admin/sources/{source_id}/enabled", post(set_enabled))
        .route("/api/admin/sources/{source_id}/gate", post(set_gate))
        .route(
            "/api/admin/sources/{source_id}/feed-token",
            post(issue_feed_token),
        )
        .route(
            "/api/admin/sources/{source_id}/sync-runs",
            get(list_sync_runs),
        )
        .with_state(state)
}

struct AdminApiState {
    auth: AdminAuthenticator,
    sources: AdminSourceService,
    feed_tokens: AdminTokenService,
    sync_runs: PostgresSyncRunRepository,
    weread_auth: AdminAuthService,
    identity_resolver: Arc<dyn ArticleIdentityResolver>,
}

#[derive(Debug, Deserialize)]
struct LoginRequest {
    username: String,
    password: String,
}

#[derive(Debug, Serialize)]
struct LoginResponse {
    username: String,
    csrf_token: String,
    expires_at: String,
}

/// Manual, non-interactive credential enrollment request.
///
/// This type intentionally does not implement `Debug`: access and refresh
/// tokens must not be included in accidental request logging.
#[derive(Deserialize)]
struct ProvisionWeReadAccountRequest {
    account_id: Option<Uuid>,
    display_name: Option<String>,
    cookie_header: String,
    access_expires_at: String,
}

#[derive(Debug, Serialize)]
struct WeReadAccountResponse {
    account_id: Uuid,
    display_name: String,
    credential_version: i64,
    access_expires_at: String,
    disabled: bool,
}

#[derive(Debug, Serialize)]
struct WeReadAccountListResponse {
    account_id: Uuid,
    display_name: String,
    status: &'static str,
}

#[derive(Debug, Deserialize)]
struct SetEnabledRequest {
    enabled: bool,
}

async fn root_redirect() -> Redirect {
    Redirect::temporary("/admin/")
}

impl From<crate::domain::credentials::WeReadAccount> for WeReadAccountResponse {
    fn from(account: crate::domain::credentials::WeReadAccount) -> Self {
        Self {
            account_id: account.account_id().as_uuid(),
            display_name: account.display_name().to_owned(),
            credential_version: account.credential_version(),
            access_expires_at: account.access_expires_at().to_rfc3339(),
            disabled: account.disabled(),
        }
    }
}

async fn login(
    State(state): State<Arc<AdminApiState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Json(request): Json<LoginRequest>,
) -> Response {
    let client_key = client_key(peer);
    match state.auth.login(
        &request.username,
        &request.password,
        &client_key,
        Utc::now(),
    ) {
        Ok((session, cookie)) => {
            let mut response = Json(LoginResponse {
                username: session.username().to_owned(),
                csrf_token: session.csrf_token().to_owned(),
                expires_at: session.expires_at().to_rfc3339(),
            })
            .into_response();
            response.headers_mut().insert(
                header::SET_COOKIE,
                HeaderValue::try_from(AdminAuthenticator::session_cookie(&cookie))
                    .expect("session cookie should be a valid header"),
            );
            response
        }
        Err(error) => auth_error_response(error),
    }
}

async fn logout(State(state): State<Arc<AdminApiState>>, headers: HeaderMap) -> Response {
    let session = match authenticate(&state.auth, &headers) {
        Ok(session) => session,
        Err(error) => return auth_error_response(error),
    };
    if let Err(error) = csrf(&state.auth, &session, &headers) {
        return auth_error_response(error);
    }
    let mut response = Json(json!({ "status": "ok" })).into_response();
    response.headers_mut().insert(
        header::SET_COOKIE,
        HeaderValue::from_static(AdminAuthenticator::clear_cookie()),
    );
    response
}

async fn provision_weread_account(
    State(state): State<Arc<AdminApiState>>,
    headers: HeaderMap,
    Json(request): Json<ProvisionWeReadAccountRequest>,
) -> Response {
    let _session = match protected_mutation(&state, &headers) {
        Ok(session) => session,
        Err(response) => return *response,
    };
    let account_id = request
        .account_id
        .map(WeReadAccountId::from_uuid)
        .unwrap_or_else(|| WeReadAccountId::from_uuid(Uuid::new_v4()));
    let credentials = match credentials_from_request(&request) {
        Ok(credentials) => credentials,
        Err(response) => return *response,
    };
    let display_name = match display_name_from_request(&request) {
        Ok(display_name) => display_name,
        Err(response) => return *response,
    };
    match state
        .weread_auth
        .provision(CredentialProvision {
            account_id,
            display_name,
            credentials,
        })
        .await
    {
        Ok(account) => (
            StatusCode::CREATED,
            Json(WeReadAccountResponse::from(account)),
        )
            .into_response(),
        Err(error) => auth_service_error_response(error),
    }
}

async fn replace_weread_account(
    State(state): State<Arc<AdminApiState>>,
    Path(account_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<ProvisionWeReadAccountRequest>,
) -> Response {
    let session = match protected_mutation(&state, &headers) {
        Ok(session) => session,
        Err(response) => return *response,
    };
    let account_id = match Uuid::parse_str(&account_id) {
        Ok(value) if !value.is_nil() => WeReadAccountId::from_uuid(value),
        _ => return validation_error("account_id must be a non-nil UUID"),
    };
    if request
        .account_id
        .is_some_and(|value| value != account_id.as_uuid())
    {
        return validation_error("account_id in the request must match the URL");
    }
    let credentials = match credentials_from_request(&request) {
        Ok(credentials) => credentials,
        Err(response) => return *response,
    };
    let display_name = match display_name_from_request(&request) {
        Ok(display_name) => display_name,
        Err(response) => return *response,
    };
    match state
        .weread_auth
        .replace(
            CredentialProvision {
                account_id,
                display_name,
                credentials,
            },
            &format!("admin:{}", session.username()),
        )
        .await
    {
        Ok(account) => Json(WeReadAccountResponse::from(account)).into_response(),
        Err(error) => auth_service_error_response(error),
    }
}

async fn list_weread_accounts(
    State(state): State<Arc<AdminApiState>>,
    headers: HeaderMap,
) -> Response {
    if let Err(error) = authenticate(&state.auth, &headers) {
        return auth_error_response(error);
    }
    let now = Utc::now();
    match state.weread_auth.list_accounts().await {
        Ok(accounts) => Json(
            accounts
                .into_iter()
                .map(|account| WeReadAccountListResponse {
                    account_id: account.account_id().as_uuid(),
                    display_name: account.display_name().to_owned(),
                    status: weread_account_status(&account, now),
                })
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(error) => auth_service_error_response(error),
    }
}

fn weread_account_status(
    account: &crate::domain::credentials::WeReadAccount,
    now: DateTime<Utc>,
) -> &'static str {
    if account.disabled() {
        "disabled"
    } else if account.access_expires_at() <= now {
        "expired"
    } else {
        "active"
    }
}

fn credentials_from_request(
    request: &ProvisionWeReadAccountRequest,
) -> Result<crate::domain::credentials::WeReadCredentials, Box<Response>> {
    let cookie_header = request.cookie_header.trim();
    let access_expires_at = match DateTime::parse_from_rfc3339(&request.access_expires_at) {
        Ok(value) => value.with_timezone(&Utc),
        Err(_) => {
            return Err(Box::new(validation_error(
                "access_expires_at must be an RFC3339 timestamp",
            )))
        }
    };
    if cookie_value(cookie_header, "wr_vid").is_none() {
        return Err(Box::new(validation_error(
            "cookie_header must contain wr_vid",
        )));
    }
    let access_token = match cookie_value(cookie_header, "wr_skey") {
        Some(value) => value,
        None => {
            return Err(Box::new(validation_error(
                "cookie_header must contain wr_skey",
            )))
        }
    };
    let refresh_token = match cookie_value(cookie_header, "wr_rt") {
        Some(value) => value,
        None => {
            return Err(Box::new(validation_error(
                "cookie_header must contain wr_rt",
            )))
        }
    };
    let issued_at = Utc::now();
    let credentials = match crate::domain::credentials::WeReadCredentials::new(
        access_token,
        refresh_token,
        access_expires_at,
        issued_at,
    ) {
        Ok(credentials) => credentials,
        Err(error) => return Err(Box::new(validation_error(error.to_string()))),
    };
    credentials
        .with_web_cookie(cookie_header.to_owned())
        .map_err(|error| Box::new(validation_error(error.to_string())))
}

fn display_name_from_request(
    request: &ProvisionWeReadAccountRequest,
) -> Result<String, Box<Response>> {
    if let Some(display_name) = request
        .display_name
        .as_deref()
        .map(str::trim)
        .filter(|display_name| !display_name.is_empty())
    {
        return Ok(display_name.to_owned());
    }

    let encoded_name = cookie_value(request.cookie_header.trim(), "wr_name").ok_or_else(|| {
        Box::new(validation_error(
            "display_name must be provided or cookie_header must contain a non-empty wr_name",
        ))
    })?;
    let display_name = percent_decode_str(&encoded_name)
        .decode_utf8()
        .map_err(|_| Box::new(validation_error("cookie_header wr_name is not valid UTF-8")))?;
    let display_name = display_name.trim();
    if display_name.is_empty() {
        return Err(Box::new(validation_error(
            "display_name must be provided or cookie_header must contain a non-empty wr_name",
        )));
    }
    Ok(display_name.to_owned())
}

async fn get_weread_account(
    State(state): State<Arc<AdminApiState>>,
    Path(account_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(error) = authenticate(&state.auth, &headers) {
        return auth_error_response(error);
    }
    let account_id = match Uuid::parse_str(&account_id) {
        Ok(value) if !value.is_nil() => WeReadAccountId::from_uuid(value),
        _ => return validation_error("account_id must be a non-nil UUID"),
    };
    match state.weread_auth.account(account_id).await {
        Ok(account) => Json(WeReadAccountResponse::from(account)).into_response(),
        Err(AuthServiceError::AccountNotFound { .. }) => not_found("WeRead account"),
        Err(error) => auth_service_error_response(error),
    }
}

async fn set_weread_account_enabled(
    State(state): State<Arc<AdminApiState>>,
    Path(account_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<SetEnabledRequest>,
) -> Response {
    let _session = match protected_mutation(&state, &headers) {
        Ok(session) => session,
        Err(response) => return *response,
    };
    let account_id = match parse_account_id(&account_id) {
        Ok(account_id) => account_id,
        Err(response) => return *response,
    };
    match state
        .weread_auth
        .set_disabled(account_id, !request.enabled)
        .await
    {
        Ok(account) => Json(WeReadAccountResponse::from(account)).into_response(),
        Err(error) => auth_service_error_response(error),
    }
}

async fn delete_weread_account(
    State(state): State<Arc<AdminApiState>>,
    Path(account_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let _session = match protected_mutation(&state, &headers) {
        Ok(session) => session,
        Err(response) => return *response,
    };
    let account_id = match parse_account_id(&account_id) {
        Ok(account_id) => account_id,
        Err(response) => return *response,
    };
    match state.weread_auth.delete(account_id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => auth_service_error_response(error),
    }
}

async fn weread_account_page(
    State(state): State<Arc<AdminApiState>>,
    Path(account_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let session = match authenticate(&state.auth, &headers) {
        Ok(session) => session,
        Err(_) => {
            return (StatusCode::SEE_OTHER, [(header::LOCATION, "/admin/login")]).into_response()
        }
    };
    let account_id = match parse_account_id(&account_id) {
        Ok(account_id) => account_id,
        Err(response) => return *response,
    };
    match state.weread_auth.account(account_id).await {
        Ok(account) => ui::weread_account_page(&session, &account).into_response(),
        Err(AuthServiceError::AccountNotFound { .. }) => not_found("WeRead account"),
        Err(error) => auth_service_error_response(error),
    }
}

async fn weread_accounts_page(
    State(state): State<Arc<AdminApiState>>,
    headers: HeaderMap,
) -> Response {
    let session = match authenticate(&state.auth, &headers) {
        Ok(session) => session,
        Err(_) => {
            return (StatusCode::SEE_OTHER, [(header::LOCATION, "/admin/login")]).into_response()
        }
    };
    ui::weread_accounts_page(&session).into_response()
}

fn parse_account_id(value: &str) -> Result<WeReadAccountId, Box<Response>> {
    match Uuid::parse_str(value) {
        Ok(value) if !value.is_nil() => Ok(WeReadAccountId::from_uuid(value)),
        _ => Err(Box::new(validation_error(
            "account_id must be a non-nil UUID",
        ))),
    }
}

async fn list_sources(State(state): State<Arc<AdminApiState>>, headers: HeaderMap) -> Response {
    if let Err(error) = authenticate(&state.auth, &headers) {
        return auth_error_response(error);
    }
    match state.sources.list().await {
        Ok(sources) => Json(
            sources
                .into_iter()
                .map(SourceResponse::from)
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(error) => application_error_response(error),
    }
}

async fn get_source(
    State(state): State<Arc<AdminApiState>>,
    Path(source_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(error) = authenticate(&state.auth, &headers) {
        return auth_error_response(error);
    }
    let source_id = match parse_source_id(&source_id) {
        Ok(id) => id,
        Err(response) => return *response,
    };
    match state.sources.find(source_id).await {
        Ok(Some(source)) => Json(SourceResponse::from(source)).into_response(),
        Ok(None) => not_found("source"),
        Err(error) => application_error_response(error),
    }
}

#[derive(Debug, Deserialize)]
struct CreateSourceRequest {
    book_id: Option<String>,
    display_name: Option<String>,
    article_url: Option<String>,
    sync_interval_seconds: Option<i64>,
    rss_item_limit: Option<u32>,
    account_id: Option<Uuid>,
    priority: Option<i32>,
    max_attempts: Option<u32>,
    enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct UpdateSourceRequest {
    /// Omitted fields retain their existing value; an empty string is passed
    /// through to domain validation so clients receive a useful error.
    book_id: Option<String>,
    display_name: Option<String>,
    /// `None` means retain the URL, while `Some(None)` clears it. The custom
    /// deserializer preserves that distinction for explicit JSON `null`.
    #[serde(default, deserialize_with = "deserialize_nullable")]
    article_url: Option<Option<String>>,
    sync_interval_seconds: Option<i64>,
    rss_item_limit: Option<u32>,
    /// `None` means retain the binding, while `Some(None)` clears it. The
    /// custom deserializer preserves that distinction for explicit JSON null.
    #[serde(default, deserialize_with = "deserialize_nullable")]
    account_id: Option<Option<Uuid>>,
    priority: Option<i32>,
    max_attempts: Option<u32>,
}

async fn create_source(
    State(state): State<Arc<AdminApiState>>,
    headers: HeaderMap,
    Json(request): Json<CreateSourceRequest>,
) -> Response {
    let session = match authenticate(&state.auth, &headers) {
        Ok(session) => session,
        Err(error) => return auth_error_response(error),
    };
    if let Err(error) = csrf(&state.auth, &session, &headers) {
        return auth_error_response(error);
    }
    let article_url = match parse_article_url(request.article_url.as_deref()) {
        Ok(article_url) => article_url,
        Err(error) => return validation_error(error),
    };
    let supplied_book_id = request
        .book_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    if supplied_book_id.is_none() && article_url.is_none() {
        return validation_error("book_id or article_url must be provided");
    }
    let identity = if supplied_book_id.is_none() {
        match state
            .identity_resolver
            .resolve(article_url.clone().expect("article URL was checked"))
            .await
        {
            Ok(identity) => Some(identity),
            Err(error) => return identity_error_response(error),
        }
    } else {
        None
    };
    let book_id = supplied_book_id
        .or_else(|| identity.as_ref().map(|value| value.book_id().to_owned()))
        .expect("book ID or identity must exist");
    let display_name = request
        .display_name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .or_else(|| {
            identity
                .as_ref()
                .and_then(|value| value.account_name().map(str::to_owned))
        })
        .unwrap_or_else(|| book_id.clone());
    let sync_interval = match Duration::try_seconds(request.sync_interval_seconds.unwrap_or(3_600))
    {
        Some(value) => value,
        None => return validation_error("sync_interval_seconds is outside the supported range"),
    };
    let account_id = request.account_id.map(WeReadAccountId::from_uuid);
    let source = NewSource {
        id: SourceId::from_uuid(Uuid::new_v4()),
        book_id,
        display_name,
        article_url,
        enabled: request.enabled.unwrap_or(true),
        sync_interval,
        rss_item_limit: request.rss_item_limit.unwrap_or(20),
        account_id,
        scheduling_gate: SchedulingGate::Ready,
        next_fetch_at: Utc::now(),
        priority: request.priority.unwrap_or(0),
        max_attempts: request.max_attempts.unwrap_or(3),
    };
    match state.sources.create(source).await {
        Ok(source) => (StatusCode::CREATED, Json(SourceResponse::from(source))).into_response(),
        Err(error) => application_error_response(error),
    }
}

async fn update_source(
    State(state): State<Arc<AdminApiState>>,
    Path(source_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<UpdateSourceRequest>,
) -> Response {
    let _session = match protected_mutation(&state, &headers) {
        Ok(session) => session,
        Err(response) => return *response,
    };
    let source_id = match parse_source_id(&source_id) {
        Ok(id) => id,
        Err(response) => return *response,
    };
    let article_url = match request.article_url {
        None => None,
        Some(value) => match parse_article_url(value.as_deref()) {
            Ok(article_url) => Some(article_url),
            Err(error) => return validation_error(error),
        },
    };
    let sync_interval = match request.sync_interval_seconds {
        Some(value) => match Duration::try_seconds(value) {
            Some(value) => Some(value),
            None => {
                return validation_error("sync_interval_seconds is outside the supported range")
            }
        },
        None => None,
    };
    let account_id = request
        .account_id
        .map(|value| value.map(WeReadAccountId::from_uuid));
    let patch = SourcePatch {
        book_id: request.book_id,
        display_name: request.display_name,
        article_url,
        sync_interval,
        rss_item_limit: request.rss_item_limit,
        account_id,
        priority: request.priority,
        max_attempts: request.max_attempts,
    };
    match state.sources.update(source_id, patch).await {
        Ok(source) => Json(SourceResponse::from(source)).into_response(),
        Err(error) => application_error_response(error),
    }
}

async fn delete_source(
    State(state): State<Arc<AdminApiState>>,
    Path(source_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let _session = match protected_mutation(&state, &headers) {
        Ok(session) => session,
        Err(response) => return *response,
    };
    let source_id = match parse_source_id(&source_id) {
        Ok(id) => id,
        Err(response) => return *response,
    };
    match state.sources.delete(source_id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => application_error_response(error),
    }
}

fn parse_article_url(
    value: Option<&str>,
) -> Result<Option<crate::domain::source::VerifiedWechatArticleUrl>, String> {
    match value {
        Some(value) if !value.trim().is_empty() => value
            .parse::<crate::domain::source::VerifiedWechatArticleUrl>()
            .map(Some)
            .map_err(|error| error.to_string()),
        _ => Ok(None),
    }
}

fn deserialize_nullable<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Ok(Some(Option::<T>::deserialize(deserializer)?))
}

#[derive(Debug, Deserialize)]
struct EnabledRequest {
    enabled: bool,
}

async fn set_enabled(
    State(state): State<Arc<AdminApiState>>,
    Path(source_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<EnabledRequest>,
) -> Response {
    let session = match protected_mutation(&state, &headers) {
        Ok(session) => session,
        Err(response) => return *response,
    };
    let source_id = match parse_source_id(&source_id) {
        Ok(id) => id,
        Err(response) => return *response,
    };
    let _ = session;
    match state.sources.set_enabled(source_id, request.enabled).await {
        Ok(source) => Json(SourceResponse::from(source)).into_response(),
        Err(error) => application_error_response(error),
    }
}

#[derive(Debug, Deserialize)]
struct GateRequest {
    gate: SchedulingGate,
}

async fn set_gate(
    State(state): State<Arc<AdminApiState>>,
    Path(source_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<GateRequest>,
) -> Response {
    let _session = match protected_mutation(&state, &headers) {
        Ok(session) => session,
        Err(response) => return *response,
    };
    let source_id = match parse_source_id(&source_id) {
        Ok(id) => id,
        Err(response) => return *response,
    };
    match state
        .sources
        .set_scheduling_gate(source_id, request.gate)
        .await
    {
        Ok(source) => Json(SourceResponse::from(source)).into_response(),
        Err(error) => application_error_response(error),
    }
}

async fn issue_feed_token(
    State(state): State<Arc<AdminApiState>>,
    Path(source_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let _session = match protected_mutation(&state, &headers) {
        Ok(session) => session,
        Err(response) => return *response,
    };
    let source_id = match parse_source_id(&source_id) {
        Ok(id) => id,
        Err(response) => return *response,
    };
    match state.feed_tokens.issue(source_id).await {
        Ok(token) => {
            Json(json!({ "feed_path": format!("/feeds/{}.xml", token.as_str()) })).into_response()
        }
        Err(error) => feed_token_error_response(error),
    }
}

#[derive(Debug, Deserialize)]
struct HistoryQuery {
    limit: Option<u32>,
}

async fn list_sync_runs(
    State(state): State<Arc<AdminApiState>>,
    Path(source_id): Path<String>,
    Query(query): Query<HistoryQuery>,
    headers: HeaderMap,
) -> Response {
    if let Err(error) = authenticate(&state.auth, &headers) {
        return auth_error_response(error);
    }
    let source_id = match parse_source_id(&source_id) {
        Ok(id) => id,
        Err(response) => return *response,
    };
    let limit = query.limit.unwrap_or(20);
    if limit == 0 || limit > 100 {
        return validation_error("limit must be between 1 and 100");
    }
    match state.sources.find(source_id).await {
        Ok(None) => not_found("source"),
        Err(error) => application_error_response(error),
        Ok(Some(_)) => match state.sync_runs.list_for_source(source_id, limit).await {
            Ok(runs) => Json(
                runs.into_iter()
                    .map(SyncRunResponse::from)
                    .collect::<Vec<_>>(),
            )
            .into_response(),
            Err(error) => sync_run_error_response(error),
        },
    }
}

async fn login_page() -> impl IntoResponse {
    ui::login_page()
}

async fn admin_page(State(state): State<Arc<AdminApiState>>, headers: HeaderMap) -> Response {
    match authenticate(&state.auth, &headers) {
        Ok(session) => ui::admin_page(&session).into_response(),
        Err(_) => (StatusCode::SEE_OTHER, [(header::LOCATION, "/admin/login")]).into_response(),
    }
}

async fn source_page(
    State(state): State<Arc<AdminApiState>>,
    Path(source_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let session = match authenticate(&state.auth, &headers) {
        Ok(session) => session,
        Err(_) => {
            return (StatusCode::SEE_OTHER, [(header::LOCATION, "/admin/login")]).into_response()
        }
    };
    let source_id = match parse_source_id(&source_id) {
        Ok(id) => id,
        Err(response) => return *response,
    };
    match state.sources.find(source_id).await {
        Ok(Some(source)) => ui::source_page(&session, &source).into_response(),
        Ok(None) => not_found("source"),
        Err(error) => application_error_response(error),
    }
}

fn protected_mutation(
    state: &AdminApiState,
    headers: &HeaderMap,
) -> Result<AdminSession, Box<Response>> {
    let session =
        authenticate(&state.auth, headers).map_err(|error| Box::new(auth_error_response(error)))?;
    csrf(&state.auth, &session, headers).map_err(|error| Box::new(auth_error_response(error)))?;
    Ok(session)
}

fn authenticate(auth: &AdminAuthenticator, headers: &HeaderMap) -> Result<AdminSession, AuthError> {
    auth.authenticate_cookie(
        headers
            .get(header::COOKIE)
            .and_then(|value| value.to_str().ok()),
        Utc::now(),
    )
}

fn csrf(
    auth: &AdminAuthenticator,
    session: &AdminSession,
    headers: &HeaderMap,
) -> Result<(), AuthError> {
    auth.verify_csrf(
        session,
        headers
            .get("x-csrf-token")
            .and_then(|value| value.to_str().ok()),
    )
}

fn client_key(peer: SocketAddr) -> String {
    peer.ip().to_string()
}

fn parse_source_id(value: &str) -> Result<SourceId, Box<Response>> {
    Uuid::parse_str(value)
        .map(SourceId::from_uuid)
        .map_err(|_| Box::new(validation_error("source_id must be a UUID")))
}

#[derive(Debug, Serialize)]
struct SourceResponse {
    id: SourceId,
    book_id: String,
    display_name: String,
    article_url: Option<String>,
    enabled: bool,
    sync_interval_seconds: i64,
    rss_item_limit: u32,
    account_id: Option<Uuid>,
    scheduling_gate: SchedulingGate,
    feed_revision: u64,
    next_fetch_at: String,
    failure_cooldown_until: Option<String>,
    schedule_reserved_until: Option<String>,
    priority: i32,
    max_attempts: u32,
}

impl From<Source> for SourceResponse {
    fn from(source: Source) -> Self {
        Self {
            id: source.id(),
            book_id: source.book_id().to_owned(),
            display_name: source.display_name().to_owned(),
            article_url: source.article_url().map(ToString::to_string),
            enabled: source.enabled(),
            sync_interval_seconds: source.sync_interval().num_seconds(),
            rss_item_limit: source.rss_item_limit(),
            account_id: source.account_id().map(WeReadAccountId::as_uuid),
            scheduling_gate: source.scheduling_gate(),
            feed_revision: source.feed_revision().as_u64(),
            next_fetch_at: source.next_fetch_at().to_rfc3339(),
            failure_cooldown_until: source
                .failure_cooldown_until()
                .map(|value| value.to_rfc3339()),
            schedule_reserved_until: source
                .schedule_reserved_until()
                .map(|value| value.to_rfc3339()),
            priority: source.priority(),
            max_attempts: source.max_attempts(),
        }
    }
}

#[derive(Debug, Serialize)]
struct SyncRunResponse {
    id: Uuid,
    source_id: SourceId,
    job_id: Option<Uuid>,
    outcome: &'static str,
    articles_seen: u32,
    articles_created: u32,
    articles_updated: u32,
    articles_failed: u32,
    archived_articles: u32,
    archived_assets: u32,
    failure_class: Option<&'static str>,
    failure_message: Option<String>,
    feed_revision: Option<u64>,
    started_at: String,
    finished_at: Option<String>,
}

impl From<SyncRun> for SyncRunResponse {
    fn from(run: SyncRun) -> Self {
        let stats = run.stats();
        Self {
            id: run.id(),
            source_id: run.source_id(),
            job_id: run.job_id(),
            outcome: run.outcome().as_str(),
            articles_seen: stats.articles_seen,
            articles_created: stats.articles_created,
            articles_updated: stats.articles_updated,
            articles_failed: stats.articles_failed,
            archived_articles: stats.archived_articles,
            archived_assets: stats.archived_assets,
            failure_class: run.failure().map(|failure| failure.class().as_str()),
            failure_message: run.failure().map(|failure| failure.message().to_owned()),
            feed_revision: run.feed_revision().map(|revision| revision.as_u64()),
            started_at: run.started_at().to_rfc3339(),
            finished_at: run.finished_at().map(|value| value.to_rfc3339()),
        }
    }
}

fn auth_error_response(error: AuthError) -> Response {
    let status = match error {
        AuthError::RateLimited => StatusCode::TOO_MANY_REQUESTS,
        AuthError::InvalidCsrf => StatusCode::FORBIDDEN,
        _ => StatusCode::UNAUTHORIZED,
    };
    (status, Json(json!({ "error": error.to_string() }))).into_response()
}

fn validation_error(message: impl Into<String>) -> Response {
    (
        StatusCode::UNPROCESSABLE_ENTITY,
        Json(json!({ "error": message.into() })),
    )
        .into_response()
}

fn identity_error_response(error: IdentityError) -> Response {
    let status = match error {
        IdentityError::Browser(_) => StatusCode::SERVICE_UNAVAILABLE,
        IdentityError::InvalidArticleUrl(_)
        | IdentityError::InvalidBiz
        | IdentityError::MissingIdentity
        | IdentityError::UnsafeRedirect
        | IdentityError::VerificationRequired => StatusCode::UNPROCESSABLE_ENTITY,
    };
    (status, Json(json!({ "error": error.to_string() }))).into_response()
}

fn auth_service_error_response(error: AuthServiceError) -> Response {
    let status = match error {
        AuthServiceError::AccountNotFound { .. } => StatusCode::NOT_FOUND,
        AuthServiceError::Repository(
            crate::persistence::repositories::credential_repository::CredentialRepositoryError::Conflict { .. },
        ) => StatusCode::CONFLICT,
        AuthServiceError::Repository(
            crate::persistence::repositories::credential_repository::CredentialRepositoryError::NotFound { .. },
        ) => StatusCode::NOT_FOUND,
        AuthServiceError::Repository(
            crate::persistence::repositories::credential_repository::CredentialRepositoryError::Invalid(_),
        ) => StatusCode::UNPROCESSABLE_ENTITY,
        AuthServiceError::Credentials(_) => StatusCode::UNPROCESSABLE_ENTITY,
        AuthServiceError::Cipher(_) | AuthServiceError::Repository(_) => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        AuthServiceError::AccountDisabled { .. }
        | AuthServiceError::AccountBusy { .. }
        | AuthServiceError::Lease(_)
        | AuthServiceError::Refresh(_) => StatusCode::SERVICE_UNAVAILABLE,
    };
    let message = match status {
        StatusCode::NOT_FOUND => "WeRead account not found",
        StatusCode::CONFLICT => "a WeRead account with this account_id already exists",
        StatusCode::UNPROCESSABLE_ENTITY => "invalid WeRead credentials",
        _ => "WeRead account service temporarily unavailable",
    };
    (status, Json(json!({ "error": message }))).into_response()
}

fn cookie_value(header: &str, name: &str) -> Option<String> {
    header.split(';').find_map(|part| {
        let (key, value) = part.trim().split_once('=')?;
        (key.trim() == name && !value.trim().is_empty()).then(|| value.trim().to_owned())
    })
}

fn not_found(resource: &str) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(json!({ "error": format!("{resource} not found") })),
    )
        .into_response()
}

fn application_error_response(error: SourceServiceError) -> Response {
    let status = match &error {
        SourceServiceError::Source(SourceRepositoryError::BookIdConflict { .. }) => {
            StatusCode::CONFLICT
        }
        SourceServiceError::Source(SourceRepositoryError::NotFound { .. }) => StatusCode::NOT_FOUND,
        SourceServiceError::Source(SourceRepositoryError::Domain(_)) => {
            StatusCode::UNPROCESSABLE_ENTITY
        }
        _ => StatusCode::SERVICE_UNAVAILABLE,
    };
    let message = match &error {
        SourceServiceError::Source(SourceRepositoryError::BookIdConflict { .. }) => {
            "a source with this book_id already exists"
        }
        SourceServiceError::Source(SourceRepositoryError::NotFound { .. }) => "source not found",
        SourceServiceError::Source(SourceRepositoryError::Domain(error)) => {
            return (status, Json(json!({ "error": error.to_string() }))).into_response()
        }
        _ => "source service temporarily unavailable",
    };
    (status, Json(json!({ "error": message }))).into_response()
}

fn feed_token_error_response(error: FeedTokenServiceError) -> Response {
    let status = match &error {
        FeedTokenServiceError::Repository(FeedTokenRepositoryError::SourceNotFound { .. }) => {
            StatusCode::NOT_FOUND
        }
        FeedTokenServiceError::Repository(FeedTokenRepositoryError::InvalidSourceId) => {
            StatusCode::UNPROCESSABLE_ENTITY
        }
        FeedTokenServiceError::Repository(_) => StatusCode::SERVICE_UNAVAILABLE,
        FeedTokenServiceError::InvalidToken => StatusCode::UNPROCESSABLE_ENTITY,
    };
    let message = match error {
        FeedTokenServiceError::Repository(FeedTokenRepositoryError::SourceNotFound { .. }) => {
            "source not found"
        }
        FeedTokenServiceError::Repository(FeedTokenRepositoryError::InvalidSourceId) => {
            "source_id must not be nil"
        }
        FeedTokenServiceError::InvalidToken => "invalid feed token",
        FeedTokenServiceError::Repository(_) => "feed token service temporarily unavailable",
    };
    (status, Json(json!({ "error": message }))).into_response()
}

fn sync_run_error_response(error: SyncRunRepositoryError) -> Response {
    let status = match error {
        SyncRunRepositoryError::InvalidLimit => StatusCode::UNPROCESSABLE_ENTITY,
        SyncRunRepositoryError::NotFound { .. } => StatusCode::NOT_FOUND,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    };
    let message = match error {
        SyncRunRepositoryError::InvalidLimit => "invalid synchronization history limit",
        SyncRunRepositoryError::NotFound { .. } => "synchronization run not found",
        _ => "synchronization history temporarily unavailable",
    };
    (status, Json(json!({ "error": message }))).into_response()
}

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;
    use std::net::{IpAddr, Ipv4Addr};

    use super::*;

    #[tokio::test]
    async fn storage_errors_are_not_returned_to_admin_clients() {
        let response = application_error_response(SourceServiceError::Source(
            SourceRepositoryError::Storage("postgres://user:password@internal/db".to_owned()),
        ));
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("error response should be readable");
        let body = String::from_utf8_lossy(&body);
        assert!(!body.contains("password"));
        assert!(body.contains("temporarily unavailable"));
    }

    #[test]
    fn login_rate_limit_key_uses_peer_ip_instead_of_forwarded_headers() {
        let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 43_210);
        assert_eq!(client_key(peer), "127.0.0.1");
    }

    #[test]
    fn missing_feed_sources_are_not_reported_as_storage_outages() {
        let response = feed_token_error_response(FeedTokenServiceError::Repository(
            FeedTokenRepositoryError::SourceNotFound {
                source_id: SourceId::from_uuid(Uuid::from_u128(42)),
            },
        ));
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    fn provisioning_request(
        display_name: Option<&str>,
        cookie_header: &str,
    ) -> ProvisionWeReadAccountRequest {
        ProvisionWeReadAccountRequest {
            account_id: None,
            display_name: display_name.map(str::to_owned),
            cookie_header: cookie_header.to_owned(),
            access_expires_at: "2030-01-01T00:00:00Z".to_owned(),
        }
    }

    #[test]
    fn derives_display_name_from_percent_encoded_weread_cookie_name() {
        let request = provisioning_request(None, " \nwr_name=Alex%20Hua; wr_vid=vid\n ");
        assert_eq!(
            display_name_from_request(&request).ok(),
            Some("Alex Hua".to_owned())
        );
    }

    #[test]
    fn explicit_display_name_takes_precedence_over_weread_cookie_name() {
        let request = provisioning_request(Some("  Chosen name \n"), "wr_name=Cookie%20name");
        assert_eq!(
            display_name_from_request(&request).ok(),
            Some("Chosen name".to_owned())
        );
    }

    #[test]
    fn credentials_trim_outer_cookie_header_whitespace_before_storage() {
        let request = provisioning_request(
            Some("Display name"),
            " \nwr_vid=vid; wr_skey=access; wr_rt=refresh\n ",
        );
        let credentials = credentials_from_request(&request).expect("trimmed cookie is valid");

        assert_eq!(
            credentials.web_cookie(),
            Some("wr_vid=vid; wr_skey=access; wr_rt=refresh")
        );
    }

    #[tokio::test]
    async fn blank_display_name_requires_a_valid_nonempty_weread_cookie_name() {
        for cookie_header in ["wr_vid=vid", "wr_name="] {
            let request = provisioning_request(Some("   "), cookie_header);
            let response = display_name_from_request(&request).unwrap_err();
            assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
            let body = to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("validation body should be readable");
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&body).unwrap()["error"],
                "display_name must be provided or cookie_header must contain a non-empty wr_name"
            );
        }

        let request = provisioning_request(Some("   "), "wr_name=%FF");
        let response = display_name_from_request(&request).unwrap_err();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[test]
    fn account_status_distinguishes_active_expired_and_disabled_accounts() {
        let now = "2026-09-02T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let active = crate::domain::credentials::WeReadAccount::from_parts(
            WeReadAccountId::from_uuid(Uuid::from_u128(1)),
            "active".to_owned(),
            1,
            now + Duration::hours(1),
            false,
        );
        let expired = crate::domain::credentials::WeReadAccount::from_parts(
            WeReadAccountId::from_uuid(Uuid::from_u128(2)),
            "expired".to_owned(),
            1,
            now,
            false,
        );
        let disabled = crate::domain::credentials::WeReadAccount::from_parts(
            WeReadAccountId::from_uuid(Uuid::from_u128(3)),
            "disabled".to_owned(),
            1,
            now + Duration::hours(1),
            true,
        );

        assert_eq!(weread_account_status(&active, now), "active");
        assert_eq!(weread_account_status(&expired, now), "expired");
        assert_eq!(weread_account_status(&disabled, now), "disabled");
    }
}
