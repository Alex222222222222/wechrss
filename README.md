# Werrss

Werrss is a learning and experimentation project for exploring asynchronous
RSS generation, durable synchronization, and browser-based acquisition
boundaries.

## Disclaimer

This project is provided for learning and research purposes only. It is not
intended for production use, unauthorized access, or bypassing authentication,
rate limits, anti-abuse systems, CAPTCHAs, environment checks, or other access
controls.

If you use, modify, or deploy this project, you are solely responsible for:

- obtaining permission to access and process the content and accounts involved;
- complying with applicable laws, copyright and privacy requirements, and the
  terms and policies of every service you access;
- protecting credentials, session data, personal information, and archived
  content; and
- applying appropriate rate limits, security controls, monitoring, and data
  retention policies.

Do not use this project to collect, redistribute, or expose content or personal
data without authorization. The authors and contributors make no warranties
about availability, accuracy, legality, or fitness for a particular purpose and
are not responsible for misuse, data loss, service disruption, account action,
or any other consequence arising from its use. This project is not affiliated
with or endorsed by any third-party service it may interact with.

Review the project documentation and the applicable third-party policies before
running any component.

## First usable version scope

The first usable version includes a small authenticated admin panel for source
management, manual WeRead credential enrollment, synchronization status,
feed-link copying, and operator-facing error states. It has one administrator
whose username and password are supplied
through `ADMIN_USERNAME` and `ADMIN_PASSWORD`; there is no user-management
lifecycle in the first version. The panel uses the same API and
application-service boundaries as other clients and does not expose credentials,
session tokens, or browser controls. The panel is implemented when
administration is explicitly enabled. The release target also
includes a reproducible Docker image build and the deployment documentation
needed to run the release image. The checked-in Dockerfile and GitHub Actions
workflow build every branch and publish a release image to GHCR for `v*.*.*`
tags.

The following items are intentionally deferred until after the first release:

- QR-code login, because it requires interactive user confirmation and a
  dedicated login-attempt lifecycle; and
- a queue and handler for articles missed during synchronization. This is a
  useful repair/backfill improvement, but the first release relies on the
  normal source synchronization path.

## HTTP listener configuration

The public feeds, health endpoints, and optional admin panel share one HTTP
listener. Configure its bind host and port with environment variables:

```text
HTTP_BIND=0.0.0.0
HTTP_PORT=8080
```

`HTTP_BIND` defaults to `0.0.0.0` and accepts an IP address or resolvable host
name. `HTTP_PORT` defaults to `8080` and must be between `1` and `65535`.
There is no separate host or port for the admin panel. For an IPv6 bind, use a
raw address such as `::1`; the runtime adds the required brackets when it
constructs the socket address.

## Environment variable reference

Configuration is loaded once at startup from environment variables. Variables
marked **required** must be present; all other variables use the documented
default. Durations use seconds unless the name ends in `_MS`. Invalid values,
unknown names under the application-owned prefixes, and incomplete conditional
settings fail startup. Keep database URLs, passwords, tokens, and encryption
keys in a secret manager or an ignored local environment file.

### Database and process roles

| Variable | Default | Explanation |
| --- | --- | --- |
| `DATABASE_URL` | **Required** | PostgreSQL connection URL, including any SQLx-supported SSL, certificate, and authentication query parameters. It is passed through to SQLx and never logged. |
| `DATABASE_POOL_MIN_CONNECTIONS` | `1` | Minimum number of PostgreSQL connections kept in the pool. It must not exceed the maximum. |
| `DATABASE_POOL_MAX_CONNECTIONS` | `10` | Maximum number of PostgreSQL connections. It must be greater than zero. |
| `APP_ROLES` | `api` | Comma-separated roles: `api`, `scheduler`, and/or `worker`; `all` enables every role. Scheduler startup does not require an enrolled WeRead account; source-sync jobs wait for the next scheduling pass when no usable account exists. |
| `APP_INSTANCE_ID` | Generated UUID | Stable instance identity used for distributed job leases. Set a distinct value for each long-lived replica; an ID is generated for local use when omitted. |
| `HTTP_BIND` | `0.0.0.0` | Host or IP address for the shared feed, health, and optional admin listener. IPv6 addresses may be supplied without brackets. |
| `HTTP_PORT` | `8080` | Shared HTTP listener port. It must be between `1` and `65535`. |
| `LOG_LEVEL` | `warn` | Minimum log severity: `off`, `error`, `warn`, `info`, `debug`, or `trace`. Each emitted line includes a timestamp, level, module target, and event detail. |
| `APP_TIMEZONE` | `UTC` | IANA timezone used for quiet-hours evaluation and browser timezone checks. |
| `QUIET_HOURS_START` | Unset | Inclusive local quiet-hours start in `HH:MM` format. It must be set together with `QUIET_HOURS_END`. |
| `QUIET_HOURS_END` | Unset | Exclusive local quiet-hours end in `HH:MM` format. Equal start and end values are rejected. |

### Browser and WeRead acquisition

| Variable | Default | Explanation |
| --- | --- | --- |
| `WEBDRIVER_URL` | `http://webdriver:4444` | HTTP(S) URL of the internal Thirtyfour WebDriver sidecar. |
| `BROWSER_ENGINE` | `chromium` | Browser behind WebDriver: `chromium`/`chrome` or `firefox`/`firefox-esr`. |
| `BROWSER_USER_AGENT` | Browser default | Optional fixed User-Agent for controlled diagnostics. It must be non-empty, contain no control characters, and be no longer than 512 characters; keep it consistent with the selected browser. |
| `BROWSER_LOCALE` | `zh-CN` | Locale passed to the browser profile. It must be a non-empty locale token without whitespace. |
| `BROWSER_VIEWPORT_WIDTH` | `1280` | Browser viewport width in CSS pixels. Valid range: `1`–`8192`. |
| `BROWSER_VIEWPORT_HEIGHT` | `2000` | Browser viewport height in CSS pixels. Valid range: `1`–`8192`. |
| `BROWSER_EXTRA_ARGS` | Empty | Optional whitespace-separated browser arguments. At most 32 arguments are accepted, each must begin with `-`, and controlled browser arguments such as User-Agent, window size, persistent-profile paths, and headless mode cannot be overridden. |
| `WEREAD_ACCOUNT_ID` | Unset | Optional stable WeRead account UUID used as the default panel-enrolled account. When unset, an unbound source-sync job randomly selects an enabled, unexpired account enrolled through the admin panel from PostgreSQL. A source-specific account ID takes precedence. |
| `WEREAD_ARTICLE_LIST_URL` | `https://weread.qq.com/web/mp/articles` | Exact HTTPS WeRead article-list endpoint. Source synchronization first opens `https://weread.qq.com/web/shelf` in the same authenticated browser session, verifies it did not redirect to login, and then requests the article list. Credentials, fragments, and non-default ports are rejected. |

### Workers, jobs, and leases

| Variable | Default | Explanation |
| --- | --- | --- |
| `WORKER_CONCURRENCY` | `1` | Maximum number of worker loops in this process. Valid range: `1`–`1024`; this controls upstream work independently from API replicas. |
| `JOB_POLL_SECONDS` | `30` | Scheduler/worker polling interval for due work and recovery. It must be positive. |
| `JOB_LEASE_SECONDS` | `600` | Duration of a claimed job lease. It must exceed `JOB_HEARTBEAT_SECONDS` plus the maximum page-operation duration. |
| `JOB_HEARTBEAT_SECONDS` | `60` | Maximum interval between job-lease heartbeats. It must be less than `JOB_LEASE_SECONDS`. |
| `JOB_MAX_ATTEMPTS` | `3` | Failure-attempt budget for retryable jobs. It must be positive. |
| `ACCOUNT_LEASE_SECONDS` | `600` | Duration of an authenticated WeRead account lease. It must exceed its heartbeat interval. |
| `ACCOUNT_HEARTBEAT_SECONDS` | `60` | Maximum interval between authenticated-account lease heartbeats. It must be less than `ACCOUNT_LEASE_SECONDS`. |
| `SOURCE_FAILURE_COOLDOWN_SECONDS` | `300` | Delay before an ordinarily failed source may be scheduled again. It may be zero and is capped at seven days. |

The interval between successful fetches of an individual source is not a
process environment variable. It is the source's `sync_interval_seconds`
setting in the admin API and defaults to one hour for a newly created source.
The next fetch is scheduled relative to the completion time of the previous
sync.

### RSS and feed-cache behavior

| Variable | Default | Explanation |
| --- | --- | --- |
| `RSS_CACHE_TTL_SECONDS` | `1800` | Freshness period for a persisted RSS document. It must be positive. |
| `RSS_STALE_WHILE_REVALIDATE_SECONDS` | `60` | Additional period during which stale RSS may be served while a rebuild is requested. It may be zero and is capped at 24 hours. |
| `RSS_CACHE_MISS_WAIT_MS` | `5000` | Bounded wait associated with a cache miss before retry advice is returned. Valid range: `1`–`60000` milliseconds. |
| `SERVER_ROOT_URL` | Unset | Public HTTP(S) root URL used to build generated RSS channel links and absolute feed links shown by the admin panel. It is required when the worker role is enabled. |
| `FEED_BUILD_LEASE_SECONDS` | `600` | Duration of a distributed feed-build lease. It must exceed its heartbeat interval. |
| `FEED_BUILD_HEARTBEAT_SECONDS` | `60` | Maximum interval between feed-build lease heartbeats. It must be less than `FEED_BUILD_LEASE_SECONDS`. |

### Request pacing and bounded scrolling

Pacing values are bounded normal-distribution parameters in milliseconds. Each
sample is clamped to its configured inclusive minimum and maximum. Every value
is finite and non-negative, each minimum must not exceed its maximum, and an
individual delay is capped at 300,000 milliseconds. A zero standard deviation
produces a constant delay. These settings reduce upstream request pressure and
allow lazy page content to settle; they are not an anti-detection mechanism.

| Variable | Default | Explanation |
| --- | --- | --- |
| `PACING_REQUEST_MEAN_MS` | `2000` | Mean delay before a WeRead or other upstream protocol request. |
| `PACING_REQUEST_STDDEV_MS` | `250` | Standard deviation for upstream-request delay sampling. |
| `PACING_REQUEST_MIN_MS` | `1000` | Inclusive lower bound for upstream-request delays. |
| `PACING_REQUEST_MAX_MS` | `4000` | Inclusive upper bound for upstream-request delays. |
| `PACING_PAGE_NAVIGATION_MEAN_MS` | `3000` | Mean delay before navigating to a public article page. |
| `PACING_PAGE_NAVIGATION_STDDEV_MS` | `500` | Standard deviation for page-navigation delay sampling. |
| `PACING_PAGE_NAVIGATION_MIN_MS` | `1500` | Inclusive lower bound for page-navigation delays. |
| `PACING_PAGE_NAVIGATION_MAX_MS` | `7000` | Inclusive upper bound for page-navigation delays. |
| `PACING_PAGE_ACTION_MEAN_MS` | `1000` | Mean delay between public page actions such as extraction and scrolling. |
| `PACING_PAGE_ACTION_STDDEV_MS` | `200` | Standard deviation for page-action delay sampling. |
| `PACING_PAGE_ACTION_MIN_MS` | `500` | Inclusive lower bound for page-action delays. |
| `PACING_PAGE_ACTION_MAX_MS` | `3000` | Inclusive upper bound for page-action delays. |
| `PACING_SCROLL_SETTLE_MEAN_MS` | `1000` | Mean delay after a scroll so lazy-loaded content can settle. |
| `PACING_SCROLL_SETTLE_STDDEV_MS` | `200` | Standard deviation for scroll-settle delay sampling. |
| `PACING_SCROLL_SETTLE_MIN_MS` | `500` | Inclusive lower bound for scroll-settle delays. |
| `PACING_SCROLL_SETTLE_MAX_MS` | `3000` | Inclusive upper bound for scroll-settle delays. |
| `SCROLL_MAX_STEPS` | `4` | Maximum number of bounded scroll actions per article page. Valid range: `1`–`64`. |
| `SCROLL_MAX_PIXELS` | `4000` | Maximum cumulative CSS-pixel scroll distance per article page. Valid range: `1`–`1000000`. |
| `SCROLL_MAX_OPERATION_SECONDS` | `30` | Maximum duration of one page interaction and scroll operation. It must be positive and no greater than one hour. |

### Optional asset archiving

| Variable | Default | Explanation |
| --- | --- | --- |
| `ASSET_ARCHIVE_BACKEND` | `disabled` | Asset mode: `disabled` keeps approved external URLs, `local` writes to a local directory, or `s3` uses an S3-compatible object store. |
| `ASSET_ARCHIVE_LOCAL_PATH` | Unset | Required when the backend is `local`; persistent directory for archived binary assets. |
| `ASSET_ARCHIVE_S3_ENDPOINT` | Unset | Required when the backend is `s3`; credential-free HTTP(S) S3-compatible endpoint. |
| `ASSET_ARCHIVE_S3_BUCKET` | Unset | Required when the backend is `s3`; object-store bucket name. |
| `ASSET_ARCHIVE_S3_REGION` | Unset | Required when the backend is `s3`; region or signing scope. |
| `ASSET_ARCHIVE_S3_ACCESS_KEY` | Unset | Required when the backend is `s3`; object-store access key. Store it as a secret. |
| `ASSET_ARCHIVE_S3_SECRET_KEY` | Unset | Required when the backend is `s3`; object-store secret key. Store it as a secret. |

### Administration and encryption

| Variable | Default | Explanation |
| --- | --- | --- |
| `ADMIN_ENABLED` | `false` | Enables the single-administrator API and panel when `true`. |
| `ADMIN_USERNAME` | Unset | Required only when `ADMIN_ENABLED=true`; configured administrator username. |
| `ADMIN_PASSWORD` | Unset | Required only when `ADMIN_ENABLED=true`; configured administrator password. Store it as a secret. |
| `SESSION_SIGNING_KEY` | Unset | Required only when `ADMIN_ENABLED=true`; independent secret used to sign admin sessions. It must differ from the admin password and credential-encryption key. |
| `CREDENTIAL_ENCRYPTION_KEY` | **Required** | Secret used to encrypt WeRead credentials before persistence. It must be protected and must not be reused as the session-signing key. |

## Roadmap

These possible features are ordered approximately by user value and operational
impact, not by implementation difficulty. The list is a planning guide rather
than a commitment:

1. **Add browser health and worker readiness diagnostics.** Report WebDriver
   availability and timezone mismatches separately from API liveness and
   PostgreSQL readiness, and prevent browser jobs from being claimed while the
   browser sidecar is unhealthy.
2. **Polish the web UI.** Improve information hierarchy, loading and empty
   states, validation feedback, and responsive behavior in the administrator
   panel so routine source and account operations are easier to understand.
3. **Add internationalization (i18n).** Move user-facing panel messages into
   translation resources and add additional locale support after the default
   Chinese-language experience is stable.
4. **Normalize application logs.** Define a consistent structured event and
   severity vocabulary across API, scheduler, worker, browser, and persistence
   paths; redact sensitive values and make correlation identifiers predictable
   for operators and log aggregation tools.
5. **Add QR-code login.** Implement the bounded, single-use login-attempt
   lifecycle and interactive confirmation flow so operators do not need to
   supply credentials manually. This remains deferred after the
   first release.
6. **Add missed-article repair/backfill jobs.** Queue and process articles
   missed during synchronization with bounded retries and deduplication. This
   improves recovery after partial upstream failures but is not required for
   the first release.
7. **Persist archived assets and rewrite feed URLs.** Store approved media in
   local or object storage so archived articles can remain useful when
   upstream assets change or disappear.
8. **Evaluate PGMQ as a queue transport optimization.** The current custom
   `jobs` table remains the version-one transport; PGMQ can be evaluated later
   if queue throughput or operational overhead becomes a demonstrated
   bottleneck.

The release image is built by `.github/workflows/container.yml`. Push a
semantic-version tag such as `v0.1.7` to build and publish
`ghcr.io/<owner>/<repository>:v0.1.7` and `:latest`; branch and pull-request
builds validate the Dockerfile without publishing. The image expects the same
environment variables described in [DEPLOYMENT.md](DEPLOYMENT.md), including
`DATABASE_URL`; no credentials are baked into the image. The Dockerfile keeps
dependency compilation in a cacheable manifest-only layer and uses a small
non-root distroless runtime image. The application image does not need
`tzdata`; the browser sidecar still does.

## WeRead authentication

Authentication is split into two separate flows. Both flows must use an
account-specific distributed lease, and neither flow may expose access or
refresh tokens in logs, API responses, job payloads, or error messages.

### Non-interactive provisioning and refresh

The implemented authentication lifecycle accepts credentials from a trusted
operator or another approved login adapter through `AuthService::provision`.
Credentials are encrypted with the configured `CREDENTIAL_ENCRYPTION_KEY`
before being written to PostgreSQL. The `weread_accounts` table stores only
encrypted credential material, account metadata, expiry, and an optimistic
credential version.

`AuthService::refresh_if_needed` samples the repository's authoritative clock,
checks the configured refresh window, and refreshes only when the access
credential is near expiry. Refresh is serialized with the existing account
lease; the lease is heartbeated while the exchange runs, and a lease-fenced,
version-checked replacement prevents stale writes. A refresh response may
rotate the refresh token. If it does not, the previous refresh token is
retained. Authentication-required and risk-control results stop the operation
and require operator action; they are not retried automatically.

This service boundary is implemented and tested. Administrators can enroll an
already-issued WeRead web cookie header from `/admin`; the API accepts it only
on the protected, CSRF-guarded `POST /api/admin/weread/accounts` route and
returns only the generated account ID and non-secret status metadata. `GET
/api/admin/weread/accounts/{account_id}` returns that same metadata for
checking an enrollment. Re-authentication uses the protected `PUT
/api/admin/weread/accounts/{account_id}` route and keeps the same account ID
for existing source references. QR login is not part of this flow, and
credentials cannot be read back through the panel.

A deployment-specific
`CredentialRefresher` can be injected into `RuntimeSupervisor`; that enables
`credential_refresh` jobs in the worker plan and makes the runtime scheduler
enqueue one deduplicated refresh job for each active account within the
refresh window. The browser-backed source-sync runtime always loads the
enrolled cookie header through `AuthService` and injects it into a fresh
authenticated browser session. See
[`AuthService`](src/application/auth_service.rs) and the
[authentication architecture](ARCHITECTURE.md#source-scheduling-and-account-leases).

Scheduler and worker roles can start before any WeRead account is enrolled.
When a due source-sync job finds no enabled, unexpired account, it records a
warning and a scheduled failure; the source is reconsidered on its next due
interval. Enrolling or replacing credentials in the admin panel makes the
account available to later jobs without restarting the worker.

When adding a source, its WeRead account ID is optional. Set the ID returned by
the admin panel to pin that source to one account; leave it empty to randomly
select among enabled, unexpired enrolled accounts for each synchronization
job.

### Single-admin panel and API

Enable the first-version administration surface with `ADMIN_ENABLED=true`,
`ADMIN_USERNAME`, `ADMIN_PASSWORD`, and an independent `SESSION_SIGNING_KEY`.
Open `/admin/login` and submit the configured credentials. A successful
`POST /api/admin/login` returns a short-lived `HttpOnly` session cookie and a
CSRF token; send that token as `X-CSRF-Token` on every state-changing admin
request. The panel is available at `/admin` and uses these API routes:

- `GET /api/admin/sources` and `POST /api/admin/sources` for source management;
- `GET /api/admin/sources/{id}` to inspect a source and `PUT`/`DELETE` on the
  same path to edit or remove it; the panel exposes this at
  `/admin/sources/{id}`;
- `POST /api/admin/sources/{id}/enabled` and `/gate` for operator controls;
- `POST /api/admin/sources/{id}/feed-token` to create/rotate a copyable feed
  link. The response always includes `feed_path` and `feed_url`; `feed_url` is
  absolute when `SERVER_ROOT_URL` is configured and `null` otherwise, and is
  the value displayed by the panel; and
- `GET /api/admin/sources/{id}/sync-runs` for synchronization history;
- `POST /api/admin/weread/accounts` to enroll an already-issued WeRead cookie
  header and expiry; and
- `PUT /api/admin/weread/accounts/{account_id}` to replace an account's cookie
  header without changing source references; and
- `GET /api/admin/weread/accounts/{account_id}` to inspect non-secret account
  status.

Sources can be created with either a `book_id` or an `article_url` (or both).
When only an article URL is supplied, the API resolves its WeRead book ID from
the long URL or through the configured clean browser sidecar for a short
`/s/...` link. A supplied book ID always wins. The display name is optional:
the resolved public-account name is preferred, then the book ID is used as a
stable fallback. An article URL is stored when supplied; book-only sources
store no URL. `account_id` is also optional and, when present, pins future
source synchronization to that WeRead account; otherwise the worker selects
an enabled, unexpired account at run time.

There is no user-management endpoint. Put the application behind TLS in a
deployment so the session cookie and credentials are protected in transit.

### QR login

QR login is intentionally not implemented yet and requires user interaction.
The planned flow is:

1. create a short-lived, single-use login attempt bound to one account;
2. display the upstream QR image or safe equivalent without logging its value;
3. poll with a bounded interval and deadline, handling pending, scanned,
   confirmed, expired, cancelled, and risk-controlled states;
4. exchange the confirmed result for credentials through a dedicated
   `CredentialRefresher`/login transport; and
5. validate the account identity, encrypt the credentials, provision the
   account, and discard the temporary login state.

The eventual route must require administrator authentication, CSRF protection,
login rate limiting, single-use attempt expiry, and explicit cancellation.
QR contents and upstream credentials must never be persisted or returned in
status responses. Until that boundary exists, operators must not expect a QR
endpoint or attempt to place login secrets in environment variables or source
files. Automated tests can cover the state machine and expiry transitions;
the real QR scan remains an opt-in manual test.

## Test coverage

Generate coverage locally with `cargo-llvm-cov`. The reports are build
artifacts and are intentionally written under `target/`, not committed to the
repository.

Install the tool and its Rust support component once:

```sh
cargo install cargo-llvm-cov --locked
rustup component add llvm-tools-preview
```

Run the database-independent unit-test coverage report with missing lines:

```sh
cargo llvm-cov --all-features --workspace --lib --text --show-missing-lines
```

Generate an HTML report for interactive inspection:

```sh
cargo llvm-cov --all-features --workspace --lib --html
# Open target/llvm-cov/html/index.html in a browser.
```

Export an LCOV report for CI or other coverage tooling:

```sh
cargo llvm-cov --all-features --workspace --lib \
  --lcov --summary-only --output-path target/llvm-cov/unit.lcov
```

To include PostgreSQL integration tests, set `DATABASE_URL` to an
administrative PostgreSQL connection that the SQLx test harness may use, then
run the integration targets:

```sh
: "${DATABASE_URL:?set DATABASE_URL before running database coverage}"
cargo llvm-cov --all-features --workspace --tests --html \
  --output-dir target/llvm-cov/integration-html
```

The integration command may create isolated databases for each
`#[sqlx::test]`; use the same external development database and resource
limits documented for the nextest workflow. Keep credentials in the shell or
an ignored local development file, never in README examples or committed
reports.

For the complete test suite, install `cargo-nextest` and use the repository's
checked-in `.config/nextest.toml` configuration:

```sh
cargo install cargo-nextest --locked
cargo nextest run --locked --tests -j 16
```

The global `-j 16` keeps database-independent tests fast, while the
configuration places the API and PostgreSQL-backed test binaries in a group
limited to sixteen concurrent isolated-database setups. This keeps the
database workload bounded independently if the global test concurrency is
raised later. The cap can be lowered in `.config/nextest.toml` for a smaller
or shared PostgreSQL service.
