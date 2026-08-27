# WechRss Rust Architecture

This document describes the planned Rust implementation of the existing
`wechrss-main` Python service. The current Rust tree is intentionally a
documentation-first skeleton: it defines boundaries and ownership without
implementing network access, browser automation, persistence, scheduling, or
HTTP behavior.

## Goals

- Fetch WeChat public-account articles through a browser-driven acquisition
  layer.
- Archive sanitized article HTML and its assets.
- Generate RSS feeds from archived data.
- Support multiple application instances with PostgreSQL-coordinated jobs.
- Keep RSS requests fast through a persisted 30-minute feed cache.
- Apply configurable, bounded pacing between upstream requests and page
  operations.
- Pause new upstream work during configured quiet hours in an IANA timezone.
- Keep authentication, browser protocol details, business rules, and storage
  replaceable independently.

## Runtime shape

The first runtime is a modular monolith. Every application instance contains
the HTTP API, minimal UI, job enqueuer, job worker, and lease-recovery loop.
PostgreSQL is the coordination point, so multiple instances can run at the
same time without executing the same active job.

```text
                 +-----------------------------+
                 | Rust application instance 1 |
                 | API + UI + enqueue + worker  |
                 +--------------+--------------+
                                |
                 +--------------v--------------+
                 | PostgreSQL                  |
                 | sources, jobs, articles,    |
                 | archive metadata, feed cache|
                 +--------------^--------------+
                                |
                 +--------------+--------------+
                 | Rust application instance N |
                 +-----------------------------+
                                |
                 +--------------v--------------+
                 | WebDriver browser sidecar  |
                 | Chromium/ChromeDriver or   |
                 | Firefox/GeckoDriver        |
                 +-----------------------------+
```

The browser endpoint is private to the application network. Browser sessions
are initially serialized per account because upstream access is rate-sensitive
and the Python reference implementation uses conservative request spacing.
Application and browser sidecar timezone configuration must use the same IANA
timezone, such as `Asia/Shanghai`.

## Data flow

### Add a source

1. The API receives an article URL or normalized `book_id`.
2. The identity acquisition module opens the URL in a browser session.
3. It extracts `biz`, decodes the numeric `bid`, and derives
   `MP_WXS_<bid>`.
4. The source repository stores the source and creates its initial schedule.
5. A deduplicated `source_sync` job becomes eligible for execution.

### Synchronize a source

1. Any instance finds due sources and inserts a deduplicated job.
2. A worker claims one queued job with a PostgreSQL row lock and lease.
3. The quiet-hours policy is checked before any upstream work begins.
4. The browser adapter loads the WeRead/account context and article list,
   applying the pacing policy between upstream operations.
5. Article pages are fetched, with bounded waits and controlled scrolls for
   lazy-loaded content, then normalized into metadata, HTML, and assets.
6. HTML is sanitized and asset URLs are rewritten to local archive URLs.
7. Articles and archive records are upserted transactionally.
8. The source feed is rendered and written to `feed_cache`.
9. The job, sync run, source status, and next schedule are committed.

All synchronization work must be idempotent. A worker can crash after an
upsert and before job completion; retrying must not create duplicate articles,
content versions, or assets.

### Serve RSS

1. The feed route reads one cached XML document from `feed_cache`.
2. A fresh cache is returned immediately with its ETag.
3. A stale cache is returned immediately while a deduplicated rebuild job is
   enqueued.
4. If no cache exists, the feed is rendered from current database records and
   stored before returning.

RSS requests never start browser work or wait for a synchronization job.

## PostgreSQL job queue

The planned `jobs` table represents individual executions and contains:

```text
id, job_type, source_id, status, priority, run_after, attempts,
max_attempts, lease_owner, lease_token, lease_until, heartbeat_at, started_at,
finished_at, last_error, payload_json, dedupe_key, created_at, updated_at
```

Active-job deduplication uses a partial unique index over `dedupe_key` for
`queued`, `running`, and `retry_wait` jobs. Workers claim jobs with
`SELECT ... FOR UPDATE SKIP LOCKED`, set an instance-specific lease and a new
per-claim fencing token, and periodically extend it. Heartbeats and terminal
updates must compare both the owner and token so an old worker cannot mutate a
later claim by the same instance. Expired leases are returned to the queue.

Expected state transitions:

```text
queued -> running -> succeeded
queued -> running -> retry_wait -> running
queued/running -> failed
```

Transient browser/network errors use bounded exponential retry. Authentication
expiry allows one refresh and one retry. Risk-control or verification states
stop the job and mark the source for operator attention; they do not trigger a
retry loop. Quiet hours are a scheduling and politeness boundary, not a way to
evade verification or anti-automation controls.

## Pacing and quiet hours

The pacing policy is explicit and shared by the WeRead and article-page
acquisition adapters. It supports separate delays for:

- before an upstream request;
- before a new article-page navigation;
- between page actions such as extraction and scrolling;
- after a page has been scrolled, to allow lazy content to settle.

The default distribution is a bounded, truncated normal distribution with a
configured mean, standard deviation, minimum, and maximum. Values are clamped
to the configured bounds, and tests use a seeded random source. This is for
request pacing and load reduction; it must not be described or tuned as an
anti-detection mechanism.

Scrolling is a small configurable sequence of bounded viewport increments with
settling waits. Its purpose is to trigger legitimate lazy-loaded article
content, not to simulate arbitrary human input. The policy must cap total
scroll distance, action count, and page duration.

Quiet hours use an IANA timezone and a local start/end time, for example:

```text
timezone: Asia/Shanghai
quiet window: 23:00-07:00
```

The scheduler must not enqueue new upstream fetch jobs during the window. A
worker checks again immediately before each request and page navigation, so a
quiet-window transition is respected even for a long-running job. The current
operation may finish; the worker then stops before the next upstream operation
and records a resumable result. Feed reads and cache serving continue normally.

All application replicas use the same configured timezone and clock policy.
The browser sidecar must contain timezone data and set `TZ` to the same IANA
value. A browser smoke test should verify the browser-visible local timezone;
container timezone alone is not considered verified until that test passes.

## Feed cache

The planned `feed_cache` table has one row per source:

```text
source_id, xml_bytes, etag, generated_at, expires_at,
article_revision, content_hash, updated_at
```

The default freshness period is 30 minutes. Successful article fetches
proactively rebuild the cache. Source edits, article updates, URL backfills,
content changes, and asset rewrites invalidate or rebuild the related row.

The feed endpoint returns `ETag`, `Last-Modified`, and
`Cache-Control: public, max-age=1800`. The database cache is an application
cache, not a replacement for HTTP conditional requests.

## Module boundaries

### Domain

Pure business concepts and invariants. Domain modules do not depend on Axum,
Fantoccini, SQLx, or concrete storage. Article identity is `review_id`; URLs
may be absent or later replaced. Jobs and sync statuses are explicit types so
retry and risk-control behavior cannot be represented as arbitrary strings.

### Application

Use-case orchestration. Application services call repository and acquisition
interfaces but do not contain SQL or browser selectors. `Scheduler` only
enqueues work and applies quiet-hours eligibility. `SyncService` executes
claimed work and re-checks quiet hours between upstream operations.
`ArchiveService` owns the content pipeline. `JobService` owns leases and
transitions.

### Acquisition

Browser and WeRead protocol adapters. Fantoccini/WebDriver details are confined
here. Identity resolution, account authentication, article listing, and
rendered article extraction expose typed results and typed acquisition errors
to the application layer. The pacing module is the only owner of randomized
wait generation and scroll policy; individual adapters must not invent their
own delays.

### Persistence

PostgreSQL connection management, migrations, transactions, and repositories.
Repositories own SQL and map rows to domain values. Job claiming, leases,
deduplication, and feed-cache reads/writes are persistence responsibilities.

### Archive

Sanitization, asset persistence, checksum-based deduplication, and URL
rewriting. The `AssetStore` abstraction supports a local persistent volume
first and S3-compatible storage later.

### RSS

Pure rendering from normalized records. It does not fetch upstream content,
open browsers, or decide when synchronization occurs. It emits stable GUIDs,
escaped XML, archived HTML, rewritten assets, and an ETag/content hash.

### Web

REST and UI boundary. Administrative endpoints are protected; RSS feed URLs
use opaque feed tokens and can be consumed without admin credentials. Tokens
and refresh credentials are never serialized into API responses or logs.

## Planned dependencies

The manifest declares architectural dependencies only. Their APIs are not
used by this skeleton yet.

- Tokio for asynchronous runtime and task coordination.
- Axum and Tower HTTP middleware for the API boundary.
- SQLx with PostgreSQL for the pool, transactions, and repositories.
- Fantoccini for WebDriver browser sessions.
- Serde, URL, Base64, and HTML/XML libraries for parsing and rendering.
- Tracing for structured diagnostics.
- Thiserror/Anyhow for typed boundary errors and application context.
- Secrecy for in-memory secret handling.

## Version-one configuration

Configuration is loaded primarily and intentionally only from environment
variables in the first version. There is no application configuration file and
no command-line override layer. Kubernetes ConfigMaps and Secrets may inject
environment variables, but the Rust process consumes them through its
environment.

The typed configuration loader should group and validate variables in these
categories:

```text
DATABASE_URL
DATABASE_POOL_MIN_CONNECTIONS / DATABASE_POOL_MAX_CONNECTIONS
WEBDRIVER_URL / BROWSER_ENGINE
APP_INSTANCE_ID / HTTP_BIND / HTTP_PORT
APP_TIMEZONE / QUIET_HOURS_START / QUIET_HOURS_END
JOB_POLL_SECONDS / JOB_LEASE_SECONDS / JOB_HEARTBEAT_SECONDS /
JOB_MAX_ATTEMPTS
RSS_CACHE_TTL_SECONDS
PACING_* / SCROLL_*
ARCHIVE_BACKEND / ARCHIVE_LOCAL_PATH / object-storage settings
ADMIN_PASSWORD / CREDENTIAL_ENCRYPTION_KEY
```

`APP_TIMEZONE` is an IANA timezone name and defaults only when a safe default
is explicitly documented. `APP_INSTANCE_ID` may be omitted for local use; the
loader then generates a random per-process UUID so application replicas do not
share job-lease ownership. Required secrets and connection strings must fail
startup when absent or invalid. `JOB_LEASE_SECONDS` must exceed the heartbeat
interval plus the maximum page-operation duration. Pacing and page-operation
values have practical upper bounds before conversion to runtime durations.
Diagnostics expose names and validation errors, never secret values.
Environment parsing should use typed deserialization (for example, the `envy`
dependency) followed by domain validation.

PostgreSQL pool sizing is configured with
`DATABASE_POOL_MIN_CONNECTIONS` and `DATABASE_POOL_MAX_CONNECTIONS`, then
applied to SQLx `PoolOptions`. All PostgreSQL SSL, certificate, private-key,
password, and related connection settings are carried through `DATABASE_URL`
and its query parameters. The application does not define separate PostgreSQL
SSL/certificate environment variables and must pass the URL through to SQLx
without exposing it in logs.

## Security and operations

- WebDriver is reachable only on the internal application network.
- Credential values are encrypted before PostgreSQL persistence.
- Encryption keys and administrative secrets come from deployment secrets.
- Browser sessions are closed on normal completion and failure.
- The application and browser sidecar use the same explicit IANA timezone;
  sidecar images include `tzdata` and set `TZ`.
- Logs contain identifiers and error classifications, never access or refresh
  tokens.
- Readiness checks PostgreSQL and browser-sidecar availability separately from
  liveness.

## Testing direction

The implementation phase should add domain tests, fixture tests for current and
legacy upstream responses, fake-browser tests, real WebDriver container tests,
PostgreSQL repository tests, cache/ETag tests, lease-recovery tests, and a
Docker Compose end-to-end test. Add pacing tests for bounds, seeded normal
sampling, quiet-window boundaries, timezone/DST behavior, and interruption
between page operations. Add a browser-sidecar test that checks the browser's
reported timezone. Real WeChat access must not be required in CI.

## Current implementation scope

The first implemented slices are the pure pacing and quiet-hours policy in
`src/domain/pacing.rs` and the environment-only typed configuration loader in
`src/config.rs`. They validate delay distributions, scroll limits, URLs,
durations, secrets, browser settings, and local quiet windows using IANA
timezones. They have no network, browser, database, scheduler, or sleeping
side effects.

The remaining tree intentionally contains no migrations, route handlers,
browser calls, database calls, scheduler loops, or business implementation.
Each remaining Rust file explains the contract it will eventually implement.
