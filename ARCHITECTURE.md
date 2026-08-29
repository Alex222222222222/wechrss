# WechRss Rust Architecture

This document describes the planned Rust implementation of the existing
`wechrss-main` Python service. The current Rust tree remains intentionally
incremental: it defines boundaries and ownership, with the domain/configuration
policies and the first PostgreSQL job/cache-persistence slices implemented
while authenticated protocol work and HTTP behavior remain unimplemented. The
pure RSS renderer, normalized article persistence, cache-first feed delivery
decision service, and unauthenticated public article browser path are
executable, but they are not yet wired to a feed-token lookup or HTTP route.

## Goals

- Fetch WeChat public-account articles through a browser-driven acquisition
  layer.
- Archive sanitized article HTML; article-asset caching is optional in version
  one.
- Generate RSS feeds from archived data.
- Support multiple application instances with PostgreSQL-coordinated jobs.
- Keep RSS requests fast through a persisted 30-minute feed cache.
- Apply configurable, bounded pacing between upstream requests and page
  operations.
- Pause new upstream work during configured quiet hours in an IANA timezone.
- Keep authentication, browser protocol details, business rules, and storage
  replaceable independently.

## Implementation status and contract policy

This document describes the target version-one architecture, not the behavior
of a deployable server. The binary is currently a no-op. The executable
foundations are typed environment parsing for the target configuration set,
pure pacing/quiet-hours policy, PostgreSQL pool/migration helpers, the job and
feed-cache domain/repository slices, their shared transaction boundary, the
stable account identity plus distributed account-lease slice, the per-source
feed-build lease slice, atomic due-source scheduling persistence, and the
unauthenticated Thirtyfour public-page navigation/extraction, bounded
pacing/scroll, and expected browser-timezone validation slice.

The current and target contracts must not be confused:

| Area | Executable now | Target contract and implementation gate |
| --- | --- | --- |
| Runtime | No server, routes, scheduler, worker, or browser adapter | Runtime composition starts only after configuration, corrected jobs, and `UnitOfWork` are executable |
| Jobs | `0001_jobs.sql` contains `deferred`, separate `claim_count`/`failure_count`, and PostgreSQL-clocked SQLx job operations | Remove caller `now` parameters only in a later queue-port contract release |
| Configuration | Environment-only `AppConfig` with role, lease, cache, admin, and optional-asset validation; unknown owned settings are rejected and legacy archive names fail with a migration hint | Runtime composition must consume the parsed role and policy values before deployment relies on them |
| Persistence | Job/source-scheduling/article/sync-run/feed-cache tables, their PostgreSQL repositories, shared job/source/article/sync-run/feed-cache transaction boundary, account leases, and feed-build leases | Credential records and remaining transaction-scoped views are design-only |
| Acquisition/web/RSS | A validated public WeChat article URL, capability-typed browser sessions, concrete public Thirtyfour navigation/extraction with bounded pacing/scroll and expected-timezone validation, and pure RSS renderer | Authenticated protocol adapter, application handlers, and feed-token routing remain future work |

Environment variables in this document are parsed into `AppConfig`, but the
binary is still a no-op and does not construct role-specific runtime
components. The loader has an explicit allowlist for application-owned names
and rejects unknown names within those prefixes so misspellings cannot be
silently ignored, while ordinary container variables such as `PATH` remain
permitted.

Application-owned prefixes are `APP_`, `HTTP_`, `WORKER_`, `JOB_`, `ACCOUNT_`,
`SOURCE_`, `RSS_`, `FEED_`, `PACING_`, `SCROLL_`, `ASSET_`, `ADMIN_`, `SESSION_`,
`CREDENTIAL_`, `QUIET_`, `WEBDRIVER_`, `BROWSER_`, and `DATABASE_POOL_`, plus
the exact `DATABASE_URL`.
The legacy `ARCHIVE_` names remain recognized only for a deprecation error or
explicit compatibility migration. Unknown variables under an owned prefix are
startup errors; unrelated environment names are ignored.

## Runtime shape

The first runtime is a modular monolith, but each process has an explicit role
set: `api`, `scheduler`, `worker`, or `all`. `all` is convenient for a small
Docker deployment; Kubernetes may scale API and worker processes independently
so RSS traffic does not implicitly increase upstream-fetch concurrency. A
browser sidecar is required only for a process with the worker role.
PostgreSQL is the coordination point, so multiple instances can run at the
same time without executing the same active job.

```text
RSS/admin clients -> API replicas -----------------+
                                                     |
Scheduler replicas -> due-source enqueue -----------v---+
                                                     PostgreSQL
Worker replicas <-> durable job/account leases -----^---+
       |
       +-- colocated private WebDriver sidecar per worker Pod
```

The browser endpoint is private to the application network. Local browser
capacity is bounded by each process, while authenticated WeRead operations are
serialized across all replicas by a PostgreSQL account lease. The account lease
is distinct from a source job lease because different source jobs may use the
same WeRead account. Version one may expose only one configured account, but it
still assigns that account a stable identifier and uses the distributed lease.
Application and browser sidecar timezone configuration must use the same IANA
timezone, such as `Asia/Shanghai`.

Public WeChat article pages use a separate ephemeral browser-session class.
Those sessions start with a clean profile, receive no WeRead credentials or
cookies, validate navigation and redirect hosts against the WeChat allowlist,
and are destroyed after use. Authenticated account sessions must never be
reused for public article extraction.

## Data flow

### Add a source

1. The API receives an article URL or normalized `book_id`.
2. The identity acquisition module opens the URL in a browser session.
3. It extracts `biz`, decodes the numeric `bid`, and derives
   `MP_WXS_<bid>`.
4. The source repository stores the source and creates its initial schedule.
5. A deduplicated `source_sync` job becomes eligible for execution.

### Synchronize a source

1. A scheduler transaction locks eligible due sources with `FOR UPDATE SKIP
   LOCKED`, verifies that no active source-sync job exists, inserts a job, and
   records the scheduling reservation atomically.
2. A worker claims one queued job with a PostgreSQL row lock and lease.
3. The quiet-hours policy is checked before any upstream work begins.
4. The authenticated WeRead adapter loads the account context and article
   list, applying the pacing policy between upstream operations.
5. Each article URL is fetched separately from the public WeChat article page.
   This content fetch does not receive WeRead credentials and does not depend
   on the account login; it uses bounded waits and controlled scrolls for
   lazy-loaded content, then normalizes metadata, HTML, and asset references.
6. HTML is sanitized. Version one may retain approved external asset URLs and
   skip binary asset downloads; an optional asset-archive mode stores assets and
   rewrites those URLs to local media URLs.
7. Browser and network acquisition finishes before the final database
   transaction begins. Lease heartbeats continue independently while upstream
   work is in progress.
8. Outside a transaction, the service merges current RSS input with normalized
   changes and renders a candidate for the expected next feed revision. A short
   persistence `UnitOfWork` then verifies the live job fencing token and expected
   base revision, upserts articles and archive records, advances the source feed
   revision, stores that candidate, updates the sync run and source schedule,
   and marks the job successful in one transaction. A concurrent revision
   change aborts this commit and causes a fresh candidate to be built. Asset
   metadata is included only when optional asset archiving is enabled.
9. If the final transaction fails, none of its writes or job completion become
   visible. The still-live or recoverable job can safely retry the idempotent
   workflow.

All synchronization work must be idempotent. A worker can crash after an
external asset write or at the final transaction boundary; retrying must not
create duplicate articles or content versions. If optional asset archiving is
enabled, asset writes must be deduplicated as well.

### Serve RSS

1. The feed route reads one cached XML document from `feed_cache`.
2. A fresh cache is returned immediately with its ETag.
3. A stale cache is returned immediately while a deduplicated rebuild job is
   enqueued. A stale response is not advertised as fresh to downstream caches.
4. In the current cache-first implementation, a cache miss enqueues a
   deduplicated rebuild and returns a typed temporary-unavailable result with
   `Retry-After`; it does not render in the request. A future database-only
   single-flight/CAS path may populate and return the first document. Concurrent
   misses must not all render and overwrite the same source feed.

RSS requests never start browser work or wait for a synchronization job.

## Transaction ownership

Cross-repository atomicity is provided by a persistence-level `UnitOfWork`, not
by a job-specific transaction. A unit of work owns one SQLx transaction and
exposes transaction-scoped source, article, sync-run, feed-cache, and job
repositories. Application code sees repository interfaces and never receives a
raw SQLx transaction.

Long browser operations, randomized waits, asset downloads, and XML generation
from a large input must not hold database row locks. A synchronization worker
first performs acquisition and normalization, then opens a short unit of work
for final persistence and fenced job completion. Job heartbeats use the pool on
a separate connection and stop the handler if ownership or fencing is lost.
Dropped or failed units of work roll back automatically; only an explicit
`commit` publishes all changes.

The target general job-queue port exposes enqueue, claim, heartbeat, and reads.
Worker outcomes (`succeeded`, `retry_wait`, `deferred`, cancellation, and
failure) are committed through the transaction-scoped unit-of-work outcome view
because they may also write a sync run, source gate/cooldown, revision, or cache.
Expired-lease recovery is a dedicated atomic persistence operation for the same
reason. `JobQueue`, `JobOutcomeTransaction`, and `ExpiredJobRecovery` now make
these boundaries executable for PostgreSQL and the in-memory test repository.
The all-in-one `JobRepository` and `JobRepositoryTransaction` remain temporary
compatibility interfaces; new application services must depend on the narrower
ports and must not receive an independently committing completion port.

## PostgreSQL job queue

The planned `jobs` table represents individual executions and contains:

```text
id, job_type, source_id, status, priority, run_after, claim_count,
failure_count, max_attempts, lease_owner, lease_token, lease_until, heartbeat_at, started_at,
finished_at, last_error, payload_json, dedupe_key, created_at, updated_at
```

Active-job deduplication uses a partial unique index over `dedupe_key` for
`queued`, `running`, `retry_wait`, and `deferred` jobs. Workers claim jobs with
`SELECT ... FOR UPDATE SKIP LOCKED`, set an instance-specific lease and a new
per-claim fencing token, and periodically extend it. Heartbeats and terminal
updates must compare both the owner and token so an old worker cannot mutate a
later claim by the same instance. Expired leases are returned to the queue.

PostgreSQL server time is authoritative for every distributed job decision.
Production claim, heartbeat, live-lease checks, retry/defer timestamps, and
expired-lease recovery derive one statement-local timestamp from
`clock_timestamp()` (or an equivalent database-owned clock) instead of trusting
a caller-provided wall clock. `run_after` eligibility is compared with that same
database time. Application-supplied clocks remain valid for pure domain tests
and the in-memory repository, but production SQL returns persisted database
timestamps and rehydrates domain values from them. Database time offset is
monitored operationally; replica clock skew cannot shorten another worker's
lease.

The current job-repository compatibility interface still accepts a caller `now`
so the existing in-memory tests and callers remain source-compatible. PostgreSQL
does not bind that value into lease-sensitive SQL: `claim_next`, `heartbeat`,
`succeed`, `defer`, `retry`, `fail`, `cancel`, and `recover_expired` use a
statement-local `clock_timestamp()`. The eventual queue-port/`UnitOfWork` split
will remove those compatibility parameters. Retry policy will then supply a
duration, which SQL adds to `db_now`; quiet-hours deferral will supply an
absolute instant calculated from an authoritative database-time sample and the
configured IANA timezone.

Expected state transitions:

```text
queued -> running -> succeeded
queued -> running -> retry_wait -> running
queued -> running -> deferred -> running
queued/running -> failed
```

`retry_wait` represents a retryable execution failure and consumes the bounded
failure budget. `deferred` represents a non-failure eligibility boundary such
as quiet hours and sets `run_after` to the next eligible instant without
consuming that budget. Durable `claim_count` is retained for observability,
while `failure_count` is compared with `max_attempts`; a claim and a failure are
not the same event. Expired-lease crash recovery increments `failure_count` so a
worker that repeatedly disappears cannot loop forever.

Job claiming accepts an allowed-job-type filter. During quiet hours, workers may
claim local jobs such as `feed_rebuild`, but they do not claim `source_sync`,
`article_backfill`, or any other job that can contact an upstream service.

Transient browser/network errors use bounded exponential retry. Authentication
expiry allows one refresh and one retry. Risk-control or verification states
stop the job and move the source to an operator-blocked scheduling state; they
do not trigger a retry loop. Quiet hours are a scheduling and politeness
boundary, not a way to evade verification or anti-automation controls.

### V1 queue transport decision

Version one keeps the custom PostgreSQL `jobs` table as the authoritative
application queue. It owns the durable job lifecycle, active-job
deduplication, retry and failure counters, quiet-hours deferral, leases,
fencing tokens, and the transaction-scoped completion contract. The
cache-first `FeedService` currently enqueues `feed_rebuild` rows through this
table using the canonical `feed_rebuild:{source_id}` key.

PGMQ is a possible future transport optimization, not a version-one
replacement for the `jobs` table. If it is evaluated later, the safe hybrid
shape is:

```text
application jobs table = authoritative lifecycle, dedupe, retry, lease, fence
PGMQ message           = optional durable wakeup/transport for a job id
```

A future PGMQ adapter must preserve the following rules:

1. The `jobs` row remains the source of truth. A PGMQ visibility timeout must
   not be treated as the application's lease or fencing token.
2. A message should carry only a durable job identifier (and optionally a
   version), never credentials or a second copy of mutable job state. Workers
   must re-read and claim the `jobs` row before executing work.
3. Enqueueing a job and publishing its wakeup must be made transactionally
   consistent, or an outbox/reconciliation path must repair either side. A
   message that arrives twice is harmless because job claiming and active
   deduplication remain authoritative.
4. PGMQ extension or SQL-only installation, version compatibility, migration
   ownership, observability, and recovery behavior must be proven in a
   separate deployment experiment before adding it as a required dependency.

Until those conditions are satisfied, adding PGMQ would increase deployment
and migration surface without removing the correctness responsibilities
already handled by the custom table.

## Source scheduling and account leases

An enabled source also has an explicit scheduling gate:

```text
ready | authentication_required | risk_controlled
```

`enabled=false` is the operator-controlled subscription pause. Only
`enabled + ready + due` sources are eligible for automatic synchronization.
Successful completion advances `next_fetch_at` in the same unit of work as job
completion. Retryable failures remain represented by the active `retry_wait`
job. Exhausted ordinary failures advance `next_fetch_at` by a configured failure
cooldown; authentication and risk-control outcomes change the scheduling gate
and require an explicit successful login or operator action before automatic
work resumes. This prevents a terminal job from being recreated immediately
after its active deduplication key is released.

The scheduler repository owns a single atomic
`enqueue_due_sources(limit, reservation_for, quiet_hours)` operation. It samples
the PostgreSQL clock inside its transaction, evaluates the supplied quiet-hours
policy against that timestamp, and returns without source writes when the
window is active. Otherwise it locks due source rows with `SKIP LOCKED`,
excludes sources with an active source-sync job, inserts jobs with the canonical
`source_sync:{source_id}` deduplication key, and records the scheduling
reservation in the same transaction. Scheduler replicas never implement this
as a read-list followed by unrelated insert calls. The application owns the
policy configuration; the repository supplies only the authoritative timestamp
and atomic execution boundary.

Each configured WeRead account has a stable `account_id`. Authenticated list,
URL-recovery, login, and credential-refresh operations acquire a durable
`account_leases` row containing owner, fencing token, lease expiry, and
heartbeat time. Public article-page sessions do not acquire that account lease
and never receive account secrets. Account lease loss cancels authenticated
browser work before another upstream request is made. Account lease acquisition,
heartbeat, expiry, and takeover also use PostgreSQL server time; an application
replica does not decide from its local clock that another account lease expired.

## Local PostgreSQL development

PostgreSQL can be run locally as a disposable or persistent Docker container.
This is intended for repository integration tests and manual development; it
is not a production database deployment. The application and future migration
tests should connect through the same `DATABASE_URL` path used in Kubernetes.

Create a named volume and start a development-only PostgreSQL instance:

```sh
docker volume create wechrss-postgres-dev-data
docker run --detach \
  --name wechrss-postgres-dev \
  --env POSTGRES_USER=wechrss \
  --env POSTGRES_PASSWORD=wechrss-dev-only \
  --env POSTGRES_DB=wechrss \
  --publish 5432:5432 \
  --volume wechrss-postgres-dev-data:/var/lib/postgresql/data \
  --health-cmd='pg_isready -U wechrss -d wechrss' \
  --health-interval=2s \
  --health-timeout=5s \
  --health-retries=15 \
  postgres:16-alpine
```

Wait until the container reports healthy before starting the application:

```sh
docker inspect --format '{{.State.Health.Status}}' wechrss-postgres-dev
```

For local development, configure the process with environment variables. The
password below is intentionally a development-only example:

```sh
export DATABASE_URL='postgresql://wechrss:wechrss-dev-only@127.0.0.1:5432/wechrss'
export DATABASE_POOL_MIN_CONNECTIONS=1
export DATABASE_POOL_MAX_CONNECTIONS=5
```

All PostgreSQL SSL modes, CA certificates, client certificates, private keys,
passwords, and related connection options must remain in `DATABASE_URL` and
its query parameters. The Rust process passes this URL to SQLx unchanged; it
does not add separate PostgreSQL SSL environment variables. A local container
normally runs without TLS, so production credentials and certificate paths
must not be copied from this example.

The container can be stopped and restarted without losing data because the
named volume is separate from the container:

```sh
docker stop wechrss-postgres-dev
docker start wechrss-postgres-dev
```

When the local database is no longer needed, remove the container and, only if
the development data is disposable, remove its volume too:

```sh
docker rm --force wechrss-postgres-dev
docker volume rm wechrss-postgres-dev-data
```

The current implementation includes the source scheduling/revision fence,
`feed_cache`, `account_leases`, and `feed_build_leases` tables and their
PostgreSQL repositories. The scheduler repository atomically reserves due
sources and inserts canonical source-sync jobs across replicas. The application
uses SQLx's embedded `Migrator` to discover files
under `migrations/`, record applied versions and checksums in
`_sqlx_migrations`, and apply only pending migrations. The PostgreSQL
integration test uses `#[sqlx::test]`, which creates an isolated temporary
database and applies the application's embedded migrator automatically. Set
`DATABASE_URL` to a PostgreSQL administrative connection and run it with:

```sh
export DATABASE_URL='postgresql://wechrss:wechrss-dev-only@127.0.0.1:5432/wechrss'
cargo test --locked --test postgres_job_repository -- --nocapture
```

`0001_jobs.sql` is part of the embedded migration history and remains immutable
after publication. It contains the corrected `deferred`, `claim_count`, and
`failure_count` model, with active indexes that include `deferred`. No release
has been published yet, so there is no legacy `attempts` column or compatibility
trigger to retain. After publication, all schema changes must use a new forward
migration; the policy is recorded in `migrations/README.md`.

Each successful test run cleans up its temporary database; a failed run may
leave it in place for diagnosis. The configured PostgreSQL role must be allowed
to create databases. Future integration tests should use the same harness and
verify feed-cache updates alongside job claiming, lease fencing, recovery, and
active-job deduplication. CI should start an equivalent ephemeral service
rather than depend on a developer's named volume.

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
scroll distance, action count, and page duration. Version one caps
`SCROLL_MAX_STEPS` at 64 actions and `SCROLL_MAX_PIXELS` at 1,000,000 CSS
pixels before the policy reaches the browser adapter, preventing malformed
environment values from causing an unbounded allocation or page operation.

Quiet hours use an IANA timezone and a local start/end time, for example:

```text
timezone: Asia/Shanghai
quiet window: 23:00-07:00
```

The scheduler must not enqueue new upstream fetch jobs during the window, and a
worker claim filters out queued upstream job types. A worker checks again
immediately before each request and page navigation, so a quiet-window
transition is respected even for a long-running job. The current bounded
operation may finish; the worker then transitions the job to `deferred` with
`run_after` set to the end of the quiet window. This is not an error and does not
consume the failure retry budget. Feed reads, local feed rebuilds, and cache
serving continue normally.

All application replicas use the same configured timezone and clock policy.
The browser sidecar must contain timezone data and set `TZ` to the same IANA
value. A browser smoke test should verify the browser-visible local timezone;
container timezone alone is not considered verified until that test passes.

## Feed cache

The `feed_cache` table has one row per source:

```text
source_id, xml_bytes, etag, generated_at, expires_at,
feed_revision, content_hash, updated_at
```

The default freshness period is 30 minutes. Successful article fetches
proactively rebuild the cache. Source edits, article updates, URL backfills,
content changes, and optional asset rewrites invalidate or rebuild the related
row by incrementing `sources.feed_revision` in the mutation transaction. A row
is fresh only when both `expires_at > now` and its revision equals the source
revision.

The current persistence slice implements source configuration, normalized
article persistence, the cache read, and the final revision/fence
compare-and-swap publication. Callers must still provide the source revision
and normalized rendered candidate through application ports. The pure renderer
is implemented in `src/rss/renderer.rs`; the cache-first feed service is
implemented in `src/application/feed_service.rs`, while feed-token lookup and
the HTTP route remain future work.

Cache replacement uses compare-and-swap semantics. A renderer records the
source revision of its database snapshot, and the repository stores the result
only if the source still has that revision and the existing cache is not newer.
If either check fails, the rendered bytes are discarded and a deduplicated
rebuild for the current revision remains eligible. This prevents a slow replica
from overwriting newer XML. For a synchronization mutation built from base
revision `R`, the final unit of work verifies `R`, atomically advances the source
to `R+1`, and stores only the candidate labeled `R+1`.

Single-flight cache generation uses a durable `feed_build_leases` row keyed by
`source_id`; it does not use a session-level advisory lock or hold a database
connection while XML is rendered. Acquisition is a short transaction that
inserts or takes over an expired row and returns `owner`, a fresh fencing token,
and `lease_until`, all based on PostgreSQL server time. The owner commits that
transaction, reads a revisioned RSS snapshot, renders outside any transaction,
and heartbeats the build lease if necessary. A short final `UnitOfWork` verifies
the build token and source revision, replaces the cache, and releases the build
lease atomically. Lease loss or revision conflict discards the candidate.

```text
feed_build_leases:
source_id, lease_owner, lease_token, lease_until, heartbeat_at, created_at,
updated_at
```

When a stale cache exists, non-owners return it immediately. On a true cache
miss, a non-owner does not render concurrently: it performs a short bounded poll
for the lease owner's result and otherwise returns a typed temporary-unavailable
response with `Retry-After`. Expired build leases are safely takeable by another
replica.

The feed endpoint returns `ETag`, `Last-Modified`, and
`Cache-Control`. Fresh responses use the remaining lifetime rather than always
resetting the full TTL. Stale responses use `max-age=0` and an explicit bounded
`stale-while-revalidate` directive, while still honoring `If-None-Match`. The
database cache is an application cache, not a replacement for HTTP conditional
requests.

## Module boundaries

### Domain

Pure business concepts and invariants. Domain modules do not depend on Axum,
Thirtyfour, SQLx, or concrete storage. Article storage identity is the pair
`(source_id, review_id)`; `review_id` is the stable upstream identity within a
source. URLs and content may be absent in a partial list observation and are
merged with previously known detail data rather than treated as deletions.
Article observations carry a monotonic version allocated before upstream work
starts; persistence ignores an older version so out-of-order workers cannot
regress newer RSS content. `fetched_at` records completion time and is never
used as the ordering fence.
Jobs and sync statuses are explicit types so retry and risk-control behavior
cannot be represented as arbitrary strings.

### Application

Use-case orchestration. Application services call repository and acquisition
interfaces but do not contain SQL or browser selectors. `Scheduler` only
enqueues work and applies quiet-hours eligibility. `SyncService` executes
claimed work and re-checks quiet hours between upstream operations.
`ArchiveService` owns the content pipeline. `JobService` owns leases and
transitions. `FeedService` owns conditional reads, fresh/stale/missing
decisions, and deduplicated rebuild enqueueing over the persisted cache.
Feed-token lookup, single-flight cache population, and rebuild orchestration
remain future extensions. Application services receive a `UnitOfWorkFactory` for
atomic final writes; they do not compose independent repository transactions.
`SourceService` now uses a narrow transaction-scoped enqueue view to create an
eligible source and its initial `source_sync` job atomically.

### Acquisition

Browser and WeRead protocol adapters. Thirtyfour/WebDriver details are confined
here. The existing Python fetching path does not include article-content
fetching, so Rust keeps the two upstream paths explicit: WeRead credentials are
used only for account/session and article-list operations, while the rendered
article-page adapter fetches public article content without credentials. Neither
adapter exposes raw protocol details to the application layer. The pacing module
is the only owner of randomized wait generation and scroll policy; individual
adapters must not invent their own delays.

The browser abstraction exposes different capabilities for authenticated and
public sessions. The public article adapter accepts only a validated WeChat URL,
uses a clean ephemeral profile, and validates the final URL after redirects.
The type system must make it impossible to pass credentials or an authenticated
session to that adapter accidentally.

The concrete capability contract is:

- `VerifiedWechatArticleUrl` is constructed only from `https` URLs whose
  normalized host is exactly `mp.weixin.qq.com`; user information, fragments,
  non-default ports, and ambiguous encoded hosts are rejected. The final URL is
  revalidated after every navigation or redirect before extraction. The
  domain-side value object is implemented in `src/domain/source.rs`; the
  acquisition ports accept it directly, and the public WebDriver adapter
  revalidates the browser-observed URL before extraction.
- `PublicBrowserSession` is a non-cloneable fresh WebDriver session with no
  imported profile, cookies, local storage, credential handle, or account-lease
  guard. It is destroyed after the public operation.
- `AuthenticatedBrowserSession` is a distinct non-cloneable capability created
  only with a live `AccountLeaseGuard` for one stable account ID. The guard owns
  cancellation state; losing its fence prevents another authenticated request.
- `AuthenticatedBrowserSession::prepare_request` performs a PostgreSQL-clocked
  heartbeat and returns a one-request authenticated capability. An expired
  lease cannot be handed to the protocol adapter; the application must repeat
  preparation for each request or bounded protocol operation.
- `ArticlePageFetcher` accepts `VerifiedWechatArticleUrl` and consumes a
  `PublicBrowserSession`. Consuming the capability makes the one-operation
  lifetime explicit and releases local browser capacity when the fetch future
  completes or is cancelled. `WeReadAdapter` accepts only the authenticated
  request capability returned by `prepare_request`; neither API accepts a
  generic session.

The executable acquisition boundary currently includes `BrowserPool`, a
Tokio-semaphore process-local capacity limit, the storage-neutral
`AccountLeaseStore` port, `AccountLeaseGuard`, the two non-cloneable session
capabilities, `WebDriverFactory`, the public article fetcher, its
`PacingController`, and expected browser-timezone validation. A public session
has no account or credential state; an
authenticated request capability is created only after a durable account
heartbeat. A failed heartbeat permanently cancels the capability. The public
adapter creates a Thirtyfour session, applies the configured browser profile,
navigates to the verified URL, rejects an unsafe final URL, executes bounded
navigation/action/settling waits and downward scrolls, and parses common
rendered WeChat metadata/body selectors. Browser health remains behind a TODO;
when `expected_timezone` is configured, session creation validates the
browser-visible timezone before returning the public capability. IANA links are
canonicalized by the browser's own `Intl` implementation so a valid alias is
not rejected; the real-browser diagnostic compares the browser-reported value
with that canonical result. The adapter also exposes an environment diagnostic,
and the optional sidecar test verifies the configured timezone and other
measurable profile values.
Environment/CAPTCHA verification pages are returned as a distinct terminal
acquisition result; the service must not attempt to bypass them. The complete
browser-session environment is part of the upstream risk-control input: the
engine, operating system, WebDriver mode, fresh profile, headers, viewport,
and network identity can all differ. An otherwise valid public page may be
returned as an environment-verification page by a server-side automated
Chromium session while it renders normally in a local interactive Chromium
session or a server-side Firefox session. This is not evidence that the URL is
invalid, and it must not be hidden by treating the verification document as an
article. Operators may select the sidecar engine explicitly with
`BROWSER_ENGINE`; automatic cross-engine retries and automation-evasion
changes are out of scope.

### Persistence

PostgreSQL connection management, migrations, transactions, and repositories.
Repositories own SQL and map rows to domain values. Job claiming, leases,
deduplication, distributed account leases, and feed-cache reads/writes are
persistence responsibilities. `UnitOfWork` is the only cross-repository
transaction owner.

### Archive

Sanitization is required. Asset persistence, checksum-based deduplication, and
URL rewriting are optional in version one. When enabled, the `AssetStore`
abstraction supports a local persistent volume first and S3-compatible storage
later. Without it, the sanitizer retains only approved external asset URLs and
the application does not need binary asset storage or media delivery.

### RSS

Pure rendering from normalized records. It does not fetch upstream content,
open browsers, or decide when synchronization occurs. It emits stable GUIDs,
escaped XML, archived HTML, approved external asset URLs by default, and an
ETag/content hash. If optional asset archiving is enabled, it may instead emit
rewritten local asset URLs.

### Web

REST and UI boundary. Administrative endpoints are protected; RSS feed URLs
use opaque feed tokens and can be consumed without admin credentials. Tokens
and refresh credentials are never serialized into API responses or logs.
Missing admin authentication configuration never means anonymous administrator
access: administration is either explicitly enabled with complete credentials
or its routes are not registered.

## Planned dependencies

The manifest declares the dependencies used by the implemented foundation and
reserved for the remaining modules.

- Tokio for asynchronous runtime and task coordination.
- Axum and Tower HTTP middleware for the API boundary.
- SQLx with PostgreSQL for the pool, transactions, repositories, and embedded
  migrations (`postgres`, `runtime-tokio-rustls`, and `migrate` features).
- Thirtyfour for WebDriver browser sessions, including typed capabilities,
  explicit waits, and request/page-load timeout configuration.
- Serde, URL, Base64, and HTML parsing libraries for normalized data.
- The `rss` crate for RSS 2.0 serialization, stable GUIDs, namespaces, and
  `content:encoded` output; the renderer hashes the resulting bytes for ETags.
- Tracing for structured diagnostics.
- Thiserror/Anyhow for typed boundary errors and application context.
- Secrecy for in-memory secret handling.

## Target version-one configuration

Configuration is loaded only from environment variables in the first version.
There is no application configuration file and no command-line override layer.
Kubernetes ConfigMaps and Secrets may inject environment variables, but the Rust
process consumes them through its environment. The implementation-status table
above distinguishes settings that are parsed from settings whose runtime
components are not yet composed.

The typed configuration loader groups and validates variables in these
categories:

```text
DATABASE_URL
DATABASE_POOL_MIN_CONNECTIONS / DATABASE_POOL_MAX_CONNECTIONS
WEBDRIVER_URL / BROWSER_ENGINE / WORKER_CONCURRENCY
BROWSER_USER_AGENT / BROWSER_LOCALE / BROWSER_VIEWPORT_WIDTH /
BROWSER_VIEWPORT_HEIGHT / BROWSER_EXTRA_ARGS
APP_INSTANCE_ID / HTTP_BIND / HTTP_PORT
APP_ROLES
APP_TIMEZONE / QUIET_HOURS_START / QUIET_HOURS_END
JOB_POLL_SECONDS / JOB_LEASE_SECONDS / JOB_HEARTBEAT_SECONDS /
JOB_MAX_ATTEMPTS
ACCOUNT_LEASE_SECONDS / ACCOUNT_HEARTBEAT_SECONDS
SOURCE_FAILURE_COOLDOWN_SECONDS
RSS_CACHE_TTL_SECONDS / RSS_STALE_WHILE_REVALIDATE_SECONDS /
RSS_CACHE_MISS_WAIT_MS
FEED_BUILD_LEASE_SECONDS / FEED_BUILD_HEARTBEAT_SECONDS
PACING_* / SCROLL_*
ASSET_ARCHIVE_BACKEND / ASSET_ARCHIVE_LOCAL_PATH /
ASSET_ARCHIVE_S3_ENDPOINT / ASSET_ARCHIVE_S3_BUCKET /
ASSET_ARCHIVE_S3_REGION / ASSET_ARCHIVE_S3_ACCESS_KEY /
ASSET_ARCHIVE_S3_SECRET_KEY
ADMIN_ENABLED / ADMIN_PASSWORD / SESSION_SIGNING_KEY /
CREDENTIAL_ENCRYPTION_KEY
```

`APP_TIMEZONE` is an IANA timezone name and defaults to `UTC`. `APP_INSTANCE_ID`
may be omitted for local use; the loader then generates a random per-process
UUID so application replicas do not share job-lease ownership. `APP_ROLES`
defaults to `all`, `WORKER_CONCURRENCY` defaults to `1`, and account/feed-build
leases default to 600 seconds with 60-second heartbeats. The source failure
cooldown defaults to 300 seconds, RSS stale-while-revalidate defaults to 60
seconds, and a cache miss wait defaults to 5 seconds. Required secrets and
connection strings fail startup when absent or invalid. `JOB_LEASE_SECONDS`
must exceed the heartbeat interval plus the maximum page-operation duration.
Pacing, worker concurrency, cooldown, stale-cache, and cache-miss values have
practical upper bounds before conversion to runtime durations. Diagnostics
expose names and validation failures, never secret values. Environment parsing
uses typed deserialization followed by domain validation.

Browser profile diagnostics are configured through `BROWSER_USER_AGENT` (an
optional fixed page User-Agent), `BROWSER_LOCALE` (default `zh-CN`),
`BROWSER_VIEWPORT_WIDTH` and `BROWSER_VIEWPORT_HEIGHT` (defaults `1280` and
`2000`), and `BROWSER_EXTRA_ARGS` (up to 32 whitespace-separated arguments).
The User-Agent must match the actual browser engine and installed version; it
must not be randomly changed between requests or made inconsistent with the
browser's other observable values. `APP_TIMEZONE` remains the expected
browser-visible timezone. The sidecar must set the same timezone, while the
real-browser diagnostic verifies the value rather than trying to change the
sidecar's operating-system timezone.
Extra arguments cannot override controlled locale, User-Agent, viewport,
headless, or profile settings, including Chromium `--user-data-dir` and
Firefox `-profile`; public sessions therefore cannot opt into a persistent
credential-bearing browser profile through this setting.

### Reference: `we-mp-rss` browser anti-detection approach

The upstream `we-mp-rss` implementation is recorded here as a research
reference, not as a promise that its technique defeats WeChat risk controls.
Its Playwright controller combines a generated desktop/mobile User-Agent,
viewport, Chinese locale, explicit timezone, Chromium launch arguments such as
`--disable-blink-features=AutomationControlled`, and an initialization script
that changes several WebDriver-visible properties. Its article path waits for
DOM content, waits briefly for page stabilization, inspects the verification
text, extracts `#js_content`, and scrolls in bounded increments to trigger lazy
images. See its [browser controller](https://raw.githubusercontent.com/rachelos/we-mp-rss/main/driver/playwright_driver.py),
[article fetcher](https://raw.githubusercontent.com/rachelos/we-mp-rss/main/driver/wxarticle.py),
and [anti-crawler script](https://github.com/rachelos/we-mp-rss/blob/main/driver/anti_crawler_advanced.js).

Our first diagnostic slice adopts only the measurable profile inputs: fixed
User-Agent, viewport, locale, timezone verification, and explicit browser
arguments. It intentionally does not inject that JavaScript or claim to hide
`navigator.webdriver`; the diagnostic must remain attributable and must
continue to classify verification pages as a terminal upstream result.

`APP_ROLES` is a validated set containing `api`, `scheduler`, and/or `worker`;
`all` expands to all three. Worker concurrency is configured independently from
API replica count. `ASSET_ARCHIVE_BACKEND` is the enum `disabled | local | s3`
and defaults to `disabled`; local paths or object-store credentials are
validated only for the selected enabled backend.

The current loader requires a feed-build lease to exceed its heartbeat
interval. Once RSS rendering exposes a configurable maximum render duration,
the same validation must include that duration. `RSS_CACHE_MISS_WAIT_MS` is a
short bounded wait and never approaches browser or source-sync timeouts.

`ADMIN_ENABLED` defaults to false. When true, both `ADMIN_PASSWORD` and an
independent `SESSION_SIGNING_KEY` are required and startup fails if either is
missing. When false, management, QR-login, and credential mutation routes are
not registered. Feed and health routes remain available. Session cookies are
`HttpOnly`, `SameSite=Lax` or stricter, and `Secure` when served over HTTPS;
state-changing UI requests require CSRF validation. Deployments terminate TLS
at the application or a trusted ingress and apply login rate limiting.

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
- Public article sessions use fresh profiles and cannot access authenticated
  account sessions.
- The application and browser sidecar use the same explicit IANA timezone;
  sidecar images include `tzdata` and set `TZ`.
- Logs contain identifiers and error classifications, never access or refresh
  tokens.
- `/api/health` is process liveness and does not contact dependencies.
- API readiness requires PostgreSQL but does not fail solely because the browser
  sidecar is unavailable; cached RSS remains serviceable. Browser and timezone
  health are reported as degraded components and prevent browser jobs from
  being claimed. A worker-only process may include browser availability in its
  own readiness condition.

## Testing direction

The implementation phase should add domain tests, fixture tests for current and
legacy upstream responses, fake-browser tests, real WebDriver container tests,
PostgreSQL repository tests, cache/ETag tests, lease-recovery tests, and a
Docker Compose end-to-end test. The optional ignored test in
`tests/real_browser.rs` exercises one real public WeChat page through a
Chromium WebDriver sidecar; it uses no credentials and is never required in
CI. It succeeds when content is extracted and also accepts the typed
`VerificationRequired` result when the upstream blocks the test environment;
it must never treat a verification page as article content. Run it only after
the sidecar is reachable through a local port forward:

```sh
WEBDRIVER_URL=http://127.0.0.1:4444 \
  cargo test --locked --test real_browser -- --ignored --nocapture
```

Set `BROWSER_ENGINE=firefox` when the sidecar uses Firefox. The same test
asserts the known article title after successful extraction, so a working
Firefox deployment exercises the Rust navigation and parser rather than only
checking sidecar availability.

The real-browser test also applies the optional browser profile variables and
prints the effective browser values. For example, an operator may run a
controlled comparison with values matching the installed sidecar browser:

```sh
APP_TIMEZONE=Asia/Shanghai \
BROWSER_LOCALE=zh-CN \
BROWSER_VIEWPORT_WIDTH=1280 \
BROWSER_VIEWPORT_HEIGHT=2000 \
BROWSER_USER_AGENT='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/<installed-version> Safari/537.36' \
BROWSER_EXTRA_ARGS='--disable-blink-features=AutomationControlled' \
BROWSER_ENGINE=chromium \
WEBDRIVER_URL=http://127.0.0.1:4444 \
  cargo test --locked --test real_browser -- --ignored --nocapture
```

Do not interpret one successful or failed profile as proof of a universal
workaround. Compare one changed variable at a time and retain the printed
User-Agent, language, viewport, timezone, final URL, and verification result.

The requested browser dimensions are outer window dimensions. The effective
CSS inner viewport can be smaller because the driver or sidecar constrains the
window; the diagnostic therefore requires positive dimensions no larger than
the request rather than assuming exact equality. This keeps the test portable
across Chromium and Firefox sidecars while still detecting an ignored or
invalid profile.

Add pacing tests for bounds, seeded normal sampling, quiet-window boundaries,
timezone/DST behavior, and interruption between page operations. The optional
browser-sidecar test checks the browser's reported timezone, and the factory
rejects a configured timezone mismatch before returning a public session. Add
unit-of-work rollback/fencing tests, scheduler tests that prove terminal and
operator-blocked sources are not immediately recreated, non-failure deferral
tests, distributed account-lease contention tests, public session isolation/
redirect tests, and cache revision/CAS stampede tests. Add a PostgreSQL test
whose application replicas deliberately supply skewed local clocks and prove
that claim, heartbeat, and recovery still follow database time.

## Contract freeze and implementation order

The next implementation changes must make existing contracts executable rather
than add more empty module shells. Work proceeds in this order:

1. Complete the job queue slice around the now-executable queue/outcome/recovery
   ports. The repository supports allowed-kind claiming, while the initial
   `0001_jobs.sql` migration already adds `deferred`, separate claim/failure
   counters, and PostgreSQL-authoritative lease time. Replace the compatibility
   all-in-one interfaces only after the first worker uses the narrower ports.
   Use an expand/contract rollout for later schema changes so mixed replica
   versions remain safe; upgrade and concurrency tests are a gate.
2. Extend the shared `UnitOfWork` with transaction-scoped repository ports and
   complete the remaining source service and credential repositories plus their
   transaction-scoped views. Source identity/create/read and atomic initial
   source-sync enqueue are executable;
   normalized article persistence,
   synchronization-run persistence,
   scheduling state, and feed-revision mutations are already executable. The
   revision-aware feed-cache publication view, account lease, and feed-build
   lease repositories are also executable. No source or feed application
   service may bypass these boundaries with convenience transactions.
3. Make `FeedService` executable over the persisted cache (cache-first
   delivery and deduplicated rebuild enqueueing are implemented), then add the
   remaining source/archive queries and database-only rebuild orchestration.
4. Build role-aware runtime composition, scheduler/worker loops, heartbeat
   cancellation, and degraded browser health behavior.
5. Complete the acquisition slice with fresh-profile creation and browser
   health checks behind the now-executable verified-URL, Thirtyfour,
   browser-capability, public pacing/scroll, and expected-timezone ports. Public
   navigation, redirect rejection, common article extraction, and bounded
   public-page pacing/scroll execution are already executable. Authenticated
   protocol pacing hooks remain part of the WeRead adapter work.
6. Implement synchronization, RSS publication, and HTTP/UI boundaries, followed
   by multi-replica fencing, DST, redirect-isolation, cache-stampede, and
   end-to-end tests.

Each phase must leave formatting, unit tests, Clippy, migrations, and applicable
PostgreSQL integration tests green before the next phase begins.

## Current implementation scope

The implemented foundation includes the pure pacing and quiet-hours policy in
`src/domain/pacing.rs`, the environment-only typed target-configuration loader
in `src/config.rs`, SQLx PostgreSQL pool/migration helpers in
`src/persistence/postgres.rs`, and the domain plus PostgreSQL/in-memory job
repositories in `src/domain/job.rs` and
`src/persistence/repositories/job_repository.rs`. The job repository supports
durable claim leases, fencing, retries, non-failure deferral, cancellation,
recovery, separate claim/failure counters, active-job deduplication, and
allowed-kind claiming. Its
`0001_jobs.sql` schema uses the final initial job contract, while PostgreSQL
job and account-lease decisions use statement-local `clock_timestamp()`. The
job persistence boundary also exposes the independently usable `JobQueue` and
`ExpiredJobRecovery` ports plus the command-shaped `JobOutcomeTransaction`
adapter. `UnitOfWork::job_outcomes()` hides the transaction implementation and
keeps outcome application inside the shared commit boundary; the older
all-in-one job traits remain only as a migration bridge. The
pacing and configuration modules have no network, browser, database, scheduler,
or sleeping side effects. `UnitOfWorkFactory` and its transaction-scoped job
view are implemented in `src/persistence/unit_of_work.rs`; the account-lease
repository is implemented in
`src/persistence/repositories/account_lease_repository.rs`. The source
revision/feed-cache reader and transaction-scoped fenced publication are
implemented in `src/persistence/repositories/feed_cache_repository.rs`, and
`UnitOfWork` exposes that publication view. Source identity/create/read and
transaction-scoped scheduling/gate/revision mutations are implemented in
`src/persistence/repositories/source_repository.rs`. `SourceService` composes
source creation/read, operator gate changes, and atomic initial source-sync
enqueueing in `src/application/source_service.rs`. The atomic source scheduler
repository and its scheduling columns are implemented in
`src/persistence/repositories/scheduler_repository.rs`; it selects due sources
with PostgreSQL row locking, inserts canonical source-sync jobs, and records
short reservations in one transaction. Normalized article and sync-run domain
values and their PostgreSQL repositories/transaction views are implemented in
`src/domain/article.rs`, `src/persistence/repositories/article_repository.rs`,
`src/domain/sync.rs`, and `src/persistence/repositories/sync_run_repository.rs`;
they provide idempotent article upserts, feed-visible change detection,
source-scoped RSS ordering, typed sync outcomes, bounded counters, and safe
failure summaries. The sync-run repository's transaction-scoped `UnitOfWork`
view is also included in that implementation. Remaining source-service
orchestration, credential, archive, and other repository views remain future
work. The pure
RSS renderer in `src/rss/renderer.rs` is executable and
produces revision-tagged cache candidates. The cache-first `FeedService` in
`src/application/feed_service.rs` is also executable: it serves fresh or stale
rows, honors conditional ETags, and enqueues deduplicated rebuild jobs through
the custom `jobs` table adapter. It is not yet wired to feed-token lookup, the
database-only rebuild/publish workflow, or an HTTP route.

The one-pass scheduler wrapper in `src/application/scheduler.rs` now forwards
the configured quiet-hours policy to the atomic source-scheduling operation.
That repository samples PostgreSQL time and applies the policy inside its
transaction. A future runtime loop still owns polling, shutdown, retry
backoff, and metrics; it must call this boundary rather than reimplement source
selection.

The remaining tree intentionally contains no route handlers, source
configuration/feed-token orchestration, scheduler loops, credential
persistence, or business implementation. Credential and archive modules remain
documentation-only. Acquisition now contains executable capability/session
ports, local capacity/lease ownership, public WebDriver navigation, common
article extraction, bounded public-page pacing/scroll execution, and expected
browser-timezone validation; authenticated protocol work, authenticated pacing
hooks, and browser health remain future work. `SourceService` implements source
create/read, operator enable/gate changes, and the initial-job slice described
above, while article and sync-run persistence and `FeedService` implement only
the database/cache boundaries described above.
`TODO(design)` markers identify existing code and migrations that must change
before the remaining contracts are implemented.
