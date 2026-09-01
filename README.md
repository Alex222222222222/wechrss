# WechRss

WechRss is a learning and experimentation project for exploring asynchronous
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
management, synchronization status, feed-link copying, and operator-facing
error states. It has one administrator whose username and password are supplied
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

## Roadmap

These possible features are ordered approximately by user value and operational
impact, not by implementation difficulty. The list is a planning guide rather
than a commitment:

1. **Add browser health and worker readiness diagnostics.** Report WebDriver
   availability and timezone mismatches separately from API liveness and
   PostgreSQL readiness, and prevent browser jobs from being claimed while the
   browser sidecar is unhealthy.
2. **Add QR-code login.** Implement the bounded, single-use login-attempt
   lifecycle and interactive confirmation flow so operators do not need to
   supply a pre-authenticated browser profile. This remains deferred after the
   first release.
3. **Add missed-article repair/backfill jobs.** Queue and process articles
   missed during synchronization with bounded retries and deduplication. This
   improves recovery after partial upstream failures but is not required for
   the first release.
4. **Persist archived assets and rewrite feed URLs.** Store approved media in
   local or object storage so archived articles can remain useful when
   upstream assets change or disappear.
5. **Evaluate PGMQ as a queue transport optimization.** The current custom
   `jobs` table remains the version-one transport; PGMQ can be evaluated later
   if queue throughput or operational overhead becomes a demonstrated
   bottleneck.

The release image is built by `.github/workflows/container.yml`. Push a
semantic-version tag such as `v0.1.0` to build and publish
`ghcr.io/<owner>/<repository>:v0.1.0` and `:latest`; branch and pull-request
builds validate the Dockerfile without publishing. The image expects the same
environment variables described in [DEPLOYMENT.md](DEPLOYMENT.md), including
`DATABASE_URL`; no credentials are baked into the image.

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

This service boundary is implemented and tested. A deployment-specific
`CredentialRefresher` can be injected into `RuntimeSupervisor`; that enables
`credential_refresh` jobs in the worker plan and makes the runtime scheduler
enqueue one deduplicated refresh job for each active account within the
refresh window. The current
browser-backed source-sync runtime still uses a pre-authenticated browser
profile. See
[`AuthService`](src/application/auth_service.rs) and the
[authentication architecture](ARCHITECTURE.md#source-scheduling-and-account-leases).

### Single-admin panel and API

Enable the first-version administration surface with `ADMIN_ENABLED=true`,
`ADMIN_USERNAME`, `ADMIN_PASSWORD`, and an independent `SESSION_SIGNING_KEY`.
Open `/admin/login` and submit the configured credentials. A successful
`POST /api/admin/login` returns a short-lived `HttpOnly` session cookie and a
CSRF token; send that token as `X-CSRF-Token` on every state-changing admin
request. The panel is available at `/admin` and uses these API routes:

- `GET /api/admin/sources` and `POST /api/admin/sources` for source management;
- `POST /api/admin/sources/{id}/enabled` and `/gate` for operator controls;
- `POST /api/admin/sources/{id}/feed-token` to create/rotate a copyable feed
  link; and
- `GET /api/admin/sources/{id}/sync-runs` for synchronization history.

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
