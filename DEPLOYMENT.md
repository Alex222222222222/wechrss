# Browser Sidecar Deployment Notes

> **Implementation status:** the Rust binary now loads environment-only
> configuration, applies pending SQLx migrations, and composes the selected API,
> scheduler, feed-rebuild, and authenticated source-sync worker roles. Encrypted
> WeRead credential provisioning and lease-serialized non-interactive refresh
> are available as an application service. Administrators can enroll an
> already-issued WeRead web cookie header from the protected `/admin` panel;
> cookie and derived token values are encrypted before persistence and are
> never returned. A
> deployment-specific refresh
> transport can be injected into the runtime worker; QR/login exchange and
> browser health checks remain unfinished. The single-admin API and panel are
> available when explicitly enabled. API
> liveness/readiness endpoints are available. Source synchronization uses the
> enrolled cookie header for every authenticated WeRead request; the account ID
> is optional because accounts can be selected from the admin-enrolled records.

When browser-backed source synchronization is enabled, its worker and browser
sidecar must use the same IANA timezone for quiet-hours decisions and
browser-visible local time. `TZ` is configuration, not a substitute for
installing timezone data. The current database-only feed-rebuild worker and
API-only process do not require a browser sidecar.

## Docker image requirements

The Chromium/Firefox sidecar image must include `tzdata` and set `TZ` from the
deployment configuration:

```dockerfile
RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata \
    && rm -rf /var/lib/apt/lists/*

ENV TZ=Asia/Shanghai
```

The application container receives the same value:

```yaml
environment:
  APP_TIMEZONE: Asia/Shanghai
  LOG_LEVEL: warn
  QUIET_HOURS_START: "23:00"
  QUIET_HOURS_END: "07:00"
```

For version one, all application configuration is supplied through
environment variables; there is no application configuration file or CLI
override layer. In production, inject the timezone from one ConfigMap or
environment source instead of maintaining separate values for the application
and sidecar. Secrets such as database URLs and encryption keys belong in a
Kubernetes Secret.

## Werrss application image

The release image is published to GHCR by `.github/workflows/container.yml`
when a semantic-version tag such as `v0.1.7` is pushed. Pull the versioned
image with:

```sh
docker pull ghcr.io/<owner>/<repository>:v0.1.7
```

For a local build, run this from the repository root:

```sh
docker build --tag werrss:local .
```

The Dockerfile builds the Rust dependencies in a manifest-only layer, so
source-only changes reuse that work when the Docker/BuildKit cache is
available. GitHub Actions exports the same intermediate layers through its
GitHub Actions cache. The final image uses the non-root distroless C/C++
runtime and has no shell or package manager; use the HTTP probes and container
logs for operational diagnostics. The Rust binary embeds the IANA timezone
database through `chrono-tz`, so `tzdata` is required by the browser sidecar
but not by the application image.

The image listens on port `8080` by default and runs as an unprivileged user.
Provide `DATABASE_URL` and the other runtime settings through the deployment
environment; do not put credentials in a Dockerfile, image layer, or committed
configuration file. The image contains the application and CA data, but
browser-backed source synchronization still requires the separately
configured WebDriver sidecar described below. Its authenticated session can
come from the admin-enrolled cookie header.

The public feeds, health endpoints, and optional admin panel share the API
listener. Set its host and port with `HTTP_BIND` and `HTTP_PORT`:

```yaml
environment:
  HTTP_BIND: 0.0.0.0
  HTTP_PORT: "8080"
```

`HTTP_BIND` defaults to `0.0.0.0`; `HTTP_PORT` defaults to `8080` and must be a
non-zero port. There is no separate admin listener. For an IPv6 bind, provide
the raw address (for example, `::1`); the application formats the socket
address correctly before binding.

The API role is executable by itself. Scheduler startup does not require a
WeRead account: source-sync workers are composed even before enrollment, and a
source-sync job records a warning and a scheduled failure when no usable
account exists. The source is then considered again on its next scheduling
interval. `APP_ROLES=all` can therefore start before the administrator fills
the WeRead authentication form.
`SERVER_ROOT_URL` must be set to the public HTTP(S) root URL that generated RSS
channels and admin-generated feed links should use. Browser-session capacity and
worker replica count must be intentional; increasing API replicas for RSS traffic
must not automatically increase upstream fetch concurrency.

### Authenticated WeRead source synchronization

The endpoint is always configured for source-sync acquisition. An account ID
is optional when using an admin-enrolled account:

```text
WEREAD_ARTICLE_LIST_URL=https://weread.qq.com/api/mp/cover
```

`WEREAD_ACCOUNT_ID` remains available as the default panel-enrolled account.
When it is unset, each source-sync job
randomly selects a non-disabled, unexpired account from PostgreSQL; a
source-specific account relationship takes precedence. If no usable account is enrolled, the
job fails with a warning and the source is left for its next scheduling
interval. Enroll or replace credentials later from the protected admin panel;
the running worker observes the account on a subsequent job without a restart.

Enroll the cookie header from the admin panel; the runtime injects it into a
fresh authenticated browser session, revisits `https://weread.qq.com/web/shelf`
after injection, and only fetches the article endpoint when that same session
remains on the shelf rather than being redirected to login. The runtime holds a PostgreSQL account
lease while the authenticated request is active and releases it before
opening a clean public session for article content. Authentication material is
never sent to public WeChat pages. The configured WeRead article
URL is restricted by configuration validation to the HTTPS
`weread.qq.com/api/mp/cover` or `weread.qq.com/web/mp/articles` endpoint without
credentials, fragments, or a non-default port. The cover endpoint is the
recommended default because the older article-list endpoint may return a
deprecated response for current accounts.

The admin panel provides the non-interactive credential enrollment flow: after
signing in at `/admin/login`, copy the complete `Cookie` request-header value
from a successful `/web/mp/articles` request in a desktop browser, then paste
it into the WeRead authentication form with its RFC3339 access-session expiry.
The display name may be left empty when the cookie contains `wr_name`; Werrss
percent-decodes that cookie value and uses it as the display name. The response
contains only the account UUID and status metadata. Use that UUID
as a source's `account_id` when a source must stay pinned to one account; leave
the field empty to randomly use any enabled, unexpired enrolled account. To
rotate a session, submit the new cookie using the same account ID. QR-code
login remains deferred, and no cookie should be placed in a ConfigMap or
container image.

#### Authenticated browser diagnostic

The ignored `real_weread` integration test exercises the same shelf-first
authenticated adapter used by source synchronization. It creates a standard
Firefox session, visits `/web/shelf`, installs the cookie in the WeRead origin,
revisits the shelf, and fetches the configured article endpoint as raw text. It does not modify
`navigator.webdriver`, inject stealth code, or spoof browser fingerprints. Keep
the cookie in a secret manager or a private ignored environment source; never
commit it, put it in a Kubernetes ConfigMap, or include it in shell history or
test output.

After forwarding a temporary Firefox WebDriver service to local port `4444`,
run it with the cookie supplied by your secret manager:

```sh
WEBDRIVER_URL=http://127.0.0.1:4444 \
WEREAD_COOKIE_HEADER="$(your-secret-manager read weread-cookie)" \
WEREAD_BOOK_ID=MP_WXS_2103095721 \
  cargo test --locked --test real_weread -- --ignored --test-threads=1 --nocapture
```

The test defaults to Firefox and the public target book above. Set
`BROWSER_ENGINE=chromium` only to compare a normal Chromium session; the test
does not attempt to conceal WebDriver automation. The existing
`BROWSER_USER_AGENT`, `BROWSER_VIEWPORT_WIDTH`, `BROWSER_VIEWPORT_HEIGHT`,
`BROWSER_LOCALE`, `APP_TIMEZONE`, and `BROWSER_EXTRA_ARGS` variables can be used
for controlled diagnostics subject to the profile validation rules. A
successful empty list is a valid upstream response; an authentication or
environment-control error should be treated as an upstream session rejection,
not bypassed.

## Kubernetes

### Sample application deployment

`k8s/sample/deployment.yaml` is a portable example with the Werrss API and an
internal Selenium standalone Firefox WebDriver sidecar. The application uses
the pod loopback address (`http://127.0.0.1:4444`) to reach Firefox; the
WebDriver port is not exposed by the ClusterIP Service. The sample runs both
containers as non-root, gives Firefox an in-memory `/dev/shm`, and uses
`/api/health` for Werrss liveness, `/api/ready` for PostgreSQL-backed Werrss
readiness, and `/status` for WebDriver health. Before applying it, create the
referenced `werrss-runtime` Secret with at least these keys using your
secret-management process:

```text
DATABASE_URL
CREDENTIAL_ENCRYPTION_KEY
```

The sample deliberately leaves the Secret out of the repository. It uses
`APP_ROLES=api` so the deployment is safe to start without configured WeRead
credentials; add the browser, WeRead, worker, and scheduler settings from the
environment-variable reference before selecting additional roles. Pin the
Firefox image to an image digest for production, and replace
either image reference when deploying a fork or a different release:

```sh
kubectl apply -f k8s/sample/deployment.yaml
kubectl rollout status deployment/werrss --timeout=180s
```

### Development PostgreSQL in Kubernetes

The repository's development PostgreSQL manifest is
`k8s/dev/postgres.yaml`. It creates a single-replica, ephemeral PostgreSQL
Deployment and ClusterIP Service in the `dev` namespace. It has no persistent
volume and is for integration testing only; deleting or recreating the Pod
loses its data. Keep cluster contexts, node names, endpoints, and credentials
outside the repository.

Create the namespace and its development-only credentials, then apply the
namespaced workload:

```sh
kubectl create namespace dev
kubectl -n dev create secret generic werrss-postgres-dev \
  --from-literal=POSTGRES_DB=werrss \
  --from-literal=POSTGRES_USER=werrss \
  --from-literal=POSTGRES_PASSWORD=werrss-dev-only
kubectl apply -f k8s/dev/postgres.yaml
kubectl -n dev rollout status deployment/werrss-postgres-dev --timeout=180s
```

If the namespace or Secret already exists, use the idempotent forms below;
the Secret command does not print the password:

```sh
kubectl get namespace dev >/dev/null 2>&1 || kubectl create namespace dev
kubectl -n dev create secret generic werrss-postgres-dev \
  --from-literal=POSTGRES_DB=werrss \
  --from-literal=POSTGRES_USER=werrss \
  --from-literal=POSTGRES_PASSWORD=werrss-dev-only \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f k8s/dev/postgres.yaml
kubectl -n dev rollout status deployment/werrss-postgres-dev --timeout=180s
```

Port-forward only the `dev` Service to a local port and run the SQLx test
harness. `DATABASE_URL` must point to the administrative database connection;
`#[sqlx::test]` creates an isolated temporary database and applies the checked-
in migrations automatically:

```sh
kubectl -n dev port-forward service/werrss-postgres-dev 55432:5432
DATABASE_URL='postgresql://werrss:werrss-dev-only@127.0.0.1:55432/werrss' \
  cargo test --locked --test postgres_job_repository -- --nocapture
```

Remove the development-only resources when testing is complete:

```sh
kubectl -n dev delete -f k8s/dev/postgres.yaml
kubectl -n dev delete secret werrss-postgres-dev
```

Delete the namespace only if it was created solely for this test and contains
no unrelated workloads:

```sh
kubectl delete namespace dev
```

Do not use these credentials, the ephemeral storage policy, or this
port-forward workflow for production. Production PostgreSQL SSL, certificate,
private-key, password, and related connection options remain in `DATABASE_URL`
and its query parameters.

For a sidecar in the same Pod, set `TZ` and install `tzdata` in the browser
image. Do not rely on the node timezone. The application should expose a
readiness diagnostic that reports its configured timezone and the browser
session should verify the browser-visible timezone during session setup.

The WebDriver port remains Pod-internal. NetworkPolicy should prevent external
clients from reaching it. Asset storage requires persistent volumes only when
local asset storage is selected. Public article extraction uses a clean
ephemeral profile and never receives account credentials.

API readiness is exposed at `/api/ready` and requires PostgreSQL; it does not
fail solely because WebDriver is unavailable, allowing persisted RSS feeds to
remain serviceable. Browser and
browser-timezone health are exposed as degraded component status and stop
workers from claiming browser jobs. A worker-only process may make browser
availability part of its own readiness condition. Liveness is exposed at
`/api/health` and is a local process check.

The first usable version includes a small authenticated admin panel for source
management, synchronization status, feed-link copying, and safe error states.
It has one administrator configured through `ADMIN_USERNAME` and
`ADMIN_PASSWORD`; user management is out of scope. Enable it with
`ADMIN_ENABLED=true`, an independent `SESSION_SIGNING_KEY`, and expose it only
through the deployment's TLS-protected ingress. `/admin/login` starts the
non-interactive login flow; successful API login returns a CSRF token for
state-changing requests.
Interactive QR-code login is deferred until after the first release because it
requires user interaction and a dedicated login-attempt lifecycle. A durable
queue and handler for articles missed during synchronization is also deferred;
it is a post-release repair/backfill improvement rather than a first-release
requirement.

## Timezone verification

The browser adapter evaluates the browser's local timezone and
compare it with the configured IANA timezone. A mismatch is a configuration
error, not a reason to silently continue. This catches images that have `TZ`
set but lack usable timezone data or browser-specific timezone configuration.

PostgreSQL server time is authoritative for distributed leases, due-job checks,
and lease recovery. Production monitoring should alert on database clock offset,
but an application Pod's wall-clock skew must not cause it to expire another
worker's lease. Application/browser timezone configuration remains necessary for
quiet-hours presentation and browser behavior; it does not replace the
PostgreSQL clock used by lease SQL.

## Quiet-hours behavior

Quiet hours block new upstream requests, page navigations, and scroll/fetch
operations. They do not block RSS reads, cached feed responses, or local
PostgreSQL maintenance. A job that crosses into quiet hours may finish its
current bounded operation and then enter the durable non-failure `deferred`
state until the next eligible instant. Deferral does not consume its retry
failure budget.
