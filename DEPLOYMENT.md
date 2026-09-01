# Browser Sidecar Deployment Notes

> **Implementation status:** the Rust binary now loads environment-only
> configuration, applies pending SQLx migrations, and composes the selected API,
> scheduler, feed-rebuild, and authenticated source-sync worker roles. Encrypted
> WeRead credential provisioning and lease-serialized non-interactive refresh
> are available as an application service. A deployment-specific refresh
> transport can be injected into the runtime worker; QR/login exchange and
> browser health checks remain unfinished. The single-admin API and panel are
> available when explicitly enabled. API
> liveness/readiness endpoints are available. Source
> synchronization still requires a pre-authenticated browser profile and
> WeRead account ID.

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
  QUIET_HOURS_START: "23:00"
  QUIET_HOURS_END: "07:00"
```

For version one, all application configuration is supplied through
environment variables; there is no application configuration file or CLI
override layer. In production, inject the timezone from one ConfigMap or
environment source instead of maintaining separate values for the application
and sidecar. Secrets such as database URLs and encryption keys belong in a
Kubernetes Secret.

## WechRss application image

The release image is published to GHCR by `.github/workflows/container.yml`
when a semantic-version tag such as `v0.1.0` is pushed. Pull the versioned
image with:

```sh
docker pull ghcr.io/<owner>/<repository>:0.1.0
```

For a local build, run this from the repository root:

```sh
docker build --tag wechrss:local .
```

The image listens on port `8080` by default and runs as an unprivileged user.
Provide `DATABASE_URL` and the other runtime settings through the deployment
environment; do not put credentials in a Dockerfile, image layer, or committed
configuration file. The image contains the application and timezone/CA data,
but browser-backed source synchronization still requires the separately
configured WebDriver sidecar described below.

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

The API role is executable by itself. The scheduler role and source-sync worker
dispatch require the authenticated settings below; without them, a worker
remains feed-rebuild-only and scheduler startup fails closed rather than
creating jobs that no worker can execute. `APP_ROLES=all` therefore requires
the same settings.
`RSS_FEED_URL` must be set to the public HTTP(S) URL that generated RSS
channels should advertise. Browser-session capacity and worker replica count
must be intentional; increasing API replicas for RSS traffic must not
automatically increase upstream fetch concurrency.

### Authenticated WeRead source synchronization

Set these values together to enable source-sync acquisition:

```text
BROWSER_AUTHENTICATED_PROFILE=/path/visible-to-the-browser-sidecar
WEREAD_ACCOUNT_ID=<stable-account-uuid>
WEREAD_ARTICLE_LIST_URL=https://i.weread.qq.com/web/mp/articles
```

The profile must already contain an authorized WeRead session and must be
mounted into the browser sidecar at the configured path. The runtime uses it
only for the authenticated article-list request, holds a PostgreSQL account
lease while that request is active, and releases the lease before opening a
clean public session for article content. It does not perform login, persist
credentials, or send the authenticated profile to public WeChat pages. The
article-list URL is restricted by configuration validation to the exact HTTPS
`i.weread.qq.com/web/mp/articles` endpoint without credentials, fragments, or a
non-default port. If either of the first two values is missing, source-sync
composition is disabled and scheduler startup is rejected.

## Kubernetes

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
kubectl -n dev create secret generic wechrss-postgres-dev \
  --from-literal=POSTGRES_DB=wechrss \
  --from-literal=POSTGRES_USER=wechrss \
  --from-literal=POSTGRES_PASSWORD=wechrss-dev-only
kubectl apply -f k8s/dev/postgres.yaml
kubectl -n dev rollout status deployment/wechrss-postgres-dev --timeout=180s
```

If the namespace or Secret already exists, use the idempotent forms below;
the Secret command does not print the password:

```sh
kubectl get namespace dev >/dev/null 2>&1 || kubectl create namespace dev
kubectl -n dev create secret generic wechrss-postgres-dev \
  --from-literal=POSTGRES_DB=wechrss \
  --from-literal=POSTGRES_USER=wechrss \
  --from-literal=POSTGRES_PASSWORD=wechrss-dev-only \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f k8s/dev/postgres.yaml
kubectl -n dev rollout status deployment/wechrss-postgres-dev --timeout=180s
```

Port-forward only the `dev` Service to a local port and run the SQLx test
harness. `DATABASE_URL` must point to the administrative database connection;
`#[sqlx::test]` creates an isolated temporary database and applies the checked-
in migrations automatically:

```sh
kubectl -n dev port-forward service/wechrss-postgres-dev 55432:5432
DATABASE_URL='postgresql://wechrss:wechrss-dev-only@127.0.0.1:55432/wechrss' \
  cargo test --locked --test postgres_job_repository -- --nocapture
```

Remove the development-only resources when testing is complete:

```sh
kubectl -n dev delete -f k8s/dev/postgres.yaml
kubectl -n dev delete secret wechrss-postgres-dev
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
clients from reaching it. Browser profile and asset storage require persistent
volumes only when session persistence or local asset storage is selected.
Public article extraction uses a clean ephemeral profile and never reuses the
authenticated account profile.

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
