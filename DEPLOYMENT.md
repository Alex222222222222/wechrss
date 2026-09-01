# Browser Sidecar Deployment Notes

> **Implementation status:** the Rust binary now loads environment-only
> configuration, applies pending SQLx migrations, and composes the selected
> API and database-only feed-rebuild worker roles. Scheduler startup is rejected
> until concrete authenticated source acquisition and browser dependencies are
> composed. The injected-port synchronization finalization handler is covered
> by tests, but authenticated transport, administrative routes, and browser
> health checks are still not executable, so deployments must not enable those
> unfinished capabilities.

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

The currently executable role sets are `APP_ROLES=api`, `worker`, or
`api,worker`. `scheduler` and `all` fail startup until concrete source-sync
acquisition is composed, because the current runtime worker cannot consume
source-sync jobs.
`RSS_FEED_URL` must be set to the public HTTP(S) URL that generated RSS
channels should advertise. Browser-session capacity and worker replica count
must be intentional; increasing API replicas for RSS traffic must not
automatically increase upstream fetch concurrency.

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

API readiness requires PostgreSQL but does not fail solely because WebDriver is
unavailable, allowing persisted RSS feeds to remain serviceable. Browser and
browser-timezone health are exposed as degraded component status and stop
workers from claiming browser jobs. A worker-only process may make browser
availability part of its own readiness condition. Liveness remains a local
process check.

## Timezone verification

The future browser adapter should evaluate the browser's local timezone and
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
