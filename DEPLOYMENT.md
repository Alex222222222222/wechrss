# Browser Sidecar Deployment Notes

The Rust application and its browser sidecar must use the same IANA timezone
for quiet-hours decisions and browser-visible local time. `TZ` is configuration,
not a substitute for installing timezone data.

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

## Kubernetes

For a sidecar in the same Pod, set `TZ` and install `tzdata` in the browser
image. Do not rely on the node timezone. The application should expose a
readiness diagnostic that reports its configured timezone and the browser
session should verify the browser-visible timezone during session setup.

The WebDriver port remains Pod-internal. NetworkPolicy should prevent external
clients from reaching it. Browser profile and asset storage require persistent
volumes only when session persistence or local asset storage is selected.

## Timezone verification

The future browser adapter should evaluate the browser's local timezone and
compare it with the configured IANA timezone. A mismatch is a configuration
error, not a reason to silently continue. This catches images that have `TZ`
set but lack usable timezone data or browser-specific timezone configuration.

## Quiet-hours behavior

Quiet hours block new upstream requests, page navigations, and scroll/fetch
operations. They do not block RSS reads, cached feed responses, or local
PostgreSQL maintenance. A job that crosses into quiet hours may finish its
current bounded operation and then return a resumable result before the next
upstream operation.
