# syntax=docker/dockerfile:1.7

FROM rust:1-bookworm AS builder

WORKDIR /usr/src/werrss

# Keep this layer dependent only on the manifests so source-only changes can
# reuse the compiled dependency artifacts from the BuildKit/GHA cache.
COPY Cargo.toml Cargo.lock ./

RUN mkdir src \
    && printf 'pub fn dependency_cache_probe() {}\n' > src/lib.rs \
    && printf 'fn main() {}\n' > src/main.rs \
    && cargo build --locked --release \
    && rm -rf src

# SQLx embeds the checked-in migrations at compile time.
COPY migrations ./migrations
COPY src ./src

RUN cargo build --locked --release \
    && cp target/release/werrss /tmp/werrss

FROM gcr.io/distroless/cc-debian12:nonroot

WORKDIR /app

COPY --link --from=builder /tmp/werrss /usr/local/bin/werrss

ENV HTTP_BIND=0.0.0.0 \
    HTTP_PORT=8080 \
    TZ=UTC

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/werrss"]
