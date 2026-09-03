# syntax=docker/dockerfile:1.7

FROM rust:1-bookworm AS builder

WORKDIR /usr/src/werrss

# Keep dependency downloads independent from source changes without creating a
# fake package binary that could accidentally be copied into the runtime image.
COPY Cargo.toml Cargo.lock ./

RUN --mount=type=cache,id=werrss-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=werrss-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    cargo fetch --locked

# SQLx embeds the checked-in migrations at compile time.
COPY migrations ./migrations
COPY src ./src

RUN --mount=type=cache,id=werrss-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=werrss-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=werrss-cargo-target,target=/usr/src/werrss/target,sharing=locked \
    <<'EOF'
set -eu
cargo build --locked --release --bin werrss

# A missing DATABASE_URL must make the real executable fail loudly. This
# catches an accidentally cached no-op binary before it can be published.
set +e
target/release/werrss >/tmp/werrss-startup-check.log 2>&1
status=$?
set -e
test "$status" -ne 0
test -s /tmp/werrss-startup-check.log

cp target/release/werrss /tmp/werrss
EOF

FROM gcr.io/distroless/cc-debian12:nonroot

WORKDIR /app

COPY --link --from=builder /tmp/werrss /usr/local/bin/werrss

ENV HTTP_BIND=0.0.0.0 \
    HTTP_PORT=8080 \
    TZ=UTC

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/werrss"]
