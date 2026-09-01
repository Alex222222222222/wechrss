# syntax=docker/dockerfile:1

FROM rust:1-bookworm AS builder

WORKDIR /usr/src/wechrss

# SQLx embeds the checked-in migrations at compile time.
COPY Cargo.toml Cargo.lock ./
COPY migrations ./migrations
COPY src ./src

RUN cargo build --locked --release

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home --shell /usr/sbin/nologin wechrss

WORKDIR /app

COPY --from=builder /usr/src/wechrss/target/release/wechrss /usr/local/bin/wechrss

ENV HTTP_BIND=0.0.0.0 \
    HTTP_PORT=8080 \
    TZ=UTC

USER wechrss
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/wechrss"]
