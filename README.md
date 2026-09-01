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
