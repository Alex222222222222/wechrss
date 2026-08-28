# Migration design notes

Applied SQLx migrations are immutable after publication because SQLx records
their checksums. This project has not published a release yet, so `0001_jobs.sql`
is kept as the complete initial coordination schema rather than introducing a
temporary rolling-upgrade migration.

The initial schema provides:

- the active non-failure `deferred` job state;
- separate durable `claim_count` and retry-budget `failure_count` values;
- active deduplication and claim indexes that include `deferred`; and
- constraints that prevent claims from consuming the failure budget;
- the minimal `sources` identity/revision row used to fence feed publication;
- revision-aware `feed_cache` rows with a source foreign key; and
- fenced `account_leases` for authenticated-account serialization; and
- fenced `feed_build_leases` for per-source cache-build single-flight.

There is no legacy `attempts` column or compatibility trigger. When a release
has been published, later changes must use a new forward migration and must not
edit `0001_jobs.sql`.

Later forward migrations are added only as their executable repository contract
is implemented. They will:

- add source scheduling gates, failure cooldown/reservation fields, stable
  account relationships, and the remaining source configuration columns;
- add article, sync-run, credential, and archive tables; and
- extend the feed-cache rows only if the final RSS contract needs fields beyond
  the current XML/ETag/revision payload.

Every migration is covered by PostgreSQL upgrade tests as well as clean-database
`#[sqlx::test]` tests.
