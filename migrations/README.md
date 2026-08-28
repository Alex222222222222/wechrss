# Migration design notes

Applied SQLx migrations are immutable after publication because SQLx records
their checksums. This project has not published a release yet, so `0001_jobs.sql`
is kept as the complete initial job schema rather than introducing a temporary
rolling-upgrade migration.

The initial schema provides:

- the active non-failure `deferred` job state;
- separate durable `claim_count` and retry-budget `failure_count` values;
- active deduplication and claim indexes that include `deferred`; and
- constraints that prevent claims from consuming the failure budget.

There is no legacy `attempts` column or compatibility trigger. When a release
has been published, later changes must use a new forward migration and must not
edit `0001_jobs.sql`.

Later forward migrations are added only as their executable repository contract
is implemented. They will:

- add source scheduling gates, failure cooldown/reservation fields, stable
  account relationships, and monotonic `feed_revision`;
- create fenced `account_leases`;
- create revision-aware `feed_cache` plus a fenced `feed_build_leases` table for
  first-cache/rebuild single-flight without connection-bound advisory locks; and
- add article, sync-run, credential, and source tables.

Every migration is covered by PostgreSQL upgrade tests as well as clean-database
`#[sqlx::test]` tests.
