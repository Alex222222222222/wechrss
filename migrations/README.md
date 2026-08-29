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
- source identity/configuration, scheduling state, gates, cooldowns,
  reservations, and feed revision used to fence feed publication and
  atomically enqueue due work;
- an optional stable `account_id` relationship for authenticated list
  acquisition; the credential/account table and foreign key are deferred until
  that repository contract is implemented;
- revision-aware `feed_cache` rows with a source foreign key;
- fenced `account_leases` for authenticated-account serialization;
- fenced `feed_build_leases` for per-source cache-build single-flight;
- normalized `articles` keyed by `(source_id, review_id)`, including sanitized
  HTML, optional external URLs, the pre-acquisition observation version, and
  the feed-order index; and
- `sync_runs` audit rows with typed outcomes, bounded counters, safe failure
  summaries, and optional published feed revisions.

There is no legacy `attempts` column or compatibility trigger. When a release
has been published, later changes must use a new forward migration and must not
edit `0001_jobs.sql`.

Later forward migrations are added only as their executable repository contract
is implemented. They will:

- add feed-token metadata and any later source lifecycle fields;
- add credential and archive tables; and
- extend the feed-cache rows only if the final RSS contract needs fields beyond
  the current XML/ETag/revision payload.

Every migration is covered by PostgreSQL upgrade tests as well as clean-database
`#[sqlx::test]` tests.
