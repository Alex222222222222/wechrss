# Migration design notes

Applied SQLx migrations are immutable because SQLx records their checksums.
Do not edit `0001_jobs.sql` to implement later job-state changes.

TODO(design): the next forward migration updates the existing job contract. It:

- adds the active non-failure `deferred` job state;
- separates durable `claim_count` from retry-budget `failure_count`;
- includes `deferred` in the active deduplication and claim indexes;
- updates constraints so claims do not consume the failure budget; and
- leaves all existing terminal rows terminal.

The legacy `attempts` value is copied to `claim_count`. For active rows,
`failure_count` is backfilled from completed prior claims: `running` uses
`greatest(attempts - 1, 0)`, while `queued` or `retry_wait` rows that have already
been claimed use `attempts`. This preserves known crash/retry failures without
counting the current running claim as failed. Upgrade tests must cover every
legacy status before the old column is removed.

This is an expand/contract rollout, not a one-step destructive migration:

1. expand the table with compatible counter columns/defaults, extend status
   constraints and indexes, and backfill while retaining `attempts`;
2. deploy code that reads the new counters and dual-writes any field still
   needed by an old replica;
3. enable creation/claiming of `deferred` only after all worker replicas
   understand it; and
4. remove legacy `attempts` and dual-write compatibility in a later release
   after verification.

Migration tests must exercise both a clean database and an existing `0001`
database. Rolling-upgrade tests must prove old and new replicas can coexist
during the expand phase without losing active-job deduplication or leases.

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
