# Changelog

## 0.1.0

Initial release.

- GenStage-based producer/consumer pipeline with per-queue concurrency
  limits and priority dispatch
- Cluster-wide producer discovery via `PgRegistry`
- Job routing: random for normal jobs, rendezvous hashing for unique jobs
- Unique job deduplication with local ETS tables and cross-node entry
  transfer on topology changes
- Graceful shutdown with job redistribution to surviving producers
- Cron scheduling via `Highlander` for cluster-wide singleton execution
- Telemetry events for job lifecycle and queue shutdown
- Testing helpers with `:manual` and `:inline` modes
- Exponential backoff with jitter for retries
- Queue pause/resume across the cluster
