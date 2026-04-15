# Stagehand

[![CI](https://github.com/twinn/stagehand/actions/workflows/ci.yml/badge.svg)](https://github.com/twinn/stagehand/actions/workflows/ci.yml)
[![Hex.pm](https://img.shields.io/hexpm/v/stagehand.svg)](https://hex.pm/packages/stagehand)
[![Docs](https://img.shields.io/badge/hex-docs-blue.svg)](https://hexdocs.pm/stagehand)
[![License](https://img.shields.io/hexpm/l/stagehand.svg)](https://github.com/twinn/stagehand/blob/main/LICENSE)

An in-memory, GenStage-based background job processing library for Elixir.

Stagehand runs entirely in-memory with no database dependency. It is
built on GenStage and uses `PgRegistry` for cluster-wide producer
discovery and `Highlander` for singleton scheduling.

## Guarantees

- **Graceful shutdown** — executing jobs complete before the node stops.
  The producer drains in-flight work within a configurable grace period.
- **No new work during shutdown** — the producer leaves the cluster
  registry so no new jobs are routed to it. Any messages already in the
  mailbox are drained before redistribution.
- **Job redistribution** — on shutdown, scheduled, queued, and in-flight
  jobs are redistributed to surviving producers. If no surviving
  producers exist, these jobs are lost.
- **At-most-once delivery** — each job runs at most once. Jobs are
  in-memory with no persistence; a VM crash loses queued, scheduled,
  and executing jobs.
- **Unique jobs (best effort)** — deduplication is backed by an ETS
  table per node. Rendezvous hashing routes the same job fingerprint to
  the same node, and the dedup check runs on that node's Unique server.
  On graceful shutdown, dedup entries are transferred to surviving
  nodes. On node join, unique checks are blocked until all existing
  producers have synced their entries. On crashes, entries on the lost
  node are gone and duplicates are possible until the uniqueness period
  expires.

## Installation

```elixir
def deps do
  [
    {:stagehand, "~> 0.1.0"}
  ]
end
```

## Configuration

```elixir
# config/config.exs
config :my_app, Stagehand,
  queues: [default: 10, mailers: 20],
  plugins: [
    {Stagehand.Plugins.Cron, crontab: [
      {"* * * * *", MyApp.MinuteWorker},
      {"@daily", MyApp.DailyWorker}
    ]}
  ]

# config/test.exs
config :my_app, Stagehand, testing: :manual
```

Add Stagehand to the application supervision tree:

```elixir
children = [
  {Stagehand, otp_app: :my_app}
]
```

## Workers

```elixir
defmodule MyApp.EmailWorker do
  use Stagehand.Worker, queue: :mailers, max_attempts: 5

  @impl true
  def perform(%Stagehand.Job{args: %{"to" => to, "body" => body}}) do
    MyApp.Mailer.send(to, body)
    :ok
  end
end
```

Inserting jobs:

```elixir
%{"to" => "user@example.com", "body" => "hello"}
|> MyApp.EmailWorker.new()
|> Stagehand.insert()
```

### Return values

- `:ok` or `{:ok, value}` — job succeeded
- `{:error, reason}` — job failed, retries if attempts remain
- `{:snooze, seconds}` — re-enqueue after delay
- `{:cancel, reason}` — stop, no more retries

### Options

- `:queue` — queue name (default `:default`)
- `:max_attempts` — retry limit (default `20`)
- `:priority` — 0-9, lower is higher priority (default `0`)
- `:unique` — uniqueness configuration or `false`
- `:schedule_in` — delay in seconds or `{amount, :seconds | :minutes | :hours | :days}`
- `:scheduled_at` — specific `DateTime`

## Testing

```elixir
# config/test.exs
config :my_app, Stagehand, testing: :manual
```

```elixir
Stagehand.Testing.assert_enqueued(Stagehand, worker: MyApp.EmailWorker)
Stagehand.Testing.refute_enqueued(Stagehand, worker: MyApp.OtherWorker)
Stagehand.Testing.perform_job(MyApp.EmailWorker, %{"to" => "test@example.com"})
```

## License

MIT
