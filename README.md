# Stagehand

GenStage-based background job processing for Elixir. In-memory, no database required.

## Why

Most job processing libraries require a database. Stagehand doesn't. It's built on GenStage and runs entirely in-memory, making it a good fit for applications that need background job processing without the overhead of external dependencies.

## Guarantees

- **Graceful shutdown** — executing jobs complete before the node stops. The producer drains in-flight work within a configurable grace period.
- **No new work during shutdown** — the producer snapshots the current cluster membership, then leaves the pg group so no new jobs are routed to it. Any messages already in the mailbox are drained before redistribution.
- **Job redistribution** — on shutdown, scheduled, queued, and in-flight jobs are redistributed to surviving producers on other nodes. On a single-node deploy, these jobs are lost.
- **At-most-once delivery** — each job runs at most once. Jobs are in-memory with no persistence, so a VM crash loses queued, scheduled, and executing jobs.
- **Unique jobs (best effort)** — deduplication is backed by a local ETS table. A consistent hash ring routes the same job fingerprint to the same producer. When a node joins or leaves, the ring only remaps keys that belong to the changed node — all other fingerprints stay on their current owner, keeping their dedup state intact. On graceful shutdown, the producer snapshots cluster membership, leaves the group, then transfers dedup entries to their new owners using the snapshot. When a new node joins, unique checks are blocked until all existing producers have synced their entries, preventing duplicates during the transition. On crashes, entries on the lost node are gone and duplicates are possible until the uniqueness period expires.

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

Add Stagehand to your supervision tree:

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

Insert jobs:

```elixir
%{"to" => "user@example.com", "body" => "hello"}
|> MyApp.EmailWorker.new()
|> Stagehand.insert()
```

### Return values

- `:ok` or `{:ok, value}` — job succeeded
- `{:error, reason}` — job failed, will retry if attempts remain
- `{:snooze, seconds}` — re-enqueue after delay
- `{:cancel, reason}` — stop, no more retries

### Options

- `:queue` — queue name (default `:default`)
- `:max_attempts` — retry limit (default `20`)
- `:priority` — 0-9, lower is higher (default `0`)
- `:unique` — uniqueness config or `false`
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
