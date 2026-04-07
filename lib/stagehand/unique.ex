defmodule Stagehand.Unique do
  @moduledoc """
  Unique job deduplication using a local ETS table.

  Each node owns its own ETS table. Consistent hashing ensures the same
  unique job always routes to the same node, so the dedup check is local.
  """

  use GenServer

  @prune_interval 30_000
  @prune_max_age 60

  defstruct [:table]

  def start_link(opts) do
    name = opts[:name]
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Generate a fingerprint for a job based on its unique configuration.
  """
  @spec fingerprint(Stagehand.Job.t()) :: non_neg_integer() | nil
  def fingerprint(%Stagehand.Job{unique: nil}), do: nil

  def fingerprint(%Stagehand.Job{unique: unique_opts} = job) do
    fields = Keyword.get(unique_opts, :fields, [:worker, :queue, :args])
    keys = Keyword.get(unique_opts, :keys, [])

    data =
      Enum.map(fields, fn
        :worker -> to_string(job.worker)
        :queue -> job.queue
        :args -> filter_keys(job.args, keys)
        :meta -> filter_keys(job.meta, keys)
      end)

    :erlang.phash2(data)
  end

  @doc """
  Check for an existing job with the same fingerprint. If none found,
  insert the fingerprint. Returns `{:ok, job}` or `{:conflict, existing}`.
  """
  @spec check_and_insert(GenServer.server(), non_neg_integer(), Stagehand.Job.t()) ::
          {:ok, Stagehand.Job.t()} | {:conflict, Stagehand.Job.t()}
  def check_and_insert(server, fingerprint, job) do
    GenServer.call(server, {:check_and_insert, fingerprint, job})
  end

  @doc """
  Remove a fingerprint entry (called when a unique job completes and
  its period has expired).
  """
  @spec remove(GenServer.server(), non_neg_integer()) :: :ok
  def remove(server, fingerprint) do
    GenServer.cast(server, {:remove, fingerprint})
  end

  @doc """
  Prune expired entries older than `max_age` seconds.
  """
  @spec prune(GenServer.server(), pos_integer()) :: :ok
  def prune(server, max_age) do
    GenServer.cast(server, {:prune, max_age})
  end

  @doc """
  Export all entries from this server. Returns a list of
  `{fingerprint, job, inserted_at}` tuples.
  """
  @spec export(GenServer.server()) :: [{non_neg_integer(), Stagehand.Job.t(), integer()}]
  def export(server) do
    GenServer.call(server, :export)
  end

  @doc """
  Import entries into this server. Existing entries are not overwritten.
  """
  @spec import(GenServer.server(), [{non_neg_integer(), Stagehand.Job.t(), integer()}]) :: :ok
  def import(server, entries) do
    GenServer.call(server, {:import, entries})
  end

  # -- Callbacks --

  @impl true
  def init(_opts) do
    table = :ets.new(:stagehand_unique, [:set, :protected, read_concurrency: true])
    Process.send_after(self(), :prune, @prune_interval)
    {:ok, %__MODULE__{table: table}}
  end

  @impl true
  def handle_call({:check_and_insert, fingerprint, job}, _from, state) do
    unique_opts = job.unique || []
    period = Keyword.get(unique_opts, :period, 60)
    states = Keyword.get(unique_opts, :states, [:available, :executing, :scheduled, :retryable])

    case :ets.lookup(state.table, fingerprint) do
      [{^fingerprint, existing_job, inserted_at}] ->
        if within_period?(inserted_at, period) and existing_job.state in states do
          {:reply, {:conflict, existing_job}, state}
        else
          :ets.insert(state.table, {fingerprint, job, System.monotonic_time(:second)})
          {:reply, {:ok, job}, state}
        end

      [] ->
        :ets.insert(state.table, {fingerprint, job, System.monotonic_time(:second)})
        {:reply, {:ok, job}, state}
    end
  end

  def handle_call(:export, _from, state) do
    entries = :ets.tab2list(state.table)
    {:reply, entries, state}
  end

  def handle_call({:import, entries}, _from, state) do
    for {fingerprint, job, inserted_at} <- entries do
      # Don't overwrite existing entries
      case :ets.lookup(state.table, fingerprint) do
        [] -> :ets.insert(state.table, {fingerprint, job, inserted_at})
        _ -> :ok
      end
    end

    {:reply, :ok, state}
  end

  @impl true
  def handle_cast({:remove, fingerprint}, state) do
    :ets.delete(state.table, fingerprint)
    {:noreply, state}
  end

  def handle_cast({:prune, max_age}, state) do
    do_prune(state.table, max_age)
    {:noreply, state}
  end

  @impl true
  def handle_info(:prune, state) do
    do_prune(state.table, @prune_max_age)
    Process.send_after(self(), :prune, @prune_interval)
    {:noreply, state}
  end

  # -- Private --

  defp within_period?(_inserted_at, :infinity), do: true

  defp within_period?(inserted_at, period) do
    now = System.monotonic_time(:second)
    now - inserted_at < period
  end

  defp filter_keys(map, []), do: map
  defp filter_keys(map, keys), do: Map.take(map, keys)

  defp do_prune(table, max_age) do
    now = System.monotonic_time(:second)

    :ets.foldl(
      fn {fingerprint, _job, inserted_at}, acc ->
        if now - inserted_at > max_age, do: :ets.delete(table, fingerprint)
        acc
      end,
      :ok,
      table
    )
  end
end
