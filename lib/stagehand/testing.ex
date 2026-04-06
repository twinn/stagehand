defmodule Stagehand.Testing do
  @moduledoc """
  Testing helpers for Stagehand.

  ## Setup

  Configure Stagehand with testing mode in your test config:

      # config/test.exs
      config :my_app, Stagehand, testing: :manual

  Or start with inline mode for synchronous execution:

      config :my_app, Stagehand, testing: :inline

  ## Usage

      Stagehand.Testing.assert_enqueued(name, worker: MyWorker)
      Stagehand.Testing.refute_enqueued(name, worker: OtherWorker)
      Stagehand.Testing.perform_job(MyWorker, %{"id" => 1})

  """

  @doc """
  Record a job in manual testing mode (called internally by `Stagehand.insert/2`).
  """
  @spec record_job(atom(), Stagehand.Job.t()) :: Stagehand.Job.t()
  def record_job(name, job) do
    table = ensure_table(name)
    ref = make_ref()
    job = %{job | ref: ref, inserted_at: DateTime.utc_now()}
    :ets.insert(table, {ref, job})
    job
  end

  @doc """
  Assert that a job matching the given opts was enqueued.

  ## Options

    * `:worker` - the worker module
    * `:queue` - the queue name
    * `:args` - expected args (partial match)
    * `:meta` - expected meta (partial match)
  """
  @spec assert_enqueued(atom(), keyword()) :: [Stagehand.Job.t()]
  def assert_enqueued(name \\ Stagehand, opts) do
    jobs = all_enqueued(name, opts)

    if Enum.empty?(jobs) do
      raise ExUnit.AssertionError,
        message: "Expected a job matching #{inspect(opts)} to be enqueued, but none was found."
    end

    jobs
  end

  @doc """
  Refute that any job matching the given opts was enqueued.
  """
  @spec refute_enqueued(atom(), keyword()) :: :ok
  def refute_enqueued(name \\ Stagehand, opts) do
    jobs = all_enqueued(name, opts)

    if !Enum.empty?(jobs) do
      raise ExUnit.AssertionError,
        message: "Expected no jobs matching #{inspect(opts)} to be enqueued, but found #{length(jobs)}."
    end

    :ok
  end

  @doc """
  Get all enqueued jobs matching the given opts.
  """
  @spec all_enqueued(atom(), keyword()) :: [Stagehand.Job.t()]
  def all_enqueued(name \\ Stagehand, opts \\ []) do
    table = ensure_table(name)

    table
    |> :ets.tab2list()
    |> Enum.map(fn {_id, job} -> job end)
    |> filter_jobs(opts)
  end

  @doc """
  Build and execute a job directly for testing. Returns the result of `perform/1`.
  """
  @spec perform_job(module(), map(), keyword()) :: term()
  def perform_job(worker, args, opts \\ []) do
    job = worker.new(args, opts)
    job = %{job | ref: make_ref(), attempt: job.attempt + 1}
    worker.perform(job)
  end

  @doc """
  Clear all recorded jobs for the given instance.
  """
  @spec drain_jobs(atom()) :: :ok
  def drain_jobs(name \\ Stagehand) do
    table = ensure_table(name)
    :ets.delete_all_objects(table)
    :ok
  end

  # -- Private --

  defp ensure_table(name) do
    table_name = :"stagehand_testing_#{name}"

    case :ets.whereis(table_name) do
      :undefined ->
        :ets.new(table_name, [:named_table, :set, :public, read_concurrency: true])

      _ref ->
        table_name
    end
  end

  defp filter_jobs(jobs, []), do: jobs

  defp filter_jobs(jobs, opts) do
    Enum.filter(jobs, fn job ->
      Enum.all?(opts, fn
        {:worker, worker} -> job.worker == worker
        {:queue, queue} -> job.queue == to_string(queue)
        {:args, args} -> map_subset?(args, job.args)
        {:meta, meta} -> map_subset?(meta, job.meta)
        {:priority, priority} -> job.priority == priority
        {:tags, tags} -> Enum.all?(tags, &(&1 in job.tags))
        _ -> true
      end)
    end)
  end

  defp map_subset?(subset, map) do
    Enum.all?(subset, fn {key, value} ->
      Map.get(map, key) == value || Map.get(map, to_string(key)) == value
    end)
  end
end
