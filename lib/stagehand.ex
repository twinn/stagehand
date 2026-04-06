defmodule Stagehand do
  @moduledoc """
  Stagehand is a GenStage-based background job processing library,
  In-memory, no database required.

  ## Configuration

      # config/config.exs
      config :my_app, Stagehand,
        queues: [default: 10, mailers: 20],
        plugins: [
          {Stagehand.Plugins.Cron, crontab: [{"* * * * *", MyWorker}]}
        ]

      # config/test.exs
      config :my_app, Stagehand, testing: :manual

  ## Usage

  Add Stagehand to your supervision tree:

      children = [
        {Stagehand, otp_app: :my_app}
      ]

  Then insert jobs:

      %{"email" => "user@example.com"}
      |> MyApp.EmailWorker.new()
      |> Stagehand.insert()

  """

  alias Stagehand.Config
  alias Stagehand.Queue.Pipeline
  alias Stagehand.Queue.Producer
  alias Stagehand.Router

  @doc """
  Start a Stagehand supervision tree.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) when is_list(opts) do
    conf = Config.new(opts)
    :persistent_term.put({__MODULE__, conf.name}, conf)
    Stagehand.Supervisor.start_link(conf)
  end

  @doc false
  def child_spec(opts) do
    %{
      id: opts[:name] || __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor
    }
  end

  @doc """
  Insert a job for execution.
  """
  @spec insert(atom(), Stagehand.Job.t()) :: {:ok, Stagehand.Job.t()} | {:error, term()}
  def insert(name \\ __MODULE__, %Stagehand.Job{} = job) do
    with %Config{} = conf <- config(name) do
      case conf.testing do
        :inline ->
          execute_inline(job)

        :manual ->
          Stagehand.Testing.record_job(name, job)
          {:ok, %{job | ref: make_ref()}}

        :disabled ->
          Router.route(job, conf)
      end
    end
  end

  @doc """
  Insert multiple jobs at once.
  """
  @spec insert_all(atom(), [Stagehand.Job.t()]) :: {:ok, [Stagehand.Job.t()]} | {:error, [term()]}
  def insert_all(name \\ __MODULE__, jobs) when is_list(jobs) do
    {ok, errors} =
      jobs
      |> Enum.map(&insert(name, &1))
      |> Enum.split_with(&match?({:ok, _}, &1))

    if errors == [] do
      {:ok, Enum.map(ok, fn {:ok, job} -> job end)}
    else
      {:error, Enum.map(errors, fn {:error, reason} -> reason end)}
    end
  end

  @doc """
  Cancel a job by its ref. For available jobs, removes from the producer.
  Returns `:ok` on success or `{:error, :not_found}`.
  """
  @spec cancel_job(atom(), Stagehand.Job.t()) :: :ok | {:error, :not_found} | :not_found
  def cancel_job(_name \\ __MODULE__, %Stagehand.Job{} = job) do
    case Process.cancel_timer(job.ref) do
      time_left when is_integer(time_left) ->
        :ok

      false ->
        case job do
          %{producer_pid: pid} when is_pid(pid) ->
            Producer.cancel(pid, job.ref)

          _ ->
            {:error, :not_found}
        end
    end
  end

  @doc """
  Retry a job by re-inserting it.
  """
  @spec retry_job(atom(), Stagehand.Job.t()) :: {:ok, Stagehand.Job.t()} | {:error, term()}
  def retry_job(name \\ __MODULE__, %Stagehand.Job{} = job) do
    insert(name, %{job | attempt: 0, errors: [], state: :available})
  end

  @doc """
  Pause a queue. Broadcasts to all producers for this queue.
  """
  @spec pause_queue(atom(), keyword()) :: :ok
  def pause_queue(name \\ __MODULE__, opts) do
    queue = Keyword.fetch!(opts, :queue)

    for producer <- Pipeline.producers_for_queue(name, queue) do
      Producer.pause(producer)
    end

    :ok
  end

  @doc """
  Resume a paused queue. Broadcasts to all producers for this queue.
  """
  @spec resume_queue(atom(), keyword()) :: :ok
  def resume_queue(name \\ __MODULE__, opts) do
    queue = Keyword.fetch!(opts, :queue)

    for producer <- Pipeline.producers_for_queue(name, queue) do
      Producer.resume(producer)
    end

    :ok
  end

  @doc """
  Drain a queue, returning all pending jobs from all producers.
  """
  @spec drain_queue(atom(), keyword()) :: [Stagehand.Job.t()]
  def drain_queue(name \\ __MODULE__, opts) do
    queue = Keyword.fetch!(opts, :queue)

    name
    |> Pipeline.producers_for_queue(queue)
    |> Enum.flat_map(&Producer.drain/1)
  end

  @doc """
  Check the status of a queue across all producers.
  """
  @spec check_queue(atom(), keyword()) :: map()
  def check_queue(name \\ __MODULE__, opts) do
    queue = Keyword.fetch!(opts, :queue)

    name
    |> Pipeline.producers_for_queue(queue)
    |> Enum.map(&Producer.check/1)
    |> Enum.reduce(%{queue: to_string(queue), paused: false, available: 0, executing: 0}, fn info, acc ->
      %{
        acc
        | paused: acc.paused or info.paused,
          available: acc.available + info.available,
          executing: acc.executing + info.executing
      }
    end)
  end

  @doc """
  Get the configuration for a Stagehand instance.
  """
  @spec config(atom()) :: Config.t() | {:error, {:not_running, atom()}}
  def config(name \\ __MODULE__) do
    case :persistent_term.get({__MODULE__, name}, nil) do
      nil -> {:error, {:not_running, name}}
      conf -> conf
    end
  end

  # -- Private --

  defp execute_inline(job) do
    job = %{job | ref: make_ref(), attempt: job.attempt + 1}

    case job.worker.perform(job) do
      :ok -> {:ok, %{job | state: :completed}}
      {:ok, _} -> {:ok, %{job | state: :completed}}
      {:error, reason} -> {:error, reason}
      {:cancel, _reason} -> {:ok, %{job | state: :cancelled}}
      {:snooze, _} -> {:ok, %{job | state: :scheduled}}
    end
  end
end
