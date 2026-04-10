defmodule Stagehand.Worker do
  @moduledoc """
  Behaviour for Stagehand workers.

  ## Usage

      defmodule MyApp.EmailWorker do
        use Stagehand.Worker, queue: :mailers, max_attempts: 5

        @impl true
        def perform(%Stagehand.Job{args: %{"to" => to, "body" => body}}) do
          MyApp.Mailer.send(to, body)
          :ok
        end
      end

  ## Options

    * `:queue` - the queue name (default: `:default`)
    * `:max_attempts` - max retry attempts (default: `20`)
    * `:priority` - priority 0-9, lower is higher (default: `0`)
    * `:tags` - list of tag strings (default: `[]`)
    * `:unique` - uniqueness config keyword list or `false` (default: `false`)

  ## Return Values from `perform/1`

    * `:ok` or `{:ok, value}` - job succeeded
    * `{:error, reason}` - job failed, will retry if attempts remain
    * `{:snooze, seconds}` - reschedule job after seconds
    * `{:cancel, reason}` - cancel job, no more retries
  """

  @callback perform(job :: Stagehand.Job.t()) ::
              :ok
              | {:ok, term()}
              | {:error, term()}
              | {:snooze, pos_integer()}
              | {:cancel, term()}

  @callback backoff(attempt :: pos_integer()) :: pos_integer()

  @callback timeout(job :: Stagehand.Job.t()) :: pos_integer()

  @optional_callbacks backoff: 1, timeout: 1

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @behaviour Stagehand.Worker

      @stagehand_opts opts

      @doc """
      Builds a new job struct for this worker.
      """
      def new(args, runtime_opts \\ []) do
        Stagehand.Worker.build_job(__MODULE__, @stagehand_opts, args, runtime_opts)
      end
    end
  end

  @doc false
  def build_job(worker, worker_opts, args, runtime_opts) do
    opts = Keyword.merge(worker_opts, runtime_opts)

    queue =
      opts
      |> Keyword.get(:queue, :default)
      |> to_string()

    scheduled_at = resolve_schedule(opts)

    unique =
      case Keyword.get(opts, :unique, false) do
        false -> nil
        true -> [period: 60, fields: [:worker, :queue, :args]]
        config when is_list(config) -> config
      end

    state = if scheduled_at, do: :scheduled, else: :available

    %Stagehand.Job{
      state: state,
      worker: worker,
      queue: queue,
      args: args,
      max_attempts: Keyword.get(opts, :max_attempts, 20),
      priority: Keyword.get(opts, :priority, 0),
      tags: Keyword.get(opts, :tags, []),
      meta: Keyword.get(opts, :meta, %{}),
      unique: unique,
      scheduled_at: scheduled_at,
      inserted_at: DateTime.utc_now()
    }
  end

  defp resolve_schedule(opts) do
    cond do
      scheduled_at = Keyword.get(opts, :scheduled_at) ->
        scheduled_at

      schedule_in = Keyword.get(opts, :schedule_in) ->
        seconds = normalize_schedule_in(schedule_in)
        DateTime.add(DateTime.utc_now(), seconds, :second)

      true ->
        nil
    end
  end

  defp normalize_schedule_in(seconds) when is_integer(seconds), do: seconds
  defp normalize_schedule_in({amount, unit}) when unit in [:second, :seconds], do: amount
  defp normalize_schedule_in({amount, unit}) when unit in [:minute, :minutes], do: amount * 60
  defp normalize_schedule_in({amount, unit}) when unit in [:hour, :hours], do: amount * 3600
  defp normalize_schedule_in({amount, unit}) when unit in [:day, :days], do: amount * 86_400
end
