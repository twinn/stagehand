defmodule Stagehand.Plugins.Cron do
  @moduledoc """
  Plugin for scheduling periodic/cron jobs.

  Uses highlander pattern — only the leader node (first in sorted :pg members)
  fires cron jobs, preventing duplicate scheduling across the cluster.

  ## Usage

      {Stagehand.Plugins.Cron,
       crontab: [
         {"* * * * *", MyApp.MinuteWorker},
         {"0 12 * * 1", MyApp.MondayWorker, queue: :scheduled},
         {"@daily", MyApp.DailyWorker, max_attempts: 1}
       ]}
  """

  use GenServer

  alias Crontab.CronExpression.Parser

  @aliases %{
    "@yearly" => "0 0 1 1 *",
    "@annually" => "0 0 1 1 *",
    "@monthly" => "0 0 1 * *",
    "@weekly" => "0 0 * * 0",
    "@daily" => "0 0 * * *",
    "@midnight" => "0 0 * * *",
    "@hourly" => "0 * * * *"
  }

  defstruct [:conf, :crontab, :timer_ref]

  def start_link(opts) do
    conf = opts[:conf]
    name = {:via, PgRegistry, {:pg, {:stagehand_cron, conf.name}}}
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    conf = opts[:conf]

    crontab =
      opts
      |> Keyword.get(:crontab, [])
      |> Enum.map(&parse_entry/1)

    state = %__MODULE__{
      conf: conf,
      crontab: crontab
    }

    state = schedule_tick(state)
    {:ok, state}
  end

  @impl true
  def handle_info(:tick, state) do
    now = NaiveDateTime.utc_now()

    for {cron, worker, opts} <- state.crontab,
        leader?(state),
        Crontab.DateChecker.matches_date?(cron, now) do
      job = worker.new(%{}, Keyword.put(opts, :meta, %{"cron" => true}))
      Stagehand.insert(state.conf.name, job)
    end

    {:noreply, schedule_tick(state)}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # -- Private --

  defp leader?(%{conf: conf}) do
    case PgRegistry.get_members(:pg, {:stagehand_cron, conf.name}) do
      [] -> true
      members -> self() == Enum.min(members)
    end
  end

  defp parse_entry({expression, worker}), do: parse_entry({expression, worker, []})

  defp parse_entry({expression, worker, opts}) do
    expression = Map.get(@aliases, expression, expression)
    cron = Parser.parse!(expression)
    {cron, worker, opts}
  end

  defp schedule_tick(state) do
    now = DateTime.utc_now()
    next_minute = now |> DateTime.add(60 - now.second, :second) |> Map.put(:second, 0)
    delay = DateTime.diff(next_minute, now, :millisecond)

    ref = Process.send_after(self(), :tick, delay)
    %{state | timer_ref: ref}
  end
end
