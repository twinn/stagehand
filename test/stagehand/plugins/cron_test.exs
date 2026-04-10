defmodule Stagehand.Plugins.CronTest do
  use ExUnit.Case, async: false

  alias Stagehand.Plugins.Cron

  @tick 10
  @timeout @tick * 20

  defmodule EveryMinuteWorker do
    @moduledoc false
    use Stagehand.Worker, queue: :default

    @impl true
    def perform(%Stagehand.Job{args: args}) do
      if pid_str = args["test_pid"] do
        pid = :erlang.list_to_pid(String.to_charlist(pid_str))
        send(pid, {:cron_ran, __MODULE__})
      end

      :ok
    end
  end

  defmodule HourlyWorker do
    @moduledoc false
    use Stagehand.Worker, queue: :default

    @impl true
    def perform(_job), do: :ok
  end

  defp start_stagehand(opts) do
    name = :"stagehand_cron_#{System.unique_integer([:positive])}"
    crontab = Keyword.fetch!(opts, :crontab)

    start_supervised!(
      {Stagehand,
       name: name,
       queues: [default: 5],
       shutdown_grace_period: @timeout,
       testing: Keyword.get(opts, :testing, :manual),
       plugins: [
         {Cron, crontab: crontab}
       ]}
    )

    name
  end

  defp cron_pid(name) do
    [{pid, _}] = Registry.lookup(Module.concat(name, Registry), :cron)
    pid
  end

  defp tick(pid) do
    send(pid, :tick)
    :sys.get_state(pid)
    :ok
  end

  describe "tick scheduling" do
    test "inserts jobs matching the current minute" do
      name = start_stagehand(crontab: [{"* * * * *", EveryMinuteWorker}])

      tick(cron_pid(name))

      jobs = Stagehand.Testing.all_enqueued(name, worker: EveryMinuteWorker)
      assert Enum.any?(jobs)
    end

    test "skips jobs that don't match the current time" do
      # "0 0 31 2 *" = Feb 31, never matches
      name = start_stagehand(crontab: [{"0 0 31 2 *", HourlyWorker}])

      tick(cron_pid(name))

      jobs = Stagehand.Testing.all_enqueued(name, worker: HourlyWorker)
      assert jobs == []
    end

    test "sets cron metadata on inserted jobs" do
      name = start_stagehand(crontab: [{"* * * * *", EveryMinuteWorker}])

      tick(cron_pid(name))

      [job] = Stagehand.Testing.all_enqueued(name, worker: EveryMinuteWorker)
      assert job.meta == %{"cron" => true}
    end

    test "passes worker options through" do
      name =
        start_stagehand(crontab: [{"* * * * *", EveryMinuteWorker, queue: :special, max_attempts: 1}])

      tick(cron_pid(name))

      [job] = Stagehand.Testing.all_enqueued(name, worker: EveryMinuteWorker)
      assert job.queue == "special"
      assert job.max_attempts == 1
    end
  end

  describe "aliases" do
    test "@daily alias is parsed" do
      name = start_stagehand(crontab: [{"@daily", HourlyWorker}])
      assert is_pid(cron_pid(name))
    end

    test "@hourly alias is parsed" do
      name = start_stagehand(crontab: [{"@hourly", HourlyWorker}])
      assert is_pid(cron_pid(name))
    end
  end

  describe "singleton" do
    test "cron is registered globally via Highlander" do
      name = start_stagehand(crontab: [{"* * * * *", EveryMinuteWorker}])
      pid = cron_pid(name)

      # Highlander registers under {Highlander, child_spec.id}
      highlander_pid = :global.whereis_name({Highlander, {Cron, name}})
      assert is_pid(highlander_pid)
      assert is_pid(pid)
      assert pid != highlander_pid
    end
  end

  defmodule CronTelemetryForwarder do
    @moduledoc false
    def handle_event(_event, _measurements, %{job: job}, pid) do
      if job.meta == %{"cron" => true}, do: send(pid, {:cron_executed, job.worker})
    end
  end

  describe "real execution" do
    test "cron job is dispatched and executed" do
      handler_id = "cron-test-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler_id,
        [:stagehand, :job, :stop],
        &CronTelemetryForwarder.handle_event/4,
        self()
      )

      name =
        start_stagehand(
          crontab: [{"* * * * *", EveryMinuteWorker}],
          testing: :disabled
        )

      tick(cron_pid(name))

      assert_receive {:cron_executed, EveryMinuteWorker}, @timeout

      :telemetry.detach(handler_id)
    end
  end
end
