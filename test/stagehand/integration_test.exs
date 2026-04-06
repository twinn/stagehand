defmodule Stagehand.IntegrationTest do
  use ExUnit.Case, async: true

  # Base time unit for all test timing. Everything is relative to this.
  @tick 10
  @timeout @tick * 20

  defmodule EchoWorker do
    @moduledoc false
    use Stagehand.Worker, queue: :default

    @impl true
    def perform(%Stagehand.Job{args: %{"test_pid" => pid_str} = args}) do
      send(:erlang.list_to_pid(String.to_charlist(pid_str)), {:job_done, args})
      :ok
    end
  end

  defmodule CountingWorker do
    @moduledoc false
    use Stagehand.Worker, queue: :default, max_attempts: 3

    @impl true
    def backoff(_attempt), do: 0

    @impl true
    def perform(%Stagehand.Job{args: %{"test_pid" => pid_str, "fail_until" => fail_until}, attempt: attempt}) do
      pid = :erlang.list_to_pid(String.to_charlist(pid_str))

      if attempt < fail_until do
        send(pid, {:attempt, attempt})
        {:error, "not yet (attempt #{attempt})"}
      else
        send(pid, {:attempt, attempt})
        :ok
      end
    end
  end

  defmodule SlowEchoWorker do
    @moduledoc false
    use Stagehand.Worker, queue: :slow

    @impl true
    def timeout(_job), do: 5 * 10

    @impl true
    def perform(%Stagehand.Job{args: %{"test_pid" => pid_str, "sleep" => sleep}}) do
      pid = :erlang.list_to_pid(String.to_charlist(pid_str))
      Process.sleep(sleep)
      send(pid, :slow_done)
      :ok
    end
  end

  defmodule SnoozeEchoWorker do
    @moduledoc false
    use Stagehand.Worker, queue: :default

    @impl true
    def perform(%Stagehand.Job{args: %{"test_pid" => pid_str}, attempt: attempt}) do
      pid = :erlang.list_to_pid(String.to_charlist(pid_str))

      if attempt == 1 do
        send(pid, {:snoozed, attempt})
        {:snooze, 0}
      else
        send(pid, {:ran_after_snooze, attempt})
        :ok
      end
    end
  end

  defmodule CancelEchoWorker do
    @moduledoc false
    use Stagehand.Worker, queue: :default

    @impl true
    def perform(%Stagehand.Job{args: %{"test_pid" => pid_str}}) do
      pid = :erlang.list_to_pid(String.to_charlist(pid_str))
      send(pid, :cancelled)
      {:cancel, "done"}
    end
  end

  defmodule UniqueEchoWorker do
    @moduledoc false
    use Stagehand.Worker, queue: :default, unique: [period: 60, fields: [:worker, :queue, :args]]

    @impl true
    def perform(%Stagehand.Job{args: %{"test_pid" => pid_str}}) do
      pid = :erlang.list_to_pid(String.to_charlist(pid_str))
      send(pid, :unique_ran)
      :ok
    end
  end

  defmodule RaisingWorker do
    @moduledoc false
    use Stagehand.Worker, queue: :default, max_attempts: 1

    @impl true
    def perform(_job), do: raise("boom")
  end

  defmodule ThrowingWorker do
    @moduledoc false
    use Stagehand.Worker, queue: :default, max_attempts: 1

    @impl true
    def perform(_job), do: throw(:boom)
  end

  defmodule ExhaustWorker do
    @moduledoc false
    use Stagehand.Worker, queue: :default, max_attempts: 2

    @impl true
    def backoff(_attempt), do: 0

    @impl true
    def perform(%Stagehand.Job{args: %{"test_pid" => pid_str}, attempt: attempt}) do
      pid = :erlang.list_to_pid(String.to_charlist(pid_str))
      send(pid, {:exhausted_attempt, attempt})
      {:error, "always fails"}
    end
  end

  defmodule HighVolumeWorker do
    @moduledoc false
    use Stagehand.Worker, queue: :bulk

    @impl true
    def perform(%Stagehand.Job{args: %{"test_pid" => pid_str, "id" => id}}) do
      pid = :erlang.list_to_pid(String.to_charlist(pid_str))
      send(pid, {:completed, id})
      :ok
    end
  end

  defp pid_str, do: self() |> :erlang.pid_to_list() |> List.to_string()

  defp start_stagehand(ctx, opts \\ []) do
    queues = Keyword.get(opts, :queues, default: 5)
    plugins = Keyword.get(opts, :plugins, [])
    testing = Keyword.get(opts, :testing, :disabled)

    name = :"stagehand_int_#{System.unique_integer([:positive])}"

    start_supervised!(
      {Stagehand, name: name, queues: queues, plugins: plugins, testing: testing, shutdown_grace_period: @timeout}
    )

    Map.put(ctx, :name, name)
  end

  describe "end-to-end job execution" do
    setup ctx, do: start_stagehand(ctx)

    test "job is dispatched and executed by the worker", %{name: name} do
      job = EchoWorker.new(%{"test_pid" => pid_str(), "value" => 42})
      assert {:ok, %Stagehand.Job{}} = Stagehand.insert(name, job)

      assert_receive {:job_done, %{"value" => 42}}, @timeout
    end

    test "insert_all dispatches multiple jobs", %{name: name} do
      jobs =
        for i <- 1..5 do
          EchoWorker.new(%{"test_pid" => pid_str(), "id" => i})
        end

      assert {:ok, inserted} = Stagehand.insert_all(name, jobs)
      assert length(inserted) == 5

      for i <- 1..5 do
        assert_receive {:job_done, %{"id" => ^i}}, @timeout
      end
    end
  end

  describe "retries" do
    setup ctx, do: start_stagehand(ctx)

    test "failed job is retried up to max_attempts", %{name: name} do
      job = CountingWorker.new(%{"test_pid" => pid_str(), "fail_until" => 3})
      Stagehand.insert(name, job)

      assert_receive {:attempt, 1}, @timeout
      assert_receive {:attempt, 2}, @timeout
      assert_receive {:attempt, 3}, @timeout
    end
  end

  describe "timeout" do
    setup ctx do
      start_stagehand(ctx, queues: [slow: 2])
    end

    test "job that exceeds timeout is terminated", %{name: name} do
      # Sleep is 2x the timeout — should be killed
      job = SlowEchoWorker.new(%{"test_pid" => pid_str(), "sleep" => @tick * 10})
      Stagehand.insert(name, job)

      refute_receive :slow_done, @tick * 5
    end

    test "job that finishes within timeout succeeds", %{name: name} do
      job = SlowEchoWorker.new(%{"test_pid" => pid_str(), "sleep" => @tick})
      Stagehand.insert(name, job)

      assert_receive :slow_done, @timeout
    end
  end

  describe "snooze" do
    setup ctx, do: start_stagehand(ctx)

    test "snoozed job is re-executed after delay", %{name: name} do
      job = SnoozeEchoWorker.new(%{"test_pid" => pid_str()})
      Stagehand.insert(name, job)

      assert_receive {:snoozed, 1}, @timeout
      assert_receive {:ran_after_snooze, 2}, @timeout
    end
  end

  describe "cancel" do
    setup ctx, do: start_stagehand(ctx)

    test "cancelled job runs once and stops", %{name: name} do
      job = CancelEchoWorker.new(%{"test_pid" => pid_str()})
      Stagehand.insert(name, job)

      assert_receive :cancelled, @timeout
      refute_receive :cancelled, @tick * 5
    end
  end

  describe "unique jobs" do
    setup ctx, do: start_stagehand(ctx)

    test "duplicate unique job returns conflict", %{name: name} do
      args = %{"test_pid" => pid_str()}
      job1 = UniqueEchoWorker.new(args)
      job2 = UniqueEchoWorker.new(args)

      assert {:ok, %{conflict?: false}} = Stagehand.insert(name, job1)
      assert {:ok, %{conflict?: true}} = Stagehand.insert(name, job2)

      assert_receive :unique_ran, @timeout
      refute_receive :unique_ran, @tick * 5
    end
  end

  describe "queue control during execution" do
    setup ctx, do: start_stagehand(ctx)

    test "pausing queue prevents new jobs from executing", %{name: name} do
      Stagehand.pause_queue(name, queue: :default)

      job = EchoWorker.new(%{"test_pid" => pid_str(), "value" => "paused"})
      Stagehand.insert(name, job)

      refute_receive {:job_done, _}, @tick * 5

      Stagehand.resume_queue(name, queue: :default)
      assert_receive {:job_done, %{"value" => "paused"}}, @timeout
    end

    test "drain returns pending jobs", %{name: name} do
      Stagehand.pause_queue(name, queue: :default)

      Stagehand.insert(name, EchoWorker.new(%{"test_pid" => pid_str(), "id" => 1}))
      Stagehand.insert(name, EchoWorker.new(%{"test_pid" => pid_str(), "id" => 2}))

      jobs = Stagehand.drain_queue(name, queue: :default)
      assert length(jobs) == 2
    end
  end

  describe "multiple queues" do
    setup ctx do
      start_stagehand(ctx, queues: [default: 2, slow: 1])
    end

    test "jobs route to correct queue", %{name: name} do
      fast_job = EchoWorker.new(%{"test_pid" => pid_str(), "type" => "fast"})
      slow_job = SlowEchoWorker.new(%{"test_pid" => pid_str(), "sleep" => @tick})

      Stagehand.insert(name, fast_job)
      Stagehand.insert(name, slow_job)

      assert_receive {:job_done, %{"type" => "fast"}}, @timeout
      assert_receive :slow_done, @timeout
    end

    test "check_queue reports per-queue status", %{name: name} do
      default_info = Stagehand.check_queue(name, queue: :default)
      slow_info = Stagehand.check_queue(name, queue: :slow)

      assert default_info.queue == "default"
      assert slow_info.queue == "slow"
    end
  end

  describe "concurrency" do
    setup ctx do
      start_stagehand(ctx, queues: [bulk: 20])
    end

    test "processes many jobs concurrently", %{name: name} do
      count = 50

      jobs =
        for i <- 1..count do
          HighVolumeWorker.new(%{"test_pid" => pid_str(), "id" => i})
        end

      Stagehand.insert_all(name, jobs)

      ids =
        for _ <- 1..count do
          assert_receive {:completed, id}, @timeout
          id
        end

      assert Enum.sort(ids) == Enum.to_list(1..count)
    end
  end

  defmodule TelemetryForwarder do
    @moduledoc false
    def handle_event(event, measurements, metadata, test_pid) do
      send(test_pid, {:telemetry, event, measurements, metadata})
    end
  end

  describe "worker crashes" do
    setup ctx, do: start_stagehand(ctx)

    test "worker that raises is caught and treated as error", %{name: name} do
      handler_id = "raise-test-#{System.unique_integer([:positive])}"
      test_pid = self()

      :telemetry.attach(
        handler_id,
        [:stagehand, :job, :exception],
        &TelemetryForwarder.handle_event/4,
        test_pid
      )

      Stagehand.insert(name, RaisingWorker.new(%{}))

      assert_receive {:telemetry, [:stagehand, :job, :exception], _, %{reason: %RuntimeError{}}},
                     @timeout

      :telemetry.detach(handler_id)
    end

    test "worker that throws is caught and treated as error", %{name: name} do
      handler_id = "throw-test-#{System.unique_integer([:positive])}"
      test_pid = self()

      :telemetry.attach(
        handler_id,
        [:stagehand, :job, :exception],
        &TelemetryForwarder.handle_event/4,
        test_pid
      )

      Stagehand.insert(name, ThrowingWorker.new(%{}))

      assert_receive {:telemetry, [:stagehand, :job, :exception], _, %{reason: {:throw, :boom}}},
                     @timeout

      :telemetry.detach(handler_id)
    end
  end

  describe "max attempts exhaustion" do
    setup ctx, do: start_stagehand(ctx)

    test "job is discarded after exhausting max_attempts", %{name: name} do
      handler_id = "discard-test-#{System.unique_integer([:positive])}"
      test_pid = self()

      :telemetry.attach(
        handler_id,
        [:stagehand, :job, :stop],
        &TelemetryForwarder.handle_event/4,
        test_pid
      )

      Stagehand.insert(name, ExhaustWorker.new(%{"test_pid" => pid_str()}))

      # Attempt 1 fails, retries
      assert_receive {:exhausted_attempt, 1}, @timeout
      # Attempt 2 fails, no more retries — discarded
      assert_receive {:exhausted_attempt, 2}, @timeout

      :telemetry.detach(handler_id)
    end
  end

  describe "error paths" do
    test "insert with invalid instance returns error" do
      job = EchoWorker.new(%{"test_pid" => pid_str()})
      assert {:error, {:not_running, :nonexistent}} = Stagehand.insert(:nonexistent, job)
    end

    test "insert_all with partial failure returns errors" do
      # Insert into a non-running instance
      jobs = [EchoWorker.new(%{}), EchoWorker.new(%{})]
      assert {:error, _reasons} = Stagehand.insert_all(:nonexistent, jobs)
    end
  end

  describe "inline testing mode" do
    setup ctx, do: start_stagehand(ctx, testing: :inline)

    test "cancel result returns cancelled state", %{name: name} do
      assert {:ok, %{state: :cancelled}} =
               Stagehand.insert(name, CancelEchoWorker.new(%{"test_pid" => pid_str()}))
    end

    test "snooze result returns scheduled state", %{name: name} do
      assert {:ok, %{state: :scheduled}} =
               Stagehand.insert(name, SnoozeEchoWorker.new(%{"test_pid" => pid_str()}))
    end
  end

  describe "telemetry" do
    setup ctx do
      ctx = start_stagehand(ctx)
      handler_id = "test-handler-#{System.unique_integer([:positive])}"

      :telemetry.attach_many(
        handler_id,
        [
          [:stagehand, :job, :start],
          [:stagehand, :job, :stop],
          [:stagehand, :job, :exception]
        ],
        &TelemetryForwarder.handle_event/4,
        self()
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      ctx
    end

    test "emits start and stop events for successful job", %{name: name} do
      job = EchoWorker.new(%{"test_pid" => pid_str(), "x" => 1})
      Stagehand.insert(name, job)

      assert_receive {:telemetry, [:stagehand, :job, :start], %{system_time: _}, %{job: _}},
                     @timeout

      assert_receive {:telemetry, [:stagehand, :job, :stop], %{duration: d}, %{job: _}}, @timeout
      assert is_integer(d)
    end

    test "emits exception event for failed job", %{name: name} do
      job = CountingWorker.new(%{"test_pid" => pid_str(), "fail_until" => 99})
      Stagehand.insert(name, job)

      assert_receive {:telemetry, [:stagehand, :job, :exception], _, %{reason: _}}, @timeout
    end
  end
end
