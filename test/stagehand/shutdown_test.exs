defmodule Stagehand.ShutdownTest do
  @moduledoc """
  Tests that verify graceful shutdown behavior — the #1 concern for
  anyone using Stagehand in production. Jobs must not be silently lost during
  normal deploys.
  """
  use ExUnit.Case, async: true

  alias Stagehand.Queue.Pipeline
  alias Stagehand.Queue.Producer

  # Base time unit. All sleeps, timeouts, and grace periods are relative.
  @tick 10
  @timeout @tick * 20

  defmodule SlowWorker do
    @moduledoc false
    use Stagehand.Worker, queue: :default

    @impl true
    def perform(%Stagehand.Job{args: %{"test_pid" => pid_str, "sleep" => sleep}}) do
      pid = :erlang.list_to_pid(String.to_charlist(pid_str))
      send(pid, {:started, self()})
      Process.sleep(sleep)
      send(pid, {:completed, self()})
      :ok
    end
  end

  defmodule QuickWorker do
    @moduledoc false
    use Stagehand.Worker, queue: :default

    @impl true
    def perform(%Stagehand.Job{args: %{"test_pid" => pid_str, "id" => id}}) do
      pid = :erlang.list_to_pid(String.to_charlist(pid_str))
      send(pid, {:completed, id})
      :ok
    end
  end

  defp pid_str, do: self() |> :erlang.pid_to_list() |> List.to_string()

  defp start_stagehand(opts \\ []) do
    grace = Keyword.get(opts, :shutdown_grace_period, @timeout * 10)
    queues = Keyword.get(opts, :queues, default: 5)

    name = :"stagehand_shutdown_#{System.unique_integer([:positive])}"

    start_supervised!(
      {Stagehand, name: name, queues: queues, shutdown_grace_period: grace},
      id: name
    )

    name
  end

  defp stop_stagehand(name) do
    stop_supervised!(name)
  end

  describe "supervisor shutdown waits for executing jobs" do
    test "executing job completes before shutdown finishes" do
      name = start_stagehand()

      job = SlowWorker.new(%{"test_pid" => pid_str(), "sleep" => @tick * 2})
      Stagehand.insert(name, job)

      assert_receive {:started, _worker_pid}, @timeout
      stop_stagehand(name)

      assert_receive {:completed, _worker_pid}, @timeout
    end

    test "multiple executing jobs all complete before shutdown" do
      name = start_stagehand()

      for i <- 1..3 do
        job = SlowWorker.new(%{"test_pid" => pid_str(), "sleep" => @tick * i})
        Stagehand.insert(name, job)
      end

      for _ <- 1..3, do: assert_receive({:started, _}, @timeout)

      stop_stagehand(name)

      for _ <- 1..3, do: assert_receive({:completed, _}, @timeout)
    end

    test "job exceeding grace period is abandoned" do
      name = start_stagehand(shutdown_grace_period: @tick * 5)

      # Job takes much longer than the grace period
      job = SlowWorker.new(%{"test_pid" => pid_str(), "sleep" => @timeout * 10})
      Stagehand.insert(name, job)

      assert_receive {:started, _worker_pid}, @timeout

      stop_stagehand(name)

      refute_receive {:completed, _worker_pid}, @tick * 5
    end
  end

  describe "available jobs during shutdown" do
    test "queued jobs are not silently lost — drain returns them" do
      name = start_stagehand()

      Stagehand.pause_queue(name, queue: :default)

      for i <- 1..5 do
        Stagehand.insert(name, QuickWorker.new(%{"test_pid" => pid_str(), "id" => i}))
      end

      jobs = Stagehand.drain_queue(name, queue: :default)
      assert length(jobs) == 5

      stop_stagehand(name)

      refute_receive {:completed, _}, @tick * 5
    end
  end

  describe "shutdown with no executing jobs" do
    test "empty queue shuts down immediately" do
      name = start_stagehand()

      {time_us, _} = :timer.tc(fn -> stop_stagehand(name) end)

      assert time_us < 1_000_000, "shutdown took #{time_us}us, expected < 1s"
    end

    test "shutdown after all jobs completed is clean" do
      name = start_stagehand()

      job = QuickWorker.new(%{"test_pid" => pid_str(), "id" => 1})
      Stagehand.insert(name, job)
      assert_receive {:completed, 1}, @timeout

      {time_us, _} = :timer.tc(fn -> stop_stagehand(name) end)
      assert time_us < 1_000_000, "shutdown took #{time_us}us, expected < 1s"
    end
  end

  describe "explicit Producer.shutdown/1" do
    test "waits for executing jobs then stops" do
      name = start_stagehand()

      job = SlowWorker.new(%{"test_pid" => pid_str(), "sleep" => @tick * 2})
      Stagehand.insert(name, job)
      assert_receive {:started, _}, @timeout

      [producer] = Pipeline.producers_for_queue(name, "default")

      Producer.shutdown(producer)

      assert_receive {:completed, _}, @timeout

      stop_stagehand(name)
    end
  end

  describe "pg group membership during shutdown" do
    test "producer leaves pg group and completes executing jobs" do
      name = start_stagehand()

      assert length(Pipeline.producers_for_queue(name, "default")) == 1

      job = SlowWorker.new(%{"test_pid" => pid_str(), "sleep" => @tick * 2})
      Stagehand.insert(name, job)
      assert_receive {:started, _}, @timeout

      stop_stagehand(name)

      assert_receive {:completed, _}, @timeout

      assert Pipeline.producers_for_queue(name, "default") == []
    end
  end
end
