defmodule Stagehand.TestingTest do
  use ExUnit.Case, async: true

  alias Stagehand.TestWorkers.FailWorker
  alias Stagehand.TestWorkers.SuccessWorker

  describe "manual mode" do
    setup do
      name = :"stagehand_manual_#{System.unique_integer([:positive])}"

      start_supervised!({Stagehand, name: name, queues: [default: 5], testing: :manual, shutdown_grace_period: 500})

      on_exit(fn -> Stagehand.Testing.drain_jobs(name) end)

      %{name: name}
    end

    test "assert_enqueued finds matching job", %{name: name} do
      %{"user_id" => 1}
      |> SuccessWorker.new()
      |> then(&Stagehand.insert(name, &1))

      Stagehand.Testing.assert_enqueued(name, worker: SuccessWorker)
    end

    test "assert_enqueued matches on args", %{name: name} do
      %{"user_id" => 42}
      |> SuccessWorker.new()
      |> then(&Stagehand.insert(name, &1))

      Stagehand.Testing.assert_enqueued(name, worker: SuccessWorker, args: %{"user_id" => 42})
    end

    test "refute_enqueued passes when no match", %{name: name} do
      %{"user_id" => 1}
      |> SuccessWorker.new()
      |> then(&Stagehand.insert(name, &1))

      Stagehand.Testing.refute_enqueued(name, worker: FailWorker)
    end

    test "all_enqueued returns matching jobs", %{name: name} do
      %{"a" => 1} |> SuccessWorker.new() |> then(&Stagehand.insert(name, &1))
      %{"a" => 2} |> SuccessWorker.new() |> then(&Stagehand.insert(name, &1))
      %{"b" => 3} |> FailWorker.new() |> then(&Stagehand.insert(name, &1))

      jobs = Stagehand.Testing.all_enqueued(name, worker: SuccessWorker)
      assert length(jobs) == 2
    end

    test "perform_job executes the worker directly" do
      assert :ok = Stagehand.Testing.perform_job(SuccessWorker, %{"key" => "value"})
    end

    test "perform_job returns error for failing worker" do
      assert {:error, "boom"} = Stagehand.Testing.perform_job(FailWorker, %{})
    end
  end

  describe "inline mode" do
    setup do
      name = :"stagehand_inline_#{System.unique_integer([:positive])}"

      start_supervised!({Stagehand, name: name, queues: [default: 5], testing: :inline, shutdown_grace_period: 500})

      %{name: name}
    end

    test "insert executes job synchronously and returns completed", %{name: name} do
      job = SuccessWorker.new(%{"key" => "value"})
      assert {:ok, %Stagehand.Job{state: :completed}} = Stagehand.insert(name, job)
    end

    test "insert returns error for failing job", %{name: name} do
      job = FailWorker.new(%{})
      assert {:error, "boom"} = Stagehand.insert(name, job)
    end
  end
end
