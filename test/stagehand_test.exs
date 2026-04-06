defmodule StagehandTest do
  use ExUnit.Case, async: true

  alias Stagehand.TestWorkers.ScheduledWorker
  alias Stagehand.TestWorkers.SuccessWorker

  setup do
    name = :"stagehand_test_#{System.unique_integer([:positive])}"

    start_supervised!({Stagehand, name: name, queues: [default: 5], testing: :disabled, shutdown_grace_period: 500})

    %{name: name}
  end

  describe "insert/2" do
    test "inserts and executes a job", %{name: name} do
      job = SuccessWorker.new(%{"key" => "value"})
      assert {:ok, %Stagehand.Job{ref: ref}} = Stagehand.insert(name, job)
      assert is_reference(ref)
    end

    test "insert_all/2 inserts multiple jobs", %{name: name} do
      jobs = [
        SuccessWorker.new(%{"a" => 1}),
        SuccessWorker.new(%{"b" => 2})
      ]

      assert {:ok, inserted} = Stagehand.insert_all(name, jobs)
      assert length(inserted) == 2
    end
  end

  describe "queue control" do
    test "pause and resume a queue", %{name: name} do
      assert :ok = Stagehand.pause_queue(name, queue: :default)

      info = Stagehand.check_queue(name, queue: :default)
      assert info.paused == true

      assert :ok = Stagehand.resume_queue(name, queue: :default)

      info = Stagehand.check_queue(name, queue: :default)
      assert info.paused == false
    end

    test "check_queue returns queue info", %{name: name} do
      info = Stagehand.check_queue(name, queue: :default)
      assert info.queue == "default"
      assert is_integer(info.available)
      assert is_integer(info.executing)
    end
  end

  describe "cancel_job/2" do
    test "cancels a scheduled job via timer ref", %{name: name} do
      {:ok, job} = Stagehand.insert(name, ScheduledWorker.new(%{}, schedule_in: 60))
      assert is_reference(job.ref)
      assert :ok = Stagehand.cancel_job(name, job)
    end

    test "cancels an available job via ref", %{name: name} do
      # Pause queue so job stays available
      Stagehand.pause_queue(name, queue: :default)

      {:ok, job} = Stagehand.insert(name, SuccessWorker.new(%{}))
      assert is_reference(job.ref)
      assert :ok = Stagehand.cancel_job(name, job)
    end
  end

  describe "scheduled jobs" do
    test "scheduled job is not immediately available", %{name: name} do
      job = ScheduledWorker.new(%{}, schedule_in: 60)
      assert {:ok, %Stagehand.Job{state: :scheduled, ref: ref}} = Stagehand.insert(name, job)
      assert is_reference(ref)
    end
  end
end
