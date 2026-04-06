defmodule Stagehand.WorkerTest do
  use ExUnit.Case, async: true

  alias Stagehand.TestWorkers.SuccessWorker
  alias Stagehand.TestWorkers.UniqueWorker

  describe "new/2" do
    test "builds a job struct with defaults" do
      job = SuccessWorker.new(%{"user_id" => 1})

      assert %Stagehand.Job{} = job
      assert job.worker == SuccessWorker
      assert job.queue == "default"
      assert job.args == %{"user_id" => 1}
      assert job.max_attempts == 20
      assert job.priority == 0
      assert job.tags == []
      assert job.state == :available
    end

    test "allows runtime overrides" do
      job = SuccessWorker.new(%{}, queue: :mailers, max_attempts: 5, priority: 3)

      assert job.queue == "mailers"
      assert job.max_attempts == 5
      assert job.priority == 3
    end

    test "schedule_in sets scheduled_at and state" do
      job = SuccessWorker.new(%{}, schedule_in: 60)

      assert job.state == :scheduled
      assert %DateTime{} = job.scheduled_at
      assert DateTime.diff(job.scheduled_at, DateTime.utc_now()) in 59..61
    end

    test "schedule_in with tuple" do
      job = SuccessWorker.new(%{}, schedule_in: {2, :hours})

      assert job.state == :scheduled
      diff = DateTime.diff(job.scheduled_at, DateTime.utc_now())
      assert diff in 7199..7201
    end

    test "scheduled_at sets directly" do
      future = DateTime.add(DateTime.utc_now(), 300, :second)
      job = SuccessWorker.new(%{}, scheduled_at: future)

      assert job.state == :scheduled
      assert job.scheduled_at == future
    end

    test "unique worker gets unique config" do
      job = UniqueWorker.new(%{"data" => "test"})

      assert job.unique == [period: 60, fields: [:worker, :queue, :args]]
    end

    test "tags and meta pass through" do
      job = SuccessWorker.new(%{}, tags: ["important"], meta: %{"source" => "api"})

      assert job.tags == ["important"]
      assert job.meta == %{"source" => "api"}
    end
  end
end
