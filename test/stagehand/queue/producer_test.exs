defmodule Stagehand.Queue.ProducerTest do
  use ExUnit.Case, async: true

  alias Stagehand.Job
  alias Stagehand.Queue.Producer

  setup do
    name = :"producer_test_#{System.unique_integer([:positive])}"

    {:ok, pid} =
      Producer.start_link(
        name: name,
        queue: "default",
        conf: %Stagehand.Config{name: :test}
      )

    %{producer: name, pid: pid}
  end

  describe "enqueue/2" do
    test "assigns a ref and returns the job", %{producer: producer} do
      job = %Job{worker: SomeWorker, args: %{"a" => 1}, queue: "default"}
      assert {:ok, %Job{ref: ref}} = Producer.enqueue(producer, job)
      assert is_reference(ref)
    end

    test "each job gets a unique ref", %{producer: producer} do
      {:ok, job1} = Producer.enqueue(producer, %Job{worker: SomeWorker, args: %{}})
      {:ok, job2} = Producer.enqueue(producer, %Job{worker: SomeWorker, args: %{}})
      assert job1.ref != job2.ref
    end
  end

  describe "pause/resume" do
    test "pausing stops dispatch", %{producer: producer} do
      Producer.pause(producer)
      info = Producer.check(producer)
      assert info.paused == true

      Producer.resume(producer)
      info = Producer.check(producer)
      assert info.paused == false
    end
  end

  describe "cancel/2" do
    test "cancels an available job by ref", %{producer: producer} do
      # Pause so jobs stay in available queue
      Producer.pause(producer)

      {:ok, job} = Producer.enqueue(producer, %Job{worker: SomeWorker, args: %{}})
      assert :ok = Producer.cancel(producer, job.ref)

      info = Producer.check(producer)
      assert info.available == 0
    end

    test "returns :not_found for unknown ref", %{producer: producer} do
      assert :not_found = Producer.cancel(producer, make_ref())
    end
  end

  describe "drain/1" do
    test "returns all available jobs and empties the queue", %{producer: producer} do
      # Pause so jobs stay in available queue
      Producer.pause(producer)

      Producer.enqueue(producer, %Job{worker: SomeWorker, args: %{"a" => 1}})
      Producer.enqueue(producer, %Job{worker: SomeWorker, args: %{"b" => 2}})

      jobs = Producer.drain(producer)
      assert length(jobs) == 2

      info = Producer.check(producer)
      assert info.available == 0
    end
  end

  describe "check/1" do
    test "returns queue status", %{producer: producer} do
      info = Producer.check(producer)

      assert info.queue == "default"
      assert info.paused == false
      assert info.available == 0
      assert info.executing == 0
    end
  end

  describe "priority" do
    test "higher priority jobs are drained before lower priority", %{producer: producer} do
      Producer.pause(producer)

      Producer.enqueue(producer, %Job{worker: SomeWorker, args: %{"id" => "low"}, priority: 5})
      Producer.enqueue(producer, %Job{worker: SomeWorker, args: %{"id" => "high"}, priority: 0})
      Producer.enqueue(producer, %Job{worker: SomeWorker, args: %{"id" => "mid"}, priority: 3})

      jobs = Producer.drain(producer)
      ids = Enum.map(jobs, & &1.args["id"])

      assert ids == ["high", "mid", "low"]
    end

    test "same priority preserves insertion order", %{producer: producer} do
      Producer.pause(producer)

      Producer.enqueue(producer, %Job{worker: SomeWorker, args: %{"id" => "first"}, priority: 0})
      Producer.enqueue(producer, %Job{worker: SomeWorker, args: %{"id" => "second"}, priority: 0})
      Producer.enqueue(producer, %Job{worker: SomeWorker, args: %{"id" => "third"}, priority: 0})

      jobs = Producer.drain(producer)
      ids = Enum.map(jobs, & &1.args["id"])

      assert ids == ["first", "second", "third"]
    end
  end
end
