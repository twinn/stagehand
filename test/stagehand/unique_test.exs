defmodule Stagehand.UniqueTest do
  use ExUnit.Case, async: true

  alias Stagehand.Job
  alias Stagehand.Unique

  setup do
    {:ok, pid} = Unique.start_link(name: :"unique_test_#{System.unique_integer([:positive])}")
    %{unique: pid}
  end

  describe "fingerprint/1" do
    test "returns nil for non-unique jobs" do
      job = %Job{unique: nil, worker: SomeWorker, queue: "default", args: %{}}
      assert Unique.fingerprint(job) == nil
    end

    test "generates consistent fingerprints for same data" do
      job = %Job{
        unique: [fields: [:worker, :queue, :args]],
        worker: SomeWorker,
        queue: "default",
        args: %{"user_id" => 1}
      }

      assert Unique.fingerprint(job) == Unique.fingerprint(job)
    end

    test "different args produce different fingerprints" do
      base = %Job{
        unique: [fields: [:worker, :queue, :args]],
        worker: SomeWorker,
        queue: "default"
      }

      fp1 = Unique.fingerprint(%{base | args: %{"user_id" => 1}})
      fp2 = Unique.fingerprint(%{base | args: %{"user_id" => 2}})

      assert fp1 != fp2
    end

    test "respects keys option for partial arg matching" do
      base = %Job{
        unique: [fields: [:worker, :queue, :args], keys: ["user_id"]],
        worker: SomeWorker,
        queue: "default"
      }

      fp1 = Unique.fingerprint(%{base | args: %{"user_id" => 1, "extra" => "a"}})
      fp2 = Unique.fingerprint(%{base | args: %{"user_id" => 1, "extra" => "b"}})

      assert fp1 == fp2
    end
  end

  describe "check_and_insert/3" do
    test "first insert succeeds", %{unique: server} do
      job = %Job{
        unique: [period: 60, fields: [:worker, :queue, :args], states: [:available]],
        worker: SomeWorker,
        queue: "default",
        args: %{"id" => 1},
        state: :available
      }

      fp = Unique.fingerprint(job)
      assert {:ok, ^job} = Unique.check_and_insert(server, fp, job)
    end

    test "duplicate within period returns conflict", %{unique: server} do
      job = %Job{
        unique: [period: 60, fields: [:worker, :queue, :args], states: [:available]],
        worker: SomeWorker,
        queue: "default",
        args: %{"id" => 1},
        state: :available
      }

      fp = Unique.fingerprint(job)
      assert {:ok, _} = Unique.check_and_insert(server, fp, job)
      assert {:conflict, _} = Unique.check_and_insert(server, fp, job)
    end

    test "different fingerprints don't conflict", %{unique: server} do
      base = %Job{
        unique: [period: 60, fields: [:worker, :queue, :args], states: [:available]],
        worker: SomeWorker,
        queue: "default",
        state: :available
      }

      job1 = %{base | args: %{"id" => 1}}
      job2 = %{base | args: %{"id" => 2}}

      fp1 = Unique.fingerprint(job1)
      fp2 = Unique.fingerprint(job2)

      assert {:ok, _} = Unique.check_and_insert(server, fp1, job1)
      assert {:ok, _} = Unique.check_and_insert(server, fp2, job2)
    end
  end
end
