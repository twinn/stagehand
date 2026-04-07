defmodule Stagehand.UniqueTransferTest do
  @moduledoc """
  Tests that unique job dedup entries survive topology changes.
  """

  use ExUnit.Case, async: true

  alias Stagehand.Job
  alias Stagehand.Queue.Pipeline
  alias Stagehand.Unique

  @tick 10
  @timeout @tick * 20

  defmodule UniqueWorker do
    @moduledoc false
    use Stagehand.Worker,
      queue: :default,
      unique: [period: 300, fields: [:worker, :queue, :args]]

    @impl true
    def perform(_job), do: :ok
  end

  describe "export and import" do
    test "entries can be exported from one server and imported into another" do
      {:ok, server_a} = Unique.start_link(name: :"unique_a_#{System.unique_integer([:positive])}")
      {:ok, server_b} = Unique.start_link(name: :"unique_b_#{System.unique_integer([:positive])}")

      job = %Job{
        unique: [period: 300, fields: [:worker, :queue, :args]],
        worker: SomeWorker,
        queue: "default",
        args: %{"key" => "same"},
        state: :available
      }

      fingerprint = Unique.fingerprint(job)

      assert {:ok, _} = Unique.check_and_insert(server_a, fingerprint, job)
      assert {:conflict, _} = Unique.check_and_insert(server_a, fingerprint, job)

      entries = Unique.export(server_a)
      Unique.import(server_b, entries)

      assert {:conflict, _} = Unique.check_and_insert(server_b, fingerprint, job)
    end
  end

  describe "transfer on topology change" do
    test "producer pushes dedup entries when new producer joins pg group" do
      name = :"stagehand_utx_#{System.unique_integer([:positive])}"

      start_supervised!(
        {Stagehand, name: name, queues: [default: 5], shutdown_grace_period: @timeout},
        id: name
      )

      # Insert a unique job — creates a dedup entry in the Unique server
      job = UniqueWorker.new(%{"key" => "dedup_me"})
      {:ok, _} = Stagehand.insert(name, job)

      # Start a second Unique server (simulates another node's Unique server)
      unique_b_name = :"unique_new_#{System.unique_integer([:positive])}"
      {:ok, _unique_b} = Unique.start_link(name: unique_b_name)

      # Simulate a new producer joining the pg group.
      # On a real cluster, the Producer monitors pg and gets a join notification.
      # It should then push dedup entries to the new node's Unique server.
      [producer] = Pipeline.producers_for_queue(name, "default")

      # Tell the producer a new "node" joined and where its Unique server is
      send(producer, {:unique_sync, unique_b_name})
      :sys.get_state(producer)

      # The new Unique server should now have the dedup entry
      fingerprint = Unique.fingerprint(job)
      assert {:conflict, _} = Unique.check_and_insert(unique_b_name, fingerprint, job)
    end

    test "entries transferred away are removed from the source Unique server" do
      # Start two Unique servers
      source_name = :"unique_src_#{System.unique_integer([:positive])}"
      target_name = :"unique_tgt_#{System.unique_integer([:positive])}"
      {:ok, _} = Unique.start_link(name: source_name)
      {:ok, _} = Unique.start_link(name: target_name)

      job = %Job{
        unique: [period: 300, fields: [:worker, :queue, :args]],
        worker: SomeWorker,
        queue: "default",
        args: %{"key" => "transfer_and_remove"},
        state: :available
      }

      fingerprint = Unique.fingerprint(job)

      # Insert into source
      assert {:ok, _} = Unique.check_and_insert(source_name, fingerprint, job)
      assert Unique.export(source_name) != []

      # Transfer and remove
      entries = Unique.export(source_name)
      Unique.import(target_name, entries)

      for {fp, _job, _ts} <- entries do
        Unique.remove(source_name, fp)
      end

      :sys.get_state(source_name)

      # Source should be empty
      assert Unique.export(source_name) == []

      # Target should have it
      assert {:conflict, _} = Unique.check_and_insert(target_name, fingerprint, job)
    end
  end
end
