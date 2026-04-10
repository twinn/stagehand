defmodule Stagehand.ClusterTest do
  use ExUnit.Case, async: false

  alias Stagehand.Test.Cluster
  alias Stagehand.Queue.Pipeline

  setup do
    name = :"stagehand_cluster_#{:erlang.unique_integer([:positive])}"
    {peer, peer_node} = Cluster.spawn_peer()

    # Start local stagehand and subscribe to producer joins
    start_supervised!(
      {Stagehand, name: name, queues: [default: 3], shutdown_grace_period: 5_000},
      id: name
    )

    pg_key = {:stagehand, name, :producers, "default"}
    {ref, _} = PgRegistry.monitor(Stagehand.ProducerRegistry, pg_key)

    # Start remote stagehand — wait for its producer to appear locally
    {:ok, _} = Cluster.start_stagehand(peer_node, name,
      queues: [default: 3],
      shutdown_grace_period: 5_000
    )

    assert_receive {^ref, :join, ^pg_key, [{pid, _}]} when node(pid) == peer_node, 5_000
    PgRegistry.demonitor(Stagehand.ProducerRegistry, ref)

    on_exit(fn ->
      try do
        :peer.stop(peer)
      catch
        :exit, _ -> :ok
      end
    end)

    {:ok, name: name, peer: peer, peer_node: peer_node}
  end

  describe "producer discovery" do
    test "both nodes see each other's producers", %{name: name, peer_node: peer_node} do
      local_producers = Pipeline.producers_for_queue(name, "default")
      remote_producers = :erpc.call(peer_node, Pipeline, :producers_for_queue, [name, "default"])

      assert length(local_producers) == 2
      assert length(remote_producers) == 2

      local_nodes = Enum.map(local_producers, &node/1) |> Enum.sort()
      assert node() in local_nodes
      assert peer_node in local_nodes
    end

    test "producers_for_queue returns only local after peer stops", %{name: name, peer: peer, peer_node: peer_node} do
      assert length(Pipeline.producers_for_queue(name, "default")) == 2

      pg_key = {:stagehand, name, :producers, "default"}
      {ref, _} = PgRegistry.monitor(Stagehand.ProducerRegistry, pg_key)

      :peer.stop(peer)

      # Wait for PgRegistry to process the leave
      assert_receive {^ref, :leave, ^pg_key, [{pid, _}]} when node(pid) == peer_node, 5_000
      PgRegistry.demonitor(Stagehand.ProducerRegistry, ref)

      producers = Pipeline.producers_for_queue(name, "default")
      assert length(producers) == 1
      assert node(hd(producers)) == node()
    end
  end

  describe "unique job dedup across nodes" do
    test "same unique fingerprint is deduplicated", %{name: name, peer_node: peer_node} do
      job = %Stagehand.Job{
        worker: SomeWorker,
        queue: "default",
        args: %{"key" => "cluster_unique"},
        unique: [period: 300, fields: [:worker, :queue, :args]],
        state: :available
      }

      {:ok, first} = Stagehand.insert(name, job)
      refute first.conflict?

      {:ok, second} = :erpc.call(peer_node, Stagehand, :insert, [name, job])
      assert second.conflict?
    end
  end

  describe "graceful shutdown" do
    test "peer shutdown transfers unique entries to survivor", %{name: name, peer_node: peer_node} do
      job = %Stagehand.Job{
        worker: SomeWorker,
        queue: "default",
        args: %{"key" => "transfer_me"},
        unique: [period: 300, fields: [:worker, :queue, :args]],
        state: :available
      }

      {:ok, _} = Stagehand.insert(name, job)

      # Subscribe to leave events, then stop the remote Stagehand
      # gracefully so the producer transfers entries while PgRegistry
      # is still running.
      pg_key = {:stagehand, name, :producers, "default"}
      {ref, _} = PgRegistry.monitor(Stagehand.ProducerRegistry, pg_key)

      :erpc.call(peer_node, Supervisor, :stop, [name])

      assert_receive {^ref, :leave, ^pg_key, [{pid, _}]} when node(pid) == peer_node, 5_000
      PgRegistry.demonitor(Stagehand.ProducerRegistry, ref)

      # Drain the local Unique server mailbox to ensure the import
      # from the dying producer has been processed.
      unique_name = Module.concat(name, Stagehand.Unique)
      _ = :sys.get_state(unique_name)

      {:ok, duplicate} = Stagehand.insert(name, job)
      assert duplicate.conflict?
    end
  end
end
