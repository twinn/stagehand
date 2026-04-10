defmodule Stagehand.Test.Cluster do
  @moduledoc false

  def spawn_peer do
    cookie = Atom.to_charlist(Node.get_cookie())
    name = :"stagehand_peer_#{:erlang.unique_integer([:positive])}"

    {:ok, peer, node} =
      :peer.start(%{
        name: name,
        args: [~c"-setcookie", cookie]
      })

    :ok = :erpc.call(node, :code, :add_paths, [:code.get_path()])
    {:ok, _} = :erpc.call(node, Application, :ensure_all_started, [:stagehand])

    {peer, node}
  end

  def start_stagehand(node, name, opts \\ []) do
    queues = Keyword.get(opts, :queues, [default: 5])
    grace = Keyword.get(opts, :shutdown_grace_period, 5_000)
    caller = self()

    # Spawn a long-lived process on the remote node to own the
    # Stagehand supervision tree. erpc callers are short-lived and
    # their exit kills linked children.
    Node.spawn(node, fn ->
      {:ok, _} = Stagehand.start_link(
        name: name, queues: queues, shutdown_grace_period: grace
      )

      send(caller, {:stagehand_started, name})
      Process.sleep(:infinity)
    end)

    receive do
      {:stagehand_started, ^name} -> {:ok, name}
    after
      5_000 -> {:error, :timeout}
    end
  end

  def sync(node) do
    :erpc.call(node, :sys, :get_state, [Stagehand.ProducerRegistry])
    :sys.get_state(Stagehand.ProducerRegistry)
  end
end
