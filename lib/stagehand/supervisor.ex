defmodule Stagehand.Supervisor do
  @moduledoc """
  Top-level supervisor for a Stagehand instance. Manages the registry,
  hash ring, unique server, queue manager, and plugins.
  """

  use Supervisor

  alias Stagehand.Queue.Pipeline

  def start_link(conf) do
    Supervisor.start_link(__MODULE__, conf, name: conf.name)
  end

  @impl true
  def init(conf) do
    registry_name = Module.concat(conf.name, Registry)
    unique_name = Module.concat(conf.name, Stagehand.Unique)

    children =
      [
        {Registry, keys: :unique, name: registry_name},
        {Stagehand.Unique, name: unique_name}
      ] ++
        queue_children(conf) ++
        plugin_children(conf)

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp queue_children(conf) do
    for {queue, limit} <- conf.queues do
      queue_str = to_string(queue)

      Supervisor.child_spec(
        {Pipeline,
         queue: queue_str,
         conf: conf,
         limit: limit,
         pipeline_name: {:via, Registry, {Module.concat(conf.name, Registry), {:pipeline, queue_str}}}},
        id: {Pipeline, queue_str}
      )
    end
  end

  defp plugin_children(conf) do
    Enum.map(conf.plugins, fn
      {module, opts} -> {module, Keyword.put(opts, :conf, conf)}
      module -> {module, conf: conf}
    end)
  end
end
