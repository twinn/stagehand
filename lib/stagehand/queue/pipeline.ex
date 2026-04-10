defmodule Stagehand.Queue.Pipeline do
  @moduledoc """
  Supervisor for a single queue's GenStage pipeline: ConsumerSupervisor + Producer.

  Children are ordered so that on shutdown (reverse order) the Producer
  stops first — leaving the pg group and draining executing jobs — before
  the ConsumerSupervisor is stopped.
  """

  use Supervisor

  alias Stagehand.Queue.ConsumerSupervisor
  alias Stagehand.Queue.Producer

  def start_link(opts) do
    name = opts[:pipeline_name]
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    queue = opts[:queue]
    conf = opts[:conf]
    limit = opts[:limit] || 10

    producer_name = producer_name(conf.name, queue)
    consumer_name = consumer_name(conf.name, queue)

    shutdown_grace = conf.shutdown_grace_period + 1_000

    children = [
      # ConsumerSupervisor starts first, but does not subscribe yet.
      %{
        id: ConsumerSupervisor,
        start:
          {ConsumerSupervisor, :start_link,
           [
             [
               name: consumer_name,
               shutdown_grace: conf.shutdown_grace_period
             ]
           ]},
        type: :supervisor,
        shutdown: shutdown_grace
      },
      # Producer starts second and registers with PgRegistry in init/1.
      # On shutdown (reverse order) it stops first, leaves the group in
      # terminate, then drains executing jobs.
      %{
        id: Producer,
        start:
          {Producer, :start_link,
           [
             [
               name: producer_name,
               queue: queue,
               conf: conf,
               consumer: consumer_name,
               max_demand: limit
             ]
           ]},
        shutdown: shutdown_grace
      }
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  @doc """
  Local producer name (for consumer subscription on the same node).
  """
  @spec producer_name(atom(), atom() | binary()) :: {:via, module(), term()}
  def producer_name(stagehand_name, queue) do
    {:via, Registry, {Module.concat(stagehand_name, Registry), {:producer, to_string(queue)}}}
  end

  @doc """
  Get all producer pids for a queue across the cluster.
  """
  @spec producers_for_queue(atom(), atom() | binary()) :: [pid()]
  def producers_for_queue(stagehand_name, queue) do
    for {pid, _} <- PgRegistry.lookup(Stagehand.ProducerRegistry, {:stagehand, stagehand_name, :producers, to_string(queue)}), do: pid
  end

  @doc """
  Get the consumer supervisor process name.
  """
  @spec consumer_name(atom(), atom() | binary()) :: {:via, module(), term()}
  def consumer_name(stagehand_name, queue) do
    {:via, Registry, {registry_name(stagehand_name), {:consumer, to_string(queue)}}}
  end

  defp registry_name(stagehand_name) do
    Module.concat(stagehand_name, Registry)
  end
end
