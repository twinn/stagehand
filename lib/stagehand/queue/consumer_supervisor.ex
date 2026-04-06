defmodule Stagehand.Queue.ConsumerSupervisor do
  @moduledoc """
  ConsumerSupervisor that spawns an Executor task for each job received
  from the Producer. Starts without a subscription — the Producer calls
  `GenStage.async_subscribe/2` after joining the pg group.
  """

  use ConsumerSupervisor

  alias Stagehand.Queue.ExecutorTask

  def start_link(opts) do
    ConsumerSupervisor.start_link(__MODULE__, opts, name: opts[:name])
  end

  @impl true
  def init(opts) do
    shutdown_grace = opts[:shutdown_grace] || 15_000

    children = [
      %{
        id: ExecutorTask,
        start: {ExecutorTask, :start_link, []},
        restart: :temporary,
        shutdown: shutdown_grace
      }
    ]

    ConsumerSupervisor.init(children, strategy: :one_for_one)
  end
end

defmodule Stagehand.Queue.ExecutorTask do
  @moduledoc false

  alias Stagehand.Queue.Executor

  def start_link(job) do
    Task.start_link(fn ->
      # Trap exits so the task can finish its work during shutdown
      # instead of being killed immediately by the ConsumerSupervisor.
      Process.flag(:trap_exit, true)
      Executor.run(job)
    end)
  end
end
