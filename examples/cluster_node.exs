defmodule Demo.PrintWorker do
  use Stagehand.Worker, queue: :default

  @impl true
  def perform(%Stagehand.Job{args: args}) do
    IO.puts("[#{node()}] Processing: #{inspect(args)}")
    Process.sleep(Enum.random(200..800))
    IO.puts("[#{node()}] Done: #{inspect(args)}")
    :ok
  end
end

defmodule Demo.UniqueWorker do
  use Stagehand.Worker, queue: :default, unique: [period: 30]

  @impl true
  def perform(%Stagehand.Job{args: args}) do
    IO.puts("[#{node()}] Unique job: #{inspect(args)}")
    Process.sleep(500)
    :ok
  end
end

{:ok, _} = Stagehand.start_link(name: Demo.Stagehand, queues: [default: 5])

IO.puts("""
Stagehand running on #{node()}.

  Connect another node:
    Node.connect(:other_node@hostname)

  Insert jobs:
    %{"id" => 1} |> Demo.PrintWorker.new() |> then(&Stagehand.insert(Demo.Stagehand, &1))

  Check producers across cluster:
    Stagehand.Queue.Pipeline.producers_for_queue(Demo.Stagehand, "default")

  Check queue status:
    Stagehand.check_queue(Demo.Stagehand, queue: :default)
""")
