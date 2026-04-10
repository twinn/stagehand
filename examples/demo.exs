Mix.install([
  {:stagehand, path: "."}
])

defmodule Demo.PrintWorker do
  use Stagehand.Worker, queue: :default

  @impl true
  def perform(%Stagehand.Job{args: args}) do
    IO.puts("[#{inspect(self())}] Processing: #{inspect(args)}")
    Process.sleep(Enum.random(100..500))
    IO.puts("[#{inspect(self())}] Done: #{inspect(args)}")
    :ok
  end
end

defmodule Demo.FailWorker do
  use Stagehand.Worker, queue: :default, max_attempts: 3

  @impl true
  def perform(%Stagehand.Job{args: args, attempt: attempt}) do
    IO.puts("[#{inspect(self())}] Attempt #{attempt} for: #{inspect(args)}")

    if attempt < 3 do
      {:error, "not yet"}
    else
      IO.puts("[#{inspect(self())}] Finally succeeded on attempt #{attempt}")
      :ok
    end
  end

  @impl true
  def backoff(_attempt), do: 1
end

# Start Stagehand
{:ok, _} = Stagehand.start_link(name: Demo.Stagehand, queues: [default: 5])

IO.puts("\n--- Inserting 10 jobs ---\n")

for i <- 1..10 do
  %{"id" => i, "message" => "hello #{i}"}
  |> Demo.PrintWorker.new()
  |> then(&Stagehand.insert(Demo.Stagehand, &1))
end

IO.puts("\n--- Inserting a failing job (retries 3 times) ---\n")

%{"task" => "flaky"}
|> Demo.FailWorker.new()
|> then(&Stagehand.insert(Demo.Stagehand, &1))

IO.puts("\n--- Waiting for jobs to complete ---\n")
Process.sleep(5_000)

IO.puts("\n--- Queue status ---")
IO.inspect(Stagehand.check_queue(Demo.Stagehand, queue: :default))

IO.puts("\n--- Done ---")
