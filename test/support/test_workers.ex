defmodule Stagehand.TestWorkers.SuccessWorker do
  @moduledoc false
  use Stagehand.Worker, queue: :default

  @impl true
  def perform(_job), do: :ok
end

defmodule Stagehand.TestWorkers.FailWorker do
  @moduledoc false
  use Stagehand.Worker, queue: :default, max_attempts: 3

  @impl true
  def perform(_job), do: {:error, "boom"}
end

defmodule Stagehand.TestWorkers.SnoozeWorker do
  @moduledoc false
  use Stagehand.Worker, queue: :default

  @impl true
  def perform(_job), do: {:snooze, 5}
end

defmodule Stagehand.TestWorkers.CancelWorker do
  @moduledoc false
  use Stagehand.Worker, queue: :default

  @impl true
  def perform(_job), do: {:cancel, "no longer needed"}
end

defmodule Stagehand.TestWorkers.SlowWorker do
  @moduledoc false
  use Stagehand.Worker, queue: :default

  @impl true
  def timeout(_job), do: 100

  @impl true
  def perform(_job) do
    Process.sleep(200)
    :ok
  end
end

defmodule Stagehand.TestWorkers.UniqueWorker do
  @moduledoc false
  use Stagehand.Worker,
    queue: :default,
    unique: [period: 60, fields: [:worker, :queue, :args]]

  @impl true
  def perform(_job), do: :ok
end

defmodule Stagehand.TestWorkers.ScheduledWorker do
  @moduledoc false
  use Stagehand.Worker, queue: :default

  @impl true
  def perform(_job), do: :ok
end

defmodule Stagehand.TestWorkers.CustomBackoffWorker do
  @moduledoc false
  use Stagehand.Worker, queue: :default, max_attempts: 3

  @impl true
  def backoff(_attempt), do: 1

  @impl true
  def perform(_job), do: {:error, "retry me"}
end
