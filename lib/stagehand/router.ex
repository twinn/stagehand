defmodule Stagehand.Router do
  @moduledoc """
  Routes jobs to the correct producer.

  - Normal jobs: random producer from the :pg group
  - Unique jobs: consistent hash → deterministic producer
  """

  alias Stagehand.Job
  alias Stagehand.Queue.Pipeline
  alias Stagehand.Queue.Producer
  alias Stagehand.Unique

  @doc """
  Route and insert a job.
  """
  @spec route(Job.t(), Stagehand.Config.t()) :: {:ok, Job.t()} | {:error, :no_producers}
  def route(%Job{} = job, conf) do
    producers = Pipeline.producers_for_queue(conf.name, job.queue)

    if producers == [] do
      {:error, :no_producers}
    else
      if job.unique do
        route_unique(job, conf, producers)
      else
        route_normal(job, Enum.random(producers))
      end
    end
  end

  defp route_normal(job, producer_pid) do
    case schedule_delay(job) do
      0 ->
        Producer.enqueue(producer_pid, job)

      delay_ms ->
        Producer.schedule(producer_pid, job, delay_ms)
    end
  end

  defp route_unique(job, conf, producers) do
    unique_server = Module.concat(conf.name, Unique)
    fingerprint = Unique.fingerprint(job)

    ring = Enum.reduce(producers, HashRing.new(), &HashRing.add_node(&2, &1))
    producer_pid = HashRing.key_to_node(ring, fingerprint)

    case Unique.check_and_insert(unique_server, fingerprint, job) do
      {:ok, job} ->
        route_normal(job, producer_pid)

      {:conflict, existing_job} ->
        {:ok, %{existing_job | conflict?: true}}
    end
  end

  defp schedule_delay(%Job{scheduled_at: nil}), do: 0

  defp schedule_delay(%Job{scheduled_at: scheduled_at}) do
    diff = DateTime.diff(scheduled_at, DateTime.utc_now(), :millisecond)
    max(0, diff)
  end
end
