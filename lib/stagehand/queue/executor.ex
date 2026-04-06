defmodule Stagehand.Queue.Executor do
  @moduledoc """
  Executes a single job. Called by the ConsumerSupervisor as a Task.

  Handles:
  - Calling `worker.perform(job)`
  - Timeout enforcement
  - Telemetry emission
  - Retry scheduling via `Process.send_after` on failure
  """

  alias Stagehand.Job
  alias Stagehand.Queue.Producer
  alias Stagehand.Telemetry

  @doc """
  Execute a job. This is the entry point called by the ConsumerSupervisor.
  """
  @spec run(Job.t()) :: term()
  def run(%Job{worker: worker} = job) do
    job = %{job | state: :executing, attempt: job.attempt + 1, attempted_at: DateTime.utc_now()}

    timeout = resolve_timeout(worker, job)

    Telemetry.job_start(job)
    start_time = System.monotonic_time()

    worker
    |> execute_with_timeout(job, timeout)
    |> handle_result(job, start_time)
  end

  defp execute_with_timeout(worker, job, :infinity) do
    safe_perform(worker, job)
  end

  defp execute_with_timeout(worker, job, timeout) do
    task = Task.async(fn -> safe_perform(worker, job) end)

    case Task.yield(task, timeout) || Task.shutdown(task, :brutal_kill) do
      {:ok, result} -> result
      nil -> {:error, %RuntimeError{message: "job timed out after #{timeout}ms"}}
    end
  end

  defp safe_perform(worker, job) do
    worker.perform(job)
  rescue
    error -> {:error, error}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp handle_result(result, job, start_time) do
    duration = System.monotonic_time() - start_time

    case result do
      :ok ->
        Telemetry.job_stop(job, duration)
        notify_producer(job, :completed)

      {:ok, _value} ->
        Telemetry.job_stop(job, duration)
        notify_producer(job, :completed)

      {:error, reason} ->
        Telemetry.job_exception(job, duration, reason)
        maybe_retry(job, reason)

      {:snooze, seconds} ->
        Telemetry.job_stop(job, duration)
        snooze(job, seconds)

      {:cancel, reason} ->
        Telemetry.job_stop(job, duration)
        notify_producer(job, :cancelled)
        {:cancel, reason}
    end
  end

  defp maybe_retry(job, reason) do
    error_entry = %{
      at: DateTime.utc_now(),
      attempt: job.attempt,
      error: inspect(reason)
    }

    job = %{job | errors: job.errors ++ [error_entry]}

    if job.attempt < job.max_attempts do
      backoff_ms = resolve_backoff(job) * 1_000

      retry_job = %{job | state: :retryable}
      Producer.schedule(job.producer_pid, retry_job, backoff_ms)
    else
      notify_producer(job, :discarded)
    end
  end

  defp snooze(job, seconds) do
    snoozed_job = %{job | state: :scheduled}
    Producer.schedule(job.producer_pid, snoozed_job, seconds * 1_000)
  end

  defp resolve_backoff(job) do
    if function_exported?(job.worker, :backoff, 1) do
      job.worker.backoff(job.attempt)
    else
      Stagehand.Backoff.compute(job.attempt)
    end
  end

  defp resolve_timeout(worker, job) do
    if function_exported?(worker, :timeout, 1) do
      worker.timeout(job)
    else
      :infinity
    end
  end

  defp notify_producer(job, state) do
    send(job.producer_pid, {:job_finished, %{job | state: state}})
  end
end
