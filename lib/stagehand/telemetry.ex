defmodule Stagehand.Telemetry do
  @moduledoc """
  Telemetry event emission for Stagehand.

  ## Events

    * `[:stagehand, :job, :start]` - Job begins execution
    * `[:stagehand, :job, :stop]` - Job completed successfully
    * `[:stagehand, :job, :exception]` - Job failed

    * `[:stagehand, :plugin, :start]` - Plugin begins execution
    * `[:stagehand, :plugin, :stop]` - Plugin completed
    * `[:stagehand, :plugin, :exception]` - Plugin failed

    * `[:stagehand, :queue, :shutdown]` - Queue shutting down
  """

  @doc false
  def job_start(job) do
    :telemetry.execute(
      [:stagehand, :job, :start],
      %{system_time: System.system_time()},
      %{job: job}
    )
  end

  @doc false
  def job_stop(job, duration) do
    :telemetry.execute(
      [:stagehand, :job, :stop],
      %{duration: duration, system_time: System.system_time()},
      %{job: job}
    )
  end

  @doc false
  def job_exception(job, duration, reason) do
    :telemetry.execute(
      [:stagehand, :job, :exception],
      %{duration: duration, system_time: System.system_time()},
      %{job: job, reason: reason}
    )
  end

  @doc false
  def plugin_start(plugin) do
    :telemetry.execute(
      [:stagehand, :plugin, :start],
      %{system_time: System.system_time()},
      %{plugin: plugin}
    )
  end

  @doc false
  def plugin_stop(plugin, duration) do
    :telemetry.execute(
      [:stagehand, :plugin, :stop],
      %{duration: duration, system_time: System.system_time()},
      %{plugin: plugin}
    )
  end

  @doc false
  def plugin_exception(plugin, duration, reason) do
    :telemetry.execute(
      [:stagehand, :plugin, :exception],
      %{duration: duration, system_time: System.system_time()},
      %{plugin: plugin, reason: reason}
    )
  end

  @doc false
  def queue_shutdown(queue) do
    :telemetry.execute(
      [:stagehand, :queue, :shutdown],
      %{system_time: System.system_time()},
      %{queue: queue}
    )
  end
end
