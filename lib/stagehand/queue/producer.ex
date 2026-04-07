defmodule Stagehand.Queue.Producer do
  @moduledoc """
  GenStage producer for a single queue. Thin demand buffer — receives jobs,
  queues them, dispatches when consumers have demand.
  """

  use GenStage

  defstruct [
    :queue,
    :conf,
    available: :queue.new(),
    scheduled: %{},
    executing: 0,
    pending_demand: 0,
    paused: false,
    shutting_down: false
  ]

  def start_link(opts) do
    name = opts[:name]
    GenStage.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Enqueue a job into this producer.
  """
  @spec enqueue(GenServer.server(), Stagehand.Job.t()) :: {:ok, Stagehand.Job.t()}
  def enqueue(producer, %Stagehand.Job{} = job) do
    GenStage.call(producer, {:enqueue, job})
  end

  @doc """
  Schedule a job to be enqueued after `delay_ms` milliseconds.
  The timer is tracked so the job can be redistributed on shutdown.
  """
  @spec schedule(GenServer.server(), Stagehand.Job.t(), non_neg_integer()) :: {:ok, Stagehand.Job.t()}
  def schedule(producer, %Stagehand.Job{} = job, delay_ms) do
    GenStage.call(producer, {:schedule, job, delay_ms})
  end

  @doc """
  Cancel a job by ref. Removes from available queue if present.
  """
  @spec cancel(GenServer.server(), reference()) :: :ok | :not_found
  def cancel(producer, ref) when is_reference(ref) do
    GenStage.call(producer, {:cancel, ref})
  end

  @doc """
  Pause this queue — stops dispatching jobs.
  """
  @spec pause(GenServer.server()) :: :ok
  def pause(producer) do
    GenStage.call(producer, :pause)
  end

  @doc """
  Resume a paused queue.
  """
  @spec resume(GenServer.server()) :: :ok
  def resume(producer) do
    GenStage.call(producer, :resume)
  end

  @doc """
  Update the concurrency limit (handled by the consumer supervisor, not here).
  Returns current queue info.
  """
  @spec check(GenServer.server()) :: map()
  def check(producer) do
    GenStage.call(producer, :check)
  end

  @doc """
  Drain all available jobs synchronously. Returns the list of jobs.
  """
  @spec drain(GenServer.server()) :: [Stagehand.Job.t()]
  def drain(producer) do
    GenStage.call(producer, :drain)
  end

  @doc """
  Initiate graceful shutdown. Stops accepting new jobs and waits for
  executing jobs to finish before terminating.
  """
  @spec shutdown(GenServer.server()) :: :ok
  def shutdown(producer) do
    GenStage.call(producer, :shutdown)
  end

  # -- Callbacks --

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    queue = opts[:queue]
    conf = opts[:conf]

    pg_group = {:stagehand, conf.name, :producers, queue}
    :pg.monitor(:pg, pg_group)

    state = %__MODULE__{
      queue: queue,
      conf: conf
    }

    if opts[:consumer] do
      send(self(), {:subscribe_consumer, opts[:consumer], opts[:max_demand]})
    end

    {:producer, state}
  end

  @impl true
  def handle_call({:enqueue, job}, _from, state) do
    job = assign_ref(job)
    state = %{state | available: :queue.in(job, state.available)}

    {events, state} = dispatch(state)
    {:reply, {:ok, job}, events, state}
  end

  def handle_call({:schedule, job, delay_ms}, _from, state) do
    job = assign_ref(job)
    timer_ref = Process.send_after(self(), {:scheduled_fire, job.ref}, delay_ms)
    state = %{state | scheduled: Map.put(state.scheduled, job.ref, {timer_ref, job})}
    {:reply, {:ok, job}, [], state}
  end

  def handle_call({:cancel, ref}, _from, state) do
    {result, state} = do_cancel(ref, state)
    {:reply, result, [], state}
  end

  def handle_call(:pause, _from, state) do
    {:reply, :ok, [], %{state | paused: true}}
  end

  def handle_call(:resume, _from, state) do
    {events, state} = dispatch(%{state | paused: false})
    {:reply, :ok, events, state}
  end

  def handle_call(:check, _from, state) do
    info = %{
      queue: state.queue,
      paused: state.paused,
      available: :queue.len(state.available),
      scheduled: map_size(state.scheduled),
      executing: state.executing
    }

    {:reply, info, [], state}
  end

  def handle_call(:drain, _from, state) do
    jobs = :queue.to_list(state.available)
    state = %{state | available: :queue.new()}
    {:reply, jobs, [], state}
  end

  def handle_call(:shutdown, _from, state) do
    state = %{state | shutting_down: true, paused: true}

    if state.executing == 0 do
      {:stop, :normal, :ok, state}
    else
      grace = if state.conf, do: state.conf.shutdown_grace_period, else: 15_000
      Process.send_after(self(), :shutdown_timeout, grace)
      {:reply, :ok, [], state}
    end
  end

  @impl true
  def handle_demand(demand, state) when demand > 0 do
    state = %{state | pending_demand: state.pending_demand + demand}
    {events, state} = dispatch(state)
    {:noreply, events, state}
  end

  @impl true
  def handle_info({:subscribe_consumer, consumer, max_demand}, state) do
    min_demand = min(div(max_demand, 2), max_demand - 1)

    GenStage.async_subscribe(consumer,
      to: self(),
      max_demand: max_demand,
      min_demand: min_demand,
      cancel: :temporary
    )

    {:noreply, [], state}
  end

  def handle_info({:scheduled_fire, ref}, state) do
    case Map.pop(state.scheduled, ref) do
      {{_timer_ref, job}, scheduled} ->
        job = %{job | state: :available}
        state = %{state | scheduled: scheduled, available: :queue.in(job, state.available)}
        {events, state} = dispatch(state)
        {:noreply, events, state}

      {nil, _} ->
        {:noreply, [], state}
    end
  end

  def handle_info({:enqueue, %Stagehand.Job{} = job}, state) do
    state = %{state | available: :queue.in(job, state.available)}
    {events, state} = dispatch(state)
    {:noreply, events, state}
  end

  def handle_info({:job_finished, _result}, state) do
    state = %{state | executing: max(0, state.executing - 1)}

    if state.shutting_down and state.executing == 0 do
      {:stop, :normal, state}
    else
      {:noreply, [], state}
    end
  end

  def handle_info(:shutdown_timeout, state) do
    Stagehand.Telemetry.queue_shutdown(state.queue)
    {:stop, :normal, state}
  end

  def handle_info({_ref, :join, _group, _pids}, state) do
    sync_unique_entries(state)
    {:noreply, [], state}
  end

  def handle_info({_ref, :leave, _group, _pids}, state) do
    {:noreply, [], state}
  end

  def handle_info({:unique_sync, target}, state) do
    unique_name = Module.concat(state.conf.name, Stagehand.Unique)
    entries = Stagehand.Unique.export(unique_name)
    if entries != [], do: Stagehand.Unique.import(target, entries)
    {:noreply, [], state}
  end

  def handle_info(_msg, state) do
    {:noreply, [], state}
  end

  @impl true
  def terminate(_reason, state) do
    # Leave the pg group first so no new jobs are routed to us.
    pg_key = {:stagehand, state.conf.name, :producers, state.queue}
    PgRegistry.unregister_name({:pg, pg_key})

    if state.executing > 0 do
      grace = if state.conf, do: state.conf.shutdown_grace_period, else: 15_000
      wait_for_executing(state.executing, grace)
    end

    redistribute_jobs(state)
    sync_unique_entries(state)

    Stagehand.Telemetry.queue_shutdown(state.queue)
    :ok
  end

  defp sync_unique_entries(state) do
    unique_name = Module.concat(state.conf.name, Stagehand.Unique)
    pg_key = {:stagehand, state.conf.name, :producers, state.queue}
    all_producers = PgRegistry.get_members(:pg, pg_key)
    entries = Stagehand.Unique.export(unique_name)

    do_sync_unique(unique_name, entries, all_producers)
  end

  defp do_sync_unique(_unique_name, [], _producers), do: :ok
  defp do_sync_unique(_unique_name, _entries, producers) when length(producers) < 2, do: :ok

  defp do_sync_unique(unique_name, entries, producers) do
    ring = Enum.reduce(producers, HashRing.new(), &HashRing.add_node(&2, &1))

    remote =
      for {fp, _job, _ts} = entry <- entries,
          owner = HashRing.key_to_node(ring, fp),
          owner != self(),
          node(owner) != node(),
          reduce: %{} do
        acc -> Map.update(acc, node(owner), [entry], &[entry | &1])
      end

    for {remote_node, batch} <- remote do
      try do
        Stagehand.Unique.import({unique_name, remote_node}, batch)
      catch
        :exit, _ -> :ok
      end

      Enum.each(batch, fn {fp, _, _} -> Stagehand.Unique.remove(unique_name, fp) end)
    end
  end

  defp wait_for_executing(0, _remaining), do: :ok
  defp wait_for_executing(_count, remaining) when remaining <= 0, do: :ok

  defp wait_for_executing(count, remaining) do
    start = System.monotonic_time(:millisecond)

    receive do
      {:job_finished, _result} ->
        elapsed = System.monotonic_time(:millisecond) - start
        wait_for_executing(count - 1, remaining - elapsed)
    after
      remaining ->
        :ok
    end
  end

  # -- Private --

  defp redistribute_jobs(state) do
    scheduled_jobs =
      for {_ref, {timer_ref, job}} <- state.scheduled do
        Process.cancel_timer(timer_ref)
        job
      end

    jobs = scheduled_jobs ++ :queue.to_list(state.available)
    pg_key = {:stagehand, state.conf.name, :producers, state.queue}
    producers = PgRegistry.get_members(:pg, pg_key) -- [self()]
    count = length(producers)

    for {job, i} <- Enum.with_index(jobs), count > 0 do
      producer = Enum.at(producers, rem(i, count))

      case job.scheduled_at do
        nil ->
          send(producer, {:enqueue, job})

        at ->
          GenStage.call(
            producer,
            {:schedule, job, max(0, DateTime.diff(at, DateTime.utc_now(), :millisecond))}
          )
      end
    end
  end

  defp assign_ref(job) do
    %{job | ref: make_ref(), producer_pid: self()}
  end

  defp dispatch(%{paused: true} = state), do: {[], state}
  defp dispatch(%{pending_demand: 0} = state), do: {[], state}

  defp dispatch(state) do
    {events, available, remaining_demand} =
      take_jobs(state.available, state.pending_demand, [])

    state = %{
      state
      | available: available,
        pending_demand: remaining_demand,
        executing: state.executing + length(events)
    }

    {events, state}
  end

  defp take_jobs(queue, 0, acc), do: {Enum.reverse(acc), queue, 0}

  defp take_jobs(queue, demand, acc) do
    case :queue.out(queue) do
      {{:value, job}, rest} ->
        take_jobs(rest, demand - 1, [job | acc])

      {:empty, queue} ->
        {Enum.reverse(acc), queue, demand}
    end
  end

  defp do_cancel(ref, state) do
    with {nil, _} <- Map.pop(state.scheduled, ref),
         {_kept, []} <- Enum.split_with(:queue.to_list(state.available), &(&1.ref != ref)) do
      {:not_found, state}
    else
      {{timer_ref, _job}, scheduled} ->
        Process.cancel_timer(timer_ref)
        {:ok, %{state | scheduled: scheduled}}

      {remaining, [_removed | _]} ->
        {:ok, %{state | available: :queue.from_list(remaining)}}
    end
  end
end
