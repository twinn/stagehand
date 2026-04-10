defmodule Stagehand.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {PgRegistry, Stagehand.ProducerRegistry}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Stagehand.ApplicationSupervisor)
  end
end
