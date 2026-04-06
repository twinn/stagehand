defmodule Stagehand.Config do
  @moduledoc """
  Configuration struct for a Stagehand instance.

  ## Usage

      # config/config.exs
      config :my_app, Stagehand,
        queues: [default: 10, mailers: 20],
        plugins: [{Stagehand.Plugins.Cron, crontab: [...]}]

      # config/test.exs
      config :my_app, Stagehand, testing: :manual

  """

  @type testing_mode :: :disabled | :inline | :manual

  @type t :: %__MODULE__{
          name: atom(),
          queues: keyword(pos_integer()),
          plugins: [module() | {module(), keyword()}],
          node: binary(),
          testing: testing_mode(),
          shutdown_grace_period: non_neg_integer()
        }

  defstruct [
    :name,
    queues: [default: 10],
    plugins: [],
    node: nil,
    testing: :disabled,
    shutdown_grace_period: 15_000
  ]

  @doc """
  Build a config struct from the given options.

  Options can be passed directly or read from Application config:

      Config.new(otp_app: :my_app, name: Stagehand)
  """
  @spec new(keyword()) :: t()
  def new(opts) do
    opts = resolve_opts(opts)
    node = opts[:node] || to_string(node())

    %__MODULE__{
      name: opts[:name] || Stagehand,
      queues: opts[:queues] || [default: 10],
      plugins: opts[:plugins] || [],
      node: node,
      testing: opts[:testing] || :disabled,
      shutdown_grace_period: opts[:shutdown_grace_period] || 15_000
    }
  end

  defp resolve_opts(opts) do
    case Keyword.fetch(opts, :otp_app) do
      {:ok, app} ->
        name = opts[:name] || Stagehand
        app_opts = Application.get_env(app, name, [])
        Keyword.merge(app_opts, opts)

      :error ->
        opts
    end
  end
end
