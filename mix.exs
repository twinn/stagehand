defmodule Stagehand.MixProject do
  use Mix.Project

  def project do
    [
      app: :stagehand,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      elixirc_paths: elixirc_paths(Mix.env()),
      aliases: aliases(),
      dialyzer: [plt_add_apps: [:ex_unit]]
    ]
  end

  def cli do
    [
      preferred_envs: [ci: :test, precommit: :test]
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp aliases do
    [
      ci: ["format --check-formatted", "credo --strict", "test"],
      precommit: ["format", "credo --strict", "test"]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:crontab, "~> 1.1"},
      {:gen_stage, "~> 1.2"},
      {:libring, "~> 1.7"},
      {:pg_registry, "~> 0.2.2"},
      {:telemetry, "~> 1.0"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:styler, "~> 1.11", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end
end
