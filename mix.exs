defmodule Stagehand.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/twinn/stagehand"

  def project do
    [
      app: :stagehand,
      version: @version,
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: description(),
      package: package(),
      docs: docs(),
      elixirc_paths: elixirc_paths(Mix.env()),
      aliases: aliases(),
      dialyzer: [plt_add_apps: [:ex_unit]],
      source_url: @source_url
    ]
  end

  def cli do
    [
      preferred_envs: [precommit: :test]
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Stagehand.Application, []}
    ]
  end

  defp aliases do
    [
      precommit: ["format", "credo --strict", "test"]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp description do
    "An in-memory, GenStage-based background job processing library for Elixir."
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url},
      files: ~w(lib mix.exs README.md LICENSE CHANGELOG.md)
    ]
  end

  defp docs do
    [
      main: "Stagehand",
      extras: ["README.md", "CHANGELOG.md", "LICENSE"]
    ]
  end

  defp deps do
    [
      {:crontab, "~> 1.1"},
      {:gen_stage, "~> 1.2"},
      {:highlander, "~> 0.2"},
      {:pg_registry, "~> 0.4"},
      {:telemetry, "~> 1.0"},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:styler, "~> 1.11", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end
end
