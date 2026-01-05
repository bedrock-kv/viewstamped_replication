defmodule Bedrockviewstamped_replication.MixProject do
  use Mix.Project

  def project do
    [
      app: :bedrock_viewstamped_replication,
      version: "0.2.3",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.json": :test
      ],
      elixirc_paths: elixirc_paths(Mix.env()),
      description:
        "An implementation of the Viewstamped Replication consensus algorithm in Elixir that doesn't force opinions. Bake the protocol into your own GenServers, send messages and manage logs how you like.",
      source_url: "https://github.com/bedrock-kv/viewstamped_replication",
      homepage_url: "https://github.com/bedrock-kv/viewstamped_replication",
      package: package(),
      docs: docs()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    []
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:telemetry, "~> 1.3.0"}
    ]
    |> add_deps_for_dev_and_test()
  end

  def add_deps_for_dev_and_test(deps) do
    deps ++
      [
        {:credo, "~> 1.6", only: [:dev, :test], runtime: false},
        {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
        {:ex_doc, "~> 0.34", only: :dev, runtime: false},
        {:mix_test_watch, "~> 1.0", only: [:dev, :test], runtime: false},
        {:mox, "~> 1.1", only: :test},
        {:excoveralls, "~> 0.18", only: :test}
      ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp package do
    [
      licenses: ["MIT"],
      links: %{
        "GitHub" => "https://github.com/bedrock-kv/viewstamped_replication",
        "Changelog" =>
          "https://github.com/bedrock-kv/viewstamped_replication/blob/main/CHANGELOG.md"
      },
      files: ~w(lib .formatter.exs mix.exs README.md LICENSE CHANGELOG.md)
    ]
  end

  defp docs do
    [
      main: "Bedrock.ViewstampedReplication",
      extras: ["README.md", "CHANGELOG.md"]
    ]
  end
end
