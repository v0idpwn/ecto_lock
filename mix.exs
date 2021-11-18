defmodule EctoLock.MixProject do
  use Mix.Project

  def project do
    [
      app: :ecto_lock,
      version: "0.1.1",
      elixir: "~> 1.11",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "Helpers for advisory locks with postgresql and ecto",
      package: [
        name: "ecto_lock",
        licenses: ["Apache-2.0"],
        links: %{"GitHub" => "https://github.com/v0idpwn/ecto_lock"}
      ],
      name: "EctoLock",
      source_url: "https://github.com/v0idpwn/ecto_lock",
      homepage_url: "https://github.com/v0idpwn/ecto_lock"
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ecto_sql, "~> 3.7"},
      {:postgrex, "~> 0.15"},
      {:ex_doc, "~> 0.25", only: :docs, runtime: false}
    ]
  end
end
