# Used by "mix format"
[
  line_length: 120,
  import_deps: [:ecto, :ecto_sql],
  subdirectories: ["priv/*/migrations"],
  inputs: ["{mix,.formatter}.exs", "{config,lib,test,test_load}/**/*.{ex,exs}", "priv/*/seeds.exs"]
]
