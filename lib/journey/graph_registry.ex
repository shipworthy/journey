defmodule Journey.GraphRegistry do
  @moduledoc """
  Provides @registered_graph attribute for auto-registering graph functions.

  ## Usage

      defmodule MyGraph do
        use Journey.GraphRegistry
        import Journey.Node
        
        @registered_graph
        def my_graph() do
          Journey.new_graph("my graph", "v1.0.0", [...])
        end
      end

  The graph will be automatically registered at application startup.
  """

  defmacro __using__(_opts) do
    quote do
      Module.register_attribute(__MODULE__, :registered_graph, accumulate: false)
      Module.register_attribute(__MODULE__, :registered_graph_functions, accumulate: true)
      @on_definition Journey.GraphRegistry
      @before_compile Journey.GraphRegistry
    end
  end

  def __on_definition__(env, _kind, name, args, _guards, _body) do
    if Module.get_attribute(env.module, :registered_graph) do
      Module.put_attribute(env.module, :registered_graph_functions, {name, length(args)})
      Module.delete_attribute(env.module, :registered_graph)
    end
  end

  defmacro __before_compile__(env) do
    functions = Module.get_attribute(env.module, :registered_graph_functions, [])

    quote do
      def __registered_graphs__, do: unquote(Enum.reverse(functions))
    end
  end

  @doc """
  Returns all registered graph functions from compiled modules.
  """
  def all_registered_graphs do
    require Logger

    # Get all modules from the journey application
    case :application.get_key(:journey, :modules) do
      {:ok, modules} ->
        Logger.info("Found #{length(modules)} Journey modules")

        registered_modules =
          modules
          |> Enum.filter(&function_exported?(&1, :__registered_graphs__, 0))

        Enum.each(registered_modules, fn module ->
          Logger.info("Module #{module} has registered graphs")
        end)

        Enum.flat_map(registered_modules, fn module ->
          for {func_name, 0} <- module.__registered_graphs__() do
            {module, func_name}
          end
        end)

      _ ->
        Logger.info("Could not get Journey modules")
        []
    end
  end
end
