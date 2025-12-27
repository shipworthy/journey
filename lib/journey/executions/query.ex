defmodule Journey.Executions.Query do
  @moduledoc false

  alias Journey.Persistence.Schema.Execution
  import Ecto.Query

  def list(graph_name, graph_version, sort_by_fields, value_filters, limit, offset, include_archived?)
      when (is_nil(graph_name) or is_binary(graph_name)) and
             (is_nil(graph_version) or is_binary(graph_version)) and
             is_list(sort_by_fields) and
             is_list(value_filters) and
             is_number(limit) and
             is_number(offset) and
             is_boolean(include_archived?) do
    # Normalize and validate sort fields
    {normalized_fields, value_fields} = prepare_sort_fields(sort_by_fields, graph_name, graph_version)

    # Build and execute query
    from(e in Execution, limit: ^limit, offset: ^offset)
    |> filter_archived(include_archived?)
    |> apply_combined_sorting(normalized_fields, value_fields)
    |> filter_by_graph_name(graph_name)
    |> filter_by_graph_version(graph_version)
    |> add_filters(value_filters)
  end

  def count(graph_name, graph_version, value_filters, include_archived?)
      when (is_nil(graph_name) or is_binary(graph_name)) and
             (is_nil(graph_version) or is_binary(graph_version)) and
             is_list(value_filters) and
             is_boolean(include_archived?) do
    # Build and execute count query (no sorting, limit, or offset needed)
    from(e in Execution)
    |> filter_archived(include_archived?)
    |> filter_by_graph_name(graph_name)
    |> filter_by_graph_version(graph_version)
    |> add_count_filters(value_filters)
  end

  defp prepare_sort_fields(sort_by_fields, graph_name, graph_version) do
    normalized_fields = normalize_sort_fields(sort_by_fields)
    value_fields = extract_value_fields(normalized_fields)

    # Validate value fields exist in the graph (if graph_name is provided)
    if graph_name != nil and graph_version != nil and value_fields != [] do
      field_names = Enum.map(value_fields, fn {field, _direction} -> field end)
      Journey.Graph.Validations.ensure_known_node_names(graph_name, graph_version, field_names)
    end

    {normalized_fields, value_fields}
  end

  defp filter_archived(query, true), do: query
  defp filter_archived(query, false), do: from(e in query, where: is_nil(e.archived_at))

  # Get execution table fields dynamically from schema
  defp execution_fields do
    Journey.Persistence.Schema.Execution.__schema__(:fields)
  end

  defp extract_value_fields(normalized_fields) do
    execution_field_set = MapSet.new(execution_fields())

    normalized_fields
    |> Enum.filter(fn {field, _direction} -> field not in execution_field_set end)
  end

  defp apply_combined_sorting(query, all_fields, _value_fields) when all_fields == [] do
    query
  end

  defp apply_combined_sorting(query, all_fields, value_fields) when value_fields == [] do
    # Only execution fields - simple ORDER BY
    execution_order_by = Enum.map(all_fields, fn {field, direction} -> {direction, field} end)
    from(e in query, order_by: ^execution_order_by)
  end

  defp apply_combined_sorting(query, all_fields, value_fields) do
    # Mixed execution and value fields - need JOINs for value fields
    query_with_joins = add_value_joins(query, value_fields)
    order_by_list = build_order_by_list(all_fields, value_fields)
    from(e in query_with_joins, order_by: ^order_by_list)
  end

  defp add_value_joins(query, value_fields) do
    value_fields
    |> Enum.with_index()
    |> Enum.reduce(query, fn {{node_name, _direction}, index}, acc_query ->
      alias_name = String.to_atom("v#{index}")

      from(e in acc_query,
        left_join: v in Journey.Persistence.Schema.Execution.Value,
        as: ^alias_name,
        on: v.execution_id == e.id and v.node_name == ^Atom.to_string(node_name)
      )
    end)
  end

  defp build_order_by_list(all_fields, value_fields) do
    execution_field_set = MapSet.new(execution_fields())
    value_field_indexes = build_value_field_index_map(value_fields)

    Enum.map(all_fields, fn {field, direction} ->
      if field in execution_field_set do
        {direction, field}
      else
        index = Map.get(value_field_indexes, field)
        alias_name = String.to_atom("v#{index}")
        {direction, dynamic([{^alias_name, v}], v.node_value)}
      end
    end)
  end

  defp build_value_field_index_map(value_fields) do
    value_fields
    |> Enum.with_index()
    |> Map.new(fn {{field, _}, index} -> {field, index} end)
  end

  # Normalize sort fields to support both atom and tuple syntax
  defp normalize_sort_fields(fields) when is_list(fields) do
    Enum.map(fields, fn
      # Atom format: bare atom defaults to :asc
      atom when is_atom(atom) ->
        {atom, :asc}

      # Tuple format: {field, direction}
      {field, direction} when is_atom(field) and direction in [:asc, :desc] ->
        {field, direction}

      # Invalid format
      invalid ->
        raise ArgumentError,
              "Invalid sort field format: #{inspect(invalid)}. Expected atom or {field, :asc/:desc} tuple."
    end)
  end

  defp filter_by_graph_name(query, nil), do: query
  defp filter_by_graph_name(query, graph_name), do: from(e in query, where: e.graph_name == ^graph_name)

  defp filter_by_graph_version(query, nil), do: query
  defp filter_by_graph_version(query, graph_version), do: from(e in query, where: e.graph_version == ^graph_version)

  # Database-level filtering for simple value comparisons
  defp add_filters(query, []), do: query |> preload_and_convert()

  defp add_filters(query, value_filters) when is_list(value_filters) do
    # Validate all filters are database-compatible before proceeding
    Enum.each(value_filters, &validate_db_filter/1)

    query
    |> apply_db_value_filters(value_filters)
    |> preload_and_convert()
  end

  # Database-level filtering for counting (no preloading or conversion needed)
  defp add_count_filters(query, []), do: Journey.Repo.aggregate(query, :count, :id)

  defp add_count_filters(query, value_filters) when is_list(value_filters) do
    # Validate all filters are database-compatible before proceeding
    Enum.each(value_filters, &validate_db_filter/1)

    query
    |> apply_db_value_filters(value_filters)
    |> Journey.Repo.aggregate(:count, :id)
  end

  # Validate filters are compatible with database-level filtering
  defp validate_db_filter({node_name, :list_contains, value})
       when is_atom(node_name) and (is_binary(value) or is_integer(value)) do
    :ok
  end

  defp validate_db_filter({node_name, op, value})
       when is_atom(node_name) and op in [:eq, :neq, :lt, :lte, :gt, :gte, :in, :not_in, :contains, :icontains] do
    # Additional validation for the value type
    if primitive_value?(value) do
      :ok
    else
      raise ArgumentError,
            "Unsupported value type for database filtering: #{inspect(value)}. " <>
              "Only strings, numbers, booleans, nil, and lists of primitives are supported."
    end
  end

  defp validate_db_filter({node_name, op})
       when is_atom(node_name) and op in [:is_nil, :is_not_nil, :is_set, :is_not_set],
       do: :ok

  # Crash with clear error message for unsupported filters
  defp validate_db_filter(filter) do
    raise ArgumentError,
          "Unsupported filter for database-level filtering: #{inspect(filter)}. " <>
            "Only simple comparisons on strings, numbers, booleans, nil, and lists of primitives are supported. " <>
            "Custom functions are not supported."
  end

  # Check if a value is a primitive type that can be handled at database level
  defp primitive_value?(value) when is_binary(value) or is_number(value) or is_boolean(value) or is_nil(value),
    do: true

  defp primitive_value?(values) when is_list(values) do
    Enum.all?(values, fn v -> is_binary(v) or is_number(v) or is_boolean(v) or is_nil(v) end)
  end

  defp primitive_value?(_), do: false

  # Escape special characters in LIKE patterns to treat them as literals
  defp escape_like_pattern(pattern) do
    pattern
    # Escape backslash first
    |> String.replace("\\", "\\\\")
    # Escape percent
    |> String.replace("%", "\\%")
    # Escape underscore
    |> String.replace("_", "\\_")
  end

  # Apply database-level value filtering using JOINs and JSONB queries
  defp apply_db_value_filters(query, value_filters) do
    # Split filters by type for different handling strategies
    {existence_filters, comparison_filters} =
      Enum.split_with(value_filters, fn
        {_, op} when op in [:is_nil, :is_not_nil, :is_set, :is_not_set] -> true
        _ -> false
      end)

    # Apply comparison filters with JOINs (leveraging unique execution_id, node_name)
    query_with_comparisons =
      Enum.reduce(comparison_filters, query, fn {node_name, op, value}, acc_query ->
        apply_comparison_filter(acc_query, node_name, op, value)
      end)

    # Apply existence filters using anti-join and inner join patterns
    Enum.reduce(existence_filters, query_with_comparisons, fn {node_name, op}, acc_query ->
      node_name_str = Atom.to_string(node_name)

      case op do
        :is_nil ->
          from(e in acc_query,
            left_join: v in Journey.Persistence.Schema.Execution.Value,
            on: v.execution_id == e.id and v.node_name == ^node_name_str,
            where: is_nil(v.id)
          )

        :is_not_nil ->
          from(e in acc_query,
            join: v in Journey.Persistence.Schema.Execution.Value,
            on: v.execution_id == e.id and v.node_name == ^node_name_str
          )

        :is_set ->
          from(e in acc_query,
            join: v in Journey.Persistence.Schema.Execution.Value,
            on: v.execution_id == e.id and v.node_name == ^node_name_str,
            where: not is_nil(v.set_time)
          )

        :is_not_set ->
          from(e in acc_query,
            left_join: v in Journey.Persistence.Schema.Execution.Value,
            on: v.execution_id == e.id and v.node_name == ^node_name_str,
            where: is_nil(v.set_time)
          )
      end
    end)
  end

  # Apply individual comparison filters with direct JSONB conditions
  defp apply_comparison_filter(query, node_name, :eq, value) do
    from(e in query,
      join: v in Journey.Persistence.Schema.Execution.Value,
      on: v.execution_id == e.id and v.node_name == ^Atom.to_string(node_name),
      where: v.node_value == ^value
    )
  end

  defp apply_comparison_filter(query, node_name, :neq, value) do
    from(e in query,
      join: v in Journey.Persistence.Schema.Execution.Value,
      on: v.execution_id == e.id and v.node_name == ^Atom.to_string(node_name),
      where: v.node_value != ^value
    )
  end

  defp apply_comparison_filter(query, node_name, :lt, value) when is_number(value) do
    from(e in query,
      join: v in Journey.Persistence.Schema.Execution.Value,
      on: v.execution_id == e.id and v.node_name == ^Atom.to_string(node_name),
      where:
        fragment(
          "CASE WHEN jsonb_typeof(?) = 'number' THEN (?)::numeric < ? ELSE false END",
          v.node_value,
          v.node_value,
          ^value
        )
    )
  end

  defp apply_comparison_filter(query, node_name, :lt, value) when is_binary(value) do
    from(e in query,
      join: v in Journey.Persistence.Schema.Execution.Value,
      on: v.execution_id == e.id and v.node_name == ^Atom.to_string(node_name),
      where: fragment("jsonb_typeof(?) = 'string' AND (? #>> '{}') < ?", v.node_value, v.node_value, ^value)
    )
  end

  defp apply_comparison_filter(query, node_name, :lte, value) when is_number(value) do
    from(e in query,
      join: v in Journey.Persistence.Schema.Execution.Value,
      on: v.execution_id == e.id and v.node_name == ^Atom.to_string(node_name),
      where:
        fragment(
          "CASE WHEN jsonb_typeof(?) = 'number' THEN (?)::numeric <= ? ELSE false END",
          v.node_value,
          v.node_value,
          ^value
        )
    )
  end

  defp apply_comparison_filter(query, node_name, :lte, value) when is_binary(value) do
    from(e in query,
      join: v in Journey.Persistence.Schema.Execution.Value,
      on: v.execution_id == e.id and v.node_name == ^Atom.to_string(node_name),
      where: fragment("jsonb_typeof(?) = 'string' AND (? #>> '{}') <= ?", v.node_value, v.node_value, ^value)
    )
  end

  defp apply_comparison_filter(query, node_name, :gt, value) when is_number(value) do
    from(e in query,
      join: v in Journey.Persistence.Schema.Execution.Value,
      on: v.execution_id == e.id and v.node_name == ^Atom.to_string(node_name),
      where:
        fragment(
          "CASE WHEN jsonb_typeof(?) = 'number' THEN (?)::numeric > ? ELSE false END",
          v.node_value,
          v.node_value,
          ^value
        )
    )
  end

  defp apply_comparison_filter(query, node_name, :gt, value) when is_binary(value) do
    from(e in query,
      join: v in Journey.Persistence.Schema.Execution.Value,
      on: v.execution_id == e.id and v.node_name == ^Atom.to_string(node_name),
      where: fragment("jsonb_typeof(?) = 'string' AND (? #>> '{}') > ?", v.node_value, v.node_value, ^value)
    )
  end

  defp apply_comparison_filter(query, node_name, :gte, value) when is_number(value) do
    from(e in query,
      join: v in Journey.Persistence.Schema.Execution.Value,
      on: v.execution_id == e.id and v.node_name == ^Atom.to_string(node_name),
      where:
        fragment(
          "CASE WHEN jsonb_typeof(?) = 'number' THEN (?)::numeric >= ? ELSE false END",
          v.node_value,
          v.node_value,
          ^value
        )
    )
  end

  defp apply_comparison_filter(query, node_name, :gte, value) when is_binary(value) do
    from(e in query,
      join: v in Journey.Persistence.Schema.Execution.Value,
      on: v.execution_id == e.id and v.node_name == ^Atom.to_string(node_name),
      where: fragment("jsonb_typeof(?) = 'string' AND (? #>> '{}') >= ?", v.node_value, v.node_value, ^value)
    )
  end

  defp apply_comparison_filter(query, node_name, :in, values) when is_list(values) do
    from(e in query,
      join: v in Journey.Persistence.Schema.Execution.Value,
      on: v.execution_id == e.id and v.node_name == ^Atom.to_string(node_name),
      where: v.node_value in ^values
    )
  end

  defp apply_comparison_filter(query, node_name, :not_in, values) when is_list(values) do
    from(e in query,
      join: v in Journey.Persistence.Schema.Execution.Value,
      on: v.execution_id == e.id and v.node_name == ^Atom.to_string(node_name),
      where: v.node_value not in ^values
    )
  end

  defp apply_comparison_filter(query, node_name, :contains, pattern) when is_binary(pattern) do
    escaped_pattern = escape_like_pattern(pattern)
    like_pattern = "%#{escaped_pattern}%"

    from(e in query,
      join: v in Journey.Persistence.Schema.Execution.Value,
      on: v.execution_id == e.id and v.node_name == ^Atom.to_string(node_name),
      where: fragment("jsonb_typeof(?) = 'string' AND (? #>> '{}') LIKE ?", v.node_value, v.node_value, ^like_pattern)
    )
  end

  defp apply_comparison_filter(query, node_name, :icontains, pattern) when is_binary(pattern) do
    escaped_pattern = escape_like_pattern(pattern)
    like_pattern = "%#{escaped_pattern}%"

    from(e in query,
      join: v in Journey.Persistence.Schema.Execution.Value,
      on: v.execution_id == e.id and v.node_name == ^Atom.to_string(node_name),
      where: fragment("jsonb_typeof(?) = 'string' AND (? #>> '{}') ILIKE ?", v.node_value, v.node_value, ^like_pattern)
    )
  end

  defp apply_comparison_filter(query, node_name, :list_contains, element)
       when is_binary(element) or is_integer(element) do
    from(e in query,
      join: v in Journey.Persistence.Schema.Execution.Value,
      on: v.execution_id == e.id and v.node_name == ^Atom.to_string(node_name),
      where:
        fragment(
          "jsonb_typeof(?) = 'array' AND ? @> ?",
          v.node_value,
          v.node_value,
          ^element
        )
    )
  end

  # Helper to preload data and convert node names to atoms
  defp preload_and_convert(query) do
    from(e in query, preload: [:values, :computations])
    |> Journey.Repo.all()
    |> Enum.map(&Journey.Executions.convert_node_names_to_atoms/1)
  end
end
