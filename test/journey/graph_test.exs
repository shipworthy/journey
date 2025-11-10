defmodule Journey.GraphTest do
  use ExUnit.Case, async: true

  import Journey.Node

  defp create_graph() do
    Journey.new_graph(
      "horoscope workflow, success #{__MODULE__}",
      "1.0.3",
      [
        input(:first_name),
        input(:birth_day),
        input(:birth_month),
        compute(
          :astrological_sign,
          [:birth_month, :birth_day],
          fn %{birth_month: _birth_month, birth_day: _birth_day} ->
            Process.sleep(1000)
            {:ok, "Taurus"}
          end
        ),
        compute(
          :horoscope,
          [:first_name, :astrological_sign],
          fn %{first_name: name, astrological_sign: sign} ->
            Process.sleep(1000)
            {:ok, "🍪s await, #{sign} #{name}!"}
          end
        ),
        compute(
          :library_of_congress_record,
          [:horoscope, :first_name],
          fn %{
               horoscope: _horoscope,
               first_name: first_name
             } ->
            Process.sleep(1000)
            {:ok, "#{first_name}'s horoscope recorded in the library of congress."}
          end
        ),
        mutate(
          :erase_name,
          [:library_of_congress_record],
          fn %{first_name: first_name} ->
            Process.sleep(1000)
            {:ok, "<strike>#{first_name}</strike>."}
          end,
          mutates: :first_name
        )
      ]
    )
  end

  describe "new_graph" do
    test "sunny day" do
      graph = create_graph()
      assert graph.name == "horoscope workflow, success Elixir.Journey.GraphTest"
      assert is_list(graph.nodes)
    end

    test "mutates witout mutation" do
      assert_raise KeyError,
                   ~r/key :mutates not found in/,
                   fn ->
                     Journey.new_graph(
                       "horoscope workflow, mutates something unknown #{__MODULE__}",
                       "1.0.3",
                       [
                         input(:birth_day),
                         input(:birth_month),
                         mutate(
                           :astrological_sign,
                           [:birth_month, :birth_day],
                           fn %{
                                birth_month: _birth_month,
                                birth_day: _birth_day
                              } ->
                             Process.sleep(1000)
                             {:ok, "Taurus"}
                           end
                         )
                       ]
                     )
                   end
    end

    test "mutates something unknown" do
      assert_raise RuntimeError,
                   "Mutation node ':astrological_sign' mutates an unknown node ':something_wicked_this_way_comes'",
                   fn ->
                     Journey.new_graph(
                       "horoscope workflow, mutates something unknown #{__MODULE__}",
                       "1.0.3",
                       [
                         input(:first_name),
                         input(:birth_day),
                         input(:birth_month),
                         mutate(
                           :astrological_sign,
                           [:birth_month, :birth_day],
                           fn %{
                                birth_month: _birth_month,
                                birth_day: _birth_day
                              } ->
                             Process.sleep(1000)
                             {:ok, "Taurus"}
                           end,
                           mutates: :something_wicked_this_way_comes
                         )
                       ]
                     )
                   end
    end

    test "self-mutating" do
      assert_raise RuntimeError, "Mutation node ':astrological_sign' attempts to mutate itself", fn ->
        Journey.new_graph(
          "horoscope workflow, mutates itself #{__MODULE__}",
          "1.0.3",
          [
            input(:first_name),
            input(:birth_day),
            input(:birth_month),
            mutate(
              :astrological_sign,
              [:birth_month, :birth_day],
              fn %{
                   birth_month: _birth_month,
                   birth_day: _birth_day
                 } ->
                Process.sleep(1000)
                {:ok, "Taurus"}
              end,
              mutates: :astrological_sign
            )
          ]
        )
      end
    end

    test "bad dependencies" do
      assert_raise RuntimeError, "Unknown upstream nodes in input node ':astrological_sign': ssn", fn ->
        Journey.new_graph(
          "horoscope workflow, bad dependencies #{__MODULE__}",
          "1.0.3",
          [
            input(:first_name),
            input(:birth_day),
            input(:birth_month),
            compute(
              :astrological_sign,
              [:birth_month, :ssn, :birth_day],
              fn %{
                   birth_month: _birth_month,
                   birth_day: _birth_day
                 } ->
                Process.sleep(1000)
                {:ok, "Taurus"}
              end
            )
          ]
        )
      end
    end

    test "duplicate nodes" do
      assert_raise RuntimeError, "Duplicate node name in graph definition: :birth_day", fn ->
        Journey.new_graph(
          "horoscope workflow, duplicate nodes #{__MODULE__}",
          "1.0.3",
          [
            input(:first_name),
            input(:birth_day),
            input(:birth_month),
            input(:birth_day),
            compute(
              :astrological_sign,
              [:birth_month, :birth_day],
              fn %{
                   birth_month: _birth_month,
                   birth_day: _birth_day
                 } ->
                Process.sleep(1000)
                {:ok, "Taurus"}
              end
            )
          ]
        )
      end
    end
  end

  describe "ensure_known_input_node_name with graph struct" do
    test "returns :ok for valid input node" do
      graph = create_graph()

      assert Journey.Graph.Validations.ensure_known_input_node_name(graph, :first_name) == :ok
      assert Journey.Graph.Validations.ensure_known_input_node_name(graph, :birth_day) == :ok
      assert Journey.Graph.Validations.ensure_known_input_node_name(graph, :birth_month) == :ok
      # Also test the auto-added input nodes
      assert Journey.Graph.Validations.ensure_known_input_node_name(graph, :execution_id) == :ok
      assert Journey.Graph.Validations.ensure_known_input_node_name(graph, :last_updated_at) == :ok
    end

    test "raises error for non-input nodes" do
      graph = create_graph()

      assert_raise RuntimeError,
                   "':astrological_sign' is not a valid input node in graph 'horoscope workflow, success Elixir.Journey.GraphTest'.'1.0.3'. Valid input node names: [:birth_day, :birth_month, :execution_id, :first_name, :last_updated_at].",
                   fn ->
                     Journey.Graph.Validations.ensure_known_input_node_name(graph, :astrological_sign)
                   end

      assert_raise RuntimeError,
                   "':horoscope' is not a valid input node in graph 'horoscope workflow, success Elixir.Journey.GraphTest'.'1.0.3'. Valid input node names: [:birth_day, :birth_month, :execution_id, :first_name, :last_updated_at].",
                   fn ->
                     Journey.Graph.Validations.ensure_known_input_node_name(graph, :horoscope)
                   end
    end

    test "raises error for unknown nodes" do
      graph = create_graph()

      assert_raise RuntimeError,
                   "':unknown_node' is not a valid input node in graph 'horoscope workflow, success Elixir.Journey.GraphTest'.'1.0.3'. Valid input node names: [:birth_day, :birth_month, :execution_id, :first_name, :last_updated_at].",
                   fn ->
                     Journey.Graph.Validations.ensure_known_input_node_name(graph, :unknown_node)
                   end
    end
  end
end
