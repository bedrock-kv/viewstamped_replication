defmodule Bedrock.ViewstampedReplication.StateStore.InMemoryLog do
  @moduledoc """
  Log-based StateStore implementation.

  Stores full mutation history in memory. Supports incremental state transfer.
  Uses a map for O(1) access to entries by op number.

  This implementation is suitable for testing and development; production use
  would require a persistent backing store.
  """

  alias Bedrock.ViewstampedReplication.StateStore

  @type op_number :: non_neg_integer()
  @type mutation :: term()

  @type t :: %__MODULE__{
          entries: %{op_number() => mutation()},
          op_number: op_number(),
          commit_number: op_number(),
          view_number: non_neg_integer()
        }

  defstruct entries: %{},
            op_number: 0,
            commit_number: 0,
            view_number: 0

  @doc """
  Create a new empty in-memory log.
  """
  @spec new() :: t()
  def new, do: %__MODULE__{}

  @doc """
  Create a new in-memory log with a specified initial view number.
  """
  @spec new(view_number :: non_neg_integer()) :: t()
  def new(view_number), do: %__MODULE__{view_number: view_number}

  defimpl StateStore do
    # === Committed State ===

    def get_state(%{commit_number: 0}), do: []

    def get_state(store) do
      for n <- 1..store.commit_number,
          entry = Map.get(store.entries, n),
          entry != nil,
          do: {n, entry}
    end

    def get_state_from(store, from_op) when from_op > store.op_number do
      {:incremental, []}
    end

    def get_state_from(store, from_op) do
      entries =
        for n <- from_op..store.op_number,
            entry = Map.get(store.entries, n),
            entry != nil,
            do: {n, entry}

      {:incremental, entries}
    end

    def apply_state(store, []) do
      %{store | entries: %{}, commit_number: 0, op_number: 0}
    end

    def apply_state(store, entries) when is_list(entries) do
      new_entries = Map.new(entries)
      new_commit = entries |> Enum.map(&elem(&1, 0)) |> Enum.max()
      %{store | entries: new_entries, commit_number: new_commit, op_number: new_commit}
    end

    def apply_mutations(store, []), do: store

    def apply_mutations(store, mutations) do
      new_entries =
        Enum.reduce(mutations, store.entries, fn {n, entry}, acc ->
          Map.put(acc, n, entry)
        end)

      new_op =
        Enum.reduce(mutations, store.op_number, fn {n, _}, acc ->
          max(n, acc)
        end)

      %{store | entries: new_entries, op_number: new_op}
    end

    # === Pending Mutations ===

    def get_pending(%{commit_number: c, op_number: o}) when c >= o, do: []

    def get_pending(store) do
      for n <- (store.commit_number + 1)..store.op_number,
          entry = Map.get(store.entries, n),
          entry != nil,
          do: {n, entry}
    end

    def record_pending(store, op_number, mutation) when op_number == store.op_number + 1 do
      new_entries = Map.put(store.entries, op_number, mutation)
      {:ok, %{store | entries: new_entries, op_number: op_number}}
    end

    def record_pending(_store, _op_number, _mutation), do: {:error, :gap}

    def commit_through(store, target_commit, _execute_fn)
        when target_commit <= store.commit_number do
      {:ok, store, []}
    end

    def commit_through(store, target_commit, execute_fn) do
      actual_target = min(target_commit, store.op_number)
      start = store.commit_number + 1
      # Use explicit step of 1 to get empty range when start > actual_target
      ops_to_commit = start..actual_target//1

      {new_store, results} =
        Enum.reduce(ops_to_commit, {store, []}, fn op_num, {acc_store, acc_results} ->
          execute_single_commit(acc_store, acc_results, op_num, execute_fn)
        end)

      {:ok, new_store, Enum.reverse(results)}
    end

    defp execute_single_commit(store, results, op_num, execute_fn) do
      case Map.get(store.entries, op_num) do
        nil -> {store, results}
        mutation -> execute_mutation(store, results, op_num, mutation, execute_fn)
      end
    end

    defp execute_mutation(store, results, op_num, mutation, execute_fn) do
      {:ok, result} = execute_fn.(mutation)
      {%{store | commit_number: op_num}, [{op_num, result} | results]}
    end

    # === Metadata ===

    def op_number(store), do: store.op_number

    def commit_number(store), do: store.commit_number

    # === View Number Persistence ===

    def get_view_number(store), do: store.view_number

    def save_view_number(store, view_number),
      do: {:ok, %{store | view_number: view_number}}
  end
end
