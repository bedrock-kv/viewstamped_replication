defmodule Bedrock.ViewstampedReplication.StateStore.SnapshotStore do
  @moduledoc """
  Snapshot-based StateStore implementation.

  Delegates state management to application callbacks. Does not store mutation
  history - operations are executed immediately and only current state exists.

  Does not support incremental state transfer; always returns full snapshots.

  ## Callbacks Module

  The callbacks module must implement:
  - `get_snapshot/0` - Returns `{:ok, snapshot}` with current application state
  - `apply_snapshot/1` - Applies a received snapshot, returns `:ok`
  - `persist_view_number/1` - Persists view number, returns `:ok`
  - `get_persisted_view_number/0` - Returns persisted view number
  """

  alias Bedrock.ViewstampedReplication.StateStore

  @type op_number :: non_neg_integer()
  @type mutation :: term()

  @type t :: %__MODULE__{
          pending: %{op_number() => mutation()},
          op_number: op_number(),
          commit_number: op_number(),
          callbacks: module()
        }

  defstruct pending: %{},
            op_number: 0,
            commit_number: 0,
            callbacks: nil

  @doc """
  Create a new SnapshotStore with the given callbacks module.
  """
  @spec new(module()) :: t()
  def new(callbacks) do
    view_number = callbacks.get_persisted_view_number()

    %__MODULE__{
      callbacks: callbacks,
      op_number: 0,
      commit_number: 0,
      pending: %{}
    }
    |> then(fn store -> %{store | op_number: 0, commit_number: 0} end)
    |> tap(fn _ -> view_number end)
  end

  defimpl StateStore do
    # === Committed State ===

    def get_state(store) do
      {:ok, snapshot} = store.callbacks.get_snapshot()
      snapshot
    end

    def get_state_from(store, _from_op) do
      {:ok, snapshot} = store.callbacks.get_snapshot()
      {:full, snapshot}
    end

    def apply_state(store, snapshot) do
      :ok = store.callbacks.apply_snapshot(snapshot)
      store
    end

    def apply_mutations(store, []), do: store

    def apply_mutations(store, mutations) do
      new_pending =
        Enum.reduce(mutations, store.pending, fn {n, entry}, acc ->
          Map.put(acc, n, entry)
        end)

      new_op =
        Enum.reduce(mutations, store.op_number, fn {n, _}, acc ->
          max(n, acc)
        end)

      %{store | pending: new_pending, op_number: new_op}
    end

    # === Pending Mutations ===

    def get_pending(store) do
      store.pending
      |> Map.to_list()
      |> Enum.sort_by(&elem(&1, 0))
    end

    def record_pending(store, op_number, mutation) when op_number == store.op_number + 1 do
      new_pending = Map.put(store.pending, op_number, mutation)
      {:ok, %{store | pending: new_pending, op_number: op_number}}
    end

    def record_pending(_store, _op_number, _mutation), do: {:error, :gap}

    def commit_through(store, target_commit, _execute_fn)
        when target_commit <= store.commit_number do
      {:ok, store, []}
    end

    def commit_through(store, target_commit, execute_fn) do
      actual_target = min(target_commit, store.op_number)
      ops_to_commit = (store.commit_number + 1)..actual_target

      {new_pending, new_commit, results} =
        Enum.reduce(ops_to_commit, {store.pending, store.commit_number, []}, fn op_num, acc ->
          execute_and_remove(acc, op_num, execute_fn)
        end)

      new_store = %{store | pending: new_pending, commit_number: new_commit}
      {:ok, new_store, Enum.reverse(results)}
    end

    defp execute_and_remove({pending, commit, results}, op_num, execute_fn) do
      case Map.get(pending, op_num) do
        nil ->
          {pending, commit, results}

        mutation ->
          {:ok, result} = execute_fn.(mutation)
          new_pending = Map.delete(pending, op_num)
          {new_pending, op_num, [{op_num, result} | results]}
      end
    end

    # === Metadata ===

    def op_number(store), do: store.op_number

    def commit_number(store), do: store.commit_number

    # === View Number Persistence ===

    def get_view_number(store) do
      store.callbacks.get_persisted_view_number()
    end

    def save_view_number(store, view_number) do
      :ok = store.callbacks.persist_view_number(view_number)
      {:ok, store}
    end
  end
end
