defmodule Bedrock.ViewstampedReplication.StateStore.SnapshotStoreTest do
  @moduledoc false
  use ExUnit.Case, async: false

  alias Bedrock.ViewstampedReplication.StateStore
  alias Bedrock.ViewstampedReplication.StateStore.SnapshotStore

  # Mock callbacks module for testing - uses process dictionary for isolation
  defmodule MockCallbacks do
    @moduledoc false

    def get_snapshot do
      snapshot = Process.get(:mock_snapshot, %{})
      {:ok, snapshot}
    end

    def apply_snapshot(snapshot) do
      Process.put(:mock_snapshot, snapshot)
      :ok
    end

    def persist_view_number(view_number) do
      Process.put(:mock_view_number, view_number)
      :ok
    end

    def get_persisted_view_number do
      Process.get(:mock_view_number, 0)
    end
  end

  setup do
    # Reset process dictionary state
    Process.put(:mock_snapshot, %{})
    Process.put(:mock_view_number, 0)
    :ok
  end

  defp set_mock_snapshot(snapshot) do
    Process.put(:mock_snapshot, snapshot)
  end

  describe "new/1" do
    test "creates a new SnapshotStore with callbacks module" do
      store = SnapshotStore.new(MockCallbacks)

      assert %SnapshotStore{} = store
      assert store.callbacks == MockCallbacks
      assert store.op_number == 0
      assert store.commit_number == 0
      assert store.pending == %{}
    end

    test "reads initial view number from callbacks" do
      Process.put(:mock_view_number, 5)
      _store = SnapshotStore.new(MockCallbacks)

      # The view number is read but stored in callbacks, not the struct
      assert MockCallbacks.get_persisted_view_number() == 5
    end
  end

  describe "get_state/1" do
    test "delegates to callbacks.get_snapshot/0" do
      set_mock_snapshot(%{key: "value", count: 42})
      store = SnapshotStore.new(MockCallbacks)

      state = StateStore.get_state(store)

      assert state == %{key: "value", count: 42}
    end
  end

  describe "get_state_from/2" do
    test "always returns {:full, snapshot} since snapshots don't support incremental" do
      set_mock_snapshot(%{data: "test"})
      store = SnapshotStore.new(MockCallbacks)

      result = StateStore.get_state_from(store, 1)

      assert {:full, %{data: "test"}} = result
    end

    test "ignores from_op parameter" do
      set_mock_snapshot(%{data: "test"})
      store = SnapshotStore.new(MockCallbacks)

      # Different from_op values should all return the same full snapshot
      assert StateStore.get_state_from(store, 1) == StateStore.get_state_from(store, 100)
    end
  end

  describe "apply_state/2" do
    test "delegates to callbacks.apply_snapshot/1" do
      store = SnapshotStore.new(MockCallbacks)

      new_store = StateStore.apply_state(store, %{new: "state"})

      assert new_store == store
      assert MockCallbacks.get_snapshot() == {:ok, %{new: "state"}}
    end
  end

  describe "apply_mutations/2" do
    test "returns store unchanged for empty mutations" do
      store = SnapshotStore.new(MockCallbacks)

      result = StateStore.apply_mutations(store, [])

      assert result == store
    end

    test "stores mutations in pending map" do
      store = SnapshotStore.new(MockCallbacks)

      mutations = [{1, {:c1, 1, :op1}}, {2, {:c1, 2, :op2}}]
      new_store = StateStore.apply_mutations(store, mutations)

      assert new_store.pending == %{1 => {:c1, 1, :op1}, 2 => {:c1, 2, :op2}}
      assert new_store.op_number == 2
    end

    test "updates op_number to highest mutation number" do
      store = SnapshotStore.new(MockCallbacks)

      mutations = [{3, :op3}, {1, :op1}, {5, :op5}]
      new_store = StateStore.apply_mutations(store, mutations)

      assert new_store.op_number == 5
    end
  end

  describe "get_pending/1" do
    test "returns empty list when no pending mutations" do
      store = SnapshotStore.new(MockCallbacks)

      assert StateStore.get_pending(store) == []
    end

    test "returns pending mutations sorted by op_number" do
      store = SnapshotStore.new(MockCallbacks)

      mutations = [{3, :op3}, {1, :op1}, {2, :op2}]
      new_store = StateStore.apply_mutations(store, mutations)

      pending = StateStore.get_pending(new_store)

      assert pending == [{1, :op1}, {2, :op2}, {3, :op3}]
    end
  end

  describe "record_pending/3" do
    test "records mutation when op_number is next expected" do
      store = SnapshotStore.new(MockCallbacks)

      {:ok, new_store} = StateStore.record_pending(store, 1, :mutation1)

      assert new_store.pending == %{1 => :mutation1}
      assert new_store.op_number == 1
    end

    test "allows sequential recording" do
      store = SnapshotStore.new(MockCallbacks)

      {:ok, store} = StateStore.record_pending(store, 1, :m1)
      {:ok, store} = StateStore.record_pending(store, 2, :m2)
      {:ok, store} = StateStore.record_pending(store, 3, :m3)

      assert store.op_number == 3
      assert map_size(store.pending) == 3
    end

    test "returns error for gap in sequence" do
      store = SnapshotStore.new(MockCallbacks)

      result = StateStore.record_pending(store, 2, :mutation2)

      assert result == {:error, :gap}
    end

    test "returns error for duplicate op_number" do
      store = SnapshotStore.new(MockCallbacks)
      {:ok, store} = StateStore.record_pending(store, 1, :m1)

      result = StateStore.record_pending(store, 1, :m1_again)

      assert result == {:error, :gap}
    end
  end

  describe "commit_through/3" do
    test "returns unchanged store when target <= commit_number" do
      store = %{SnapshotStore.new(MockCallbacks) | commit_number: 5}

      {:ok, new_store, results} = StateStore.commit_through(store, 3, fn _ -> {:ok, :result} end)

      assert new_store == store
      assert results == []
    end

    test "executes pending mutations up to target" do
      store = SnapshotStore.new(MockCallbacks)
      {:ok, store} = StateStore.record_pending(store, 1, {:c1, 1, :op1})
      {:ok, store} = StateStore.record_pending(store, 2, {:c1, 2, :op2})

      execute_fn = fn mutation -> {:ok, {:executed, mutation}} end

      {:ok, new_store, results} = StateStore.commit_through(store, 2, execute_fn)

      assert new_store.commit_number == 2
      assert length(results) == 2
      assert {1, {:executed, {:c1, 1, :op1}}} in results
      assert {2, {:executed, {:c1, 2, :op2}}} in results
    end

    test "removes executed mutations from pending" do
      store = SnapshotStore.new(MockCallbacks)
      {:ok, store} = StateStore.record_pending(store, 1, :op1)
      {:ok, store} = StateStore.record_pending(store, 2, :op2)
      {:ok, store} = StateStore.record_pending(store, 3, :op3)

      {:ok, new_store, _results} = StateStore.commit_through(store, 2, fn _ -> {:ok, :r} end)

      # Op 1 and 2 committed and removed, op 3 still pending
      assert new_store.pending == %{3 => :op3}
    end

    test "skips missing ops in pending" do
      store = %{SnapshotStore.new(MockCallbacks) | pending: %{2 => :op2}, op_number: 2}

      {:ok, new_store, results} = StateStore.commit_through(store, 2, fn _ -> {:ok, :r} end)

      # Op 1 was missing, only op 2 executed
      assert length(results) == 1
      assert {2, :r} in results
      assert new_store.commit_number == 2
    end

    test "limits commit to op_number even if target is higher" do
      store = SnapshotStore.new(MockCallbacks)
      {:ok, store} = StateStore.record_pending(store, 1, :op1)

      {:ok, new_store, results} = StateStore.commit_through(store, 10, fn _ -> {:ok, :r} end)

      assert new_store.commit_number == 1
      assert length(results) == 1
    end
  end

  describe "op_number/1" do
    test "returns current op_number" do
      store = SnapshotStore.new(MockCallbacks)
      {:ok, store} = StateStore.record_pending(store, 1, :m1)
      {:ok, store} = StateStore.record_pending(store, 2, :m2)

      assert StateStore.op_number(store) == 2
    end
  end

  describe "commit_number/1" do
    test "returns current commit_number" do
      store = SnapshotStore.new(MockCallbacks)
      {:ok, store} = StateStore.record_pending(store, 1, :m1)
      {:ok, store, _} = StateStore.commit_through(store, 1, fn _ -> {:ok, :r} end)

      assert StateStore.commit_number(store) == 1
    end
  end

  describe "get_view_number/1" do
    test "delegates to callbacks.get_persisted_view_number/0" do
      MockCallbacks.persist_view_number(7)
      store = SnapshotStore.new(MockCallbacks)

      assert StateStore.get_view_number(store) == 7
    end
  end

  describe "save_view_number/2" do
    test "delegates to callbacks.persist_view_number/1" do
      store = SnapshotStore.new(MockCallbacks)

      {:ok, new_store} = StateStore.save_view_number(store, 10)

      assert new_store == store
      assert MockCallbacks.get_persisted_view_number() == 10
    end
  end
end
