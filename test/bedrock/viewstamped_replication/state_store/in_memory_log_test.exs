defmodule Bedrock.ViewstampedReplication.StateStore.InMemoryLogTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Bedrock.ViewstampedReplication.StateStore
  alias Bedrock.ViewstampedReplication.StateStore.InMemoryLog

  describe "new/0" do
    test "creates empty log with default values" do
      log = InMemoryLog.new()

      assert %InMemoryLog{} = log
      assert log.entries == %{}
      assert log.op_number == 0
      assert log.commit_number == 0
      assert log.view_number == 0
    end
  end

  describe "new/1" do
    test "creates log with specified view number" do
      log = InMemoryLog.new(5)

      assert log.view_number == 5
      assert log.entries == %{}
      assert log.op_number == 0
      assert log.commit_number == 0
    end
  end

  describe "get_state/1" do
    test "returns empty list when commit_number is 0" do
      log = InMemoryLog.new()

      assert StateStore.get_state(log) == []
    end

    test "returns committed entries in order" do
      log = %InMemoryLog{
        entries: %{1 => :op1, 2 => :op2, 3 => :op3},
        commit_number: 2,
        op_number: 3
      }

      state = StateStore.get_state(log)

      assert state == [{1, :op1}, {2, :op2}]
    end

    test "skips missing entries in range" do
      log = %InMemoryLog{
        entries: %{1 => :op1, 3 => :op3},
        commit_number: 3,
        op_number: 3
      }

      state = StateStore.get_state(log)

      # Entry 2 is missing, so it's skipped
      assert state == [{1, :op1}, {3, :op3}]
    end
  end

  describe "get_state_from/2" do
    test "returns incremental entries from specified op" do
      log = %InMemoryLog{
        entries: %{1 => :op1, 2 => :op2, 3 => :op3},
        op_number: 3,
        commit_number: 3
      }

      result = StateStore.get_state_from(log, 2)

      assert {:incremental, [{2, :op2}, {3, :op3}]} = result
    end

    test "returns empty incremental when from_op exceeds op_number" do
      log = %InMemoryLog{
        entries: %{1 => :op1},
        op_number: 1
      }

      result = StateStore.get_state_from(log, 5)

      assert {:incremental, []} = result
    end

    test "returns all entries when from_op is 1" do
      log = %InMemoryLog{
        entries: %{1 => :op1, 2 => :op2},
        op_number: 2
      }

      result = StateStore.get_state_from(log, 1)

      assert {:incremental, [{1, :op1}, {2, :op2}]} = result
    end

    test "skips missing entries in range" do
      log = %InMemoryLog{
        entries: %{1 => :op1, 3 => :op3},
        op_number: 3
      }

      result = StateStore.get_state_from(log, 1)

      assert {:incremental, [{1, :op1}, {3, :op3}]} = result
    end
  end

  describe "apply_state/2" do
    test "applies empty state" do
      log = %InMemoryLog{
        entries: %{1 => :old},
        commit_number: 1,
        op_number: 1
      }

      new_log = StateStore.apply_state(log, [])

      assert new_log.entries == %{}
      assert new_log.commit_number == 0
      assert new_log.op_number == 0
    end

    test "replaces entries with provided state" do
      log = InMemoryLog.new()

      entries = [{1, :new1}, {2, :new2}, {3, :new3}]
      new_log = StateStore.apply_state(log, entries)

      assert new_log.entries == %{1 => :new1, 2 => :new2, 3 => :new3}
      assert new_log.commit_number == 3
      assert new_log.op_number == 3
    end

    test "handles non-sequential entries" do
      log = InMemoryLog.new()

      entries = [{1, :a}, {5, :b}, {3, :c}]
      new_log = StateStore.apply_state(log, entries)

      assert new_log.commit_number == 5
      assert new_log.op_number == 5
    end
  end

  describe "apply_mutations/2" do
    test "returns unchanged store for empty mutations" do
      log = InMemoryLog.new()

      result = StateStore.apply_mutations(log, [])

      assert result == log
    end

    test "appends mutations to entries" do
      log = %InMemoryLog{
        entries: %{1 => :existing},
        op_number: 1
      }

      mutations = [{2, :new1}, {3, :new2}]
      new_log = StateStore.apply_mutations(log, mutations)

      assert new_log.entries == %{1 => :existing, 2 => :new1, 3 => :new2}
      assert new_log.op_number == 3
    end

    test "updates op_number to highest mutation number" do
      log = InMemoryLog.new()

      mutations = [{3, :a}, {1, :b}, {5, :c}]
      new_log = StateStore.apply_mutations(log, mutations)

      assert new_log.op_number == 5
    end

    test "overwrites existing entries with same op_number" do
      log = %InMemoryLog{
        entries: %{1 => :old},
        op_number: 1
      }

      new_log = StateStore.apply_mutations(log, [{1, :new}])

      assert new_log.entries == %{1 => :new}
    end
  end

  describe "get_pending/1" do
    test "returns empty list when no pending mutations" do
      log = InMemoryLog.new()

      assert StateStore.get_pending(log) == []
    end

    test "returns empty list when commit_number equals op_number" do
      log = %InMemoryLog{
        entries: %{1 => :op1},
        commit_number: 1,
        op_number: 1
      }

      assert StateStore.get_pending(log) == []
    end

    test "returns pending mutations" do
      log = %InMemoryLog{
        entries: %{1 => :op1, 2 => :op2, 3 => :op3},
        commit_number: 1,
        op_number: 3
      }

      pending = StateStore.get_pending(log)

      assert pending == [{2, :op2}, {3, :op3}]
    end

    test "skips missing entries in pending range" do
      log = %InMemoryLog{
        entries: %{1 => :op1, 3 => :op3},
        commit_number: 1,
        op_number: 3
      }

      pending = StateStore.get_pending(log)

      # Entry 2 missing, skipped
      assert pending == [{3, :op3}]
    end
  end

  describe "record_pending/3" do
    test "records mutation when op_number is next expected" do
      log = InMemoryLog.new()

      {:ok, new_log} = StateStore.record_pending(log, 1, :mutation1)

      assert new_log.entries == %{1 => :mutation1}
      assert new_log.op_number == 1
    end

    test "allows sequential recording" do
      log = InMemoryLog.new()

      {:ok, log} = StateStore.record_pending(log, 1, :m1)
      {:ok, log} = StateStore.record_pending(log, 2, :m2)
      {:ok, log} = StateStore.record_pending(log, 3, :m3)

      assert log.op_number == 3
      assert map_size(log.entries) == 3
    end

    test "returns error for gap in sequence" do
      log = InMemoryLog.new()

      result = StateStore.record_pending(log, 2, :mutation)

      assert result == {:error, :gap}
    end

    test "returns error for duplicate op_number" do
      log = InMemoryLog.new()
      {:ok, log} = StateStore.record_pending(log, 1, :m1)

      result = StateStore.record_pending(log, 1, :m1_again)

      assert result == {:error, :gap}
    end
  end

  describe "commit_through/3" do
    test "returns unchanged store when target <= commit_number" do
      log = %InMemoryLog{commit_number: 5, op_number: 5}

      {:ok, new_log, results} = StateStore.commit_through(log, 3, fn _ -> {:ok, :result} end)

      assert new_log == log
      assert results == []
    end

    test "executes mutations up to target" do
      log = InMemoryLog.new()
      {:ok, log} = StateStore.record_pending(log, 1, :op1)
      {:ok, log} = StateStore.record_pending(log, 2, :op2)

      execute_fn = fn mutation -> {:ok, {:executed, mutation}} end

      {:ok, new_log, results} = StateStore.commit_through(log, 2, execute_fn)

      assert new_log.commit_number == 2
      assert length(results) == 2
      assert {1, {:executed, :op1}} in results
      assert {2, {:executed, :op2}} in results
    end

    test "limits commit to op_number even if target is higher" do
      log = InMemoryLog.new()
      {:ok, log} = StateStore.record_pending(log, 1, :op1)

      {:ok, new_log, results} = StateStore.commit_through(log, 10, fn _ -> {:ok, :r} end)

      assert new_log.commit_number == 1
      assert length(results) == 1
    end

    test "skips missing entries during commit" do
      # Manually create log with gap (entry 2 missing)
      log = %InMemoryLog{
        entries: %{1 => :op1, 3 => :op3},
        op_number: 3,
        commit_number: 0
      }

      {:ok, new_log, results} = StateStore.commit_through(log, 3, fn m -> {:ok, m} end)

      # Only ops 1 and 3 executed (2 was missing)
      assert length(results) == 2
      assert {1, :op1} in results
      assert {3, :op3} in results
      assert new_log.commit_number == 3
    end

    test "handles commit when start > target (no-op range)" do
      log = %InMemoryLog{
        entries: %{1 => :op1},
        op_number: 1,
        commit_number: 1
      }

      {:ok, new_log, results} = StateStore.commit_through(log, 2, fn _ -> {:ok, :r} end)

      # commit_number is 1, target is 2, but op_number is only 1
      # so actual_target becomes 1, start is 2, range 2..1//1 is empty
      assert results == []
      assert new_log.commit_number == 1
    end
  end

  describe "op_number/1" do
    test "returns current op_number" do
      log = %InMemoryLog{op_number: 42}

      assert StateStore.op_number(log) == 42
    end
  end

  describe "commit_number/1" do
    test "returns current commit_number" do
      log = %InMemoryLog{commit_number: 15}

      assert StateStore.commit_number(log) == 15
    end
  end

  describe "get_view_number/1" do
    test "returns current view_number" do
      log = InMemoryLog.new(7)

      assert StateStore.get_view_number(log) == 7
    end
  end

  describe "save_view_number/2" do
    test "updates view_number" do
      log = InMemoryLog.new()

      {:ok, new_log} = StateStore.save_view_number(log, 10)

      assert new_log.view_number == 10
    end

    test "preserves other fields" do
      log = %InMemoryLog{
        entries: %{1 => :op1},
        op_number: 1,
        commit_number: 1,
        view_number: 5
      }

      {:ok, new_log} = StateStore.save_view_number(log, 10)

      assert new_log.entries == %{1 => :op1}
      assert new_log.op_number == 1
      assert new_log.commit_number == 1
      assert new_log.view_number == 10
    end
  end
end
