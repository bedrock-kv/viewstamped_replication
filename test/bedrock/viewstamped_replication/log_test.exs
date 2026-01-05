defmodule Bedrock.ViewstampedReplication.LogTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Bedrock.ViewstampedReplication.Log
  alias Bedrock.ViewstampedReplication.Log.InMemoryLog

  describe "append_entries/2 (viewstamped_replication-z1q)" do
    test "appends entries to empty log" do
      log = InMemoryLog.new()
      entries = [{1, {:c1, 1, :op1}}, {2, {:c1, 2, :op2}}]

      new_log = Log.append_entries(log, entries)

      assert Log.get(new_log, 1) == {:c1, 1, :op1}
      assert Log.get(new_log, 2) == {:c1, 2, :op2}
      assert Log.newest_op_number(new_log) == 2
    end

    test "appends entries to existing log" do
      log = InMemoryLog.new()
      {:ok, log} = Log.append(log, 1, {:c1, 1, :op1})
      {:ok, log} = Log.append(log, 2, {:c1, 2, :op2})

      new_entries = [{3, {:c1, 3, :op3}}, {4, {:c1, 4, :op4}}]
      new_log = Log.append_entries(log, new_entries)

      # Original entries preserved
      assert Log.get(new_log, 1) == {:c1, 1, :op1}
      assert Log.get(new_log, 2) == {:c1, 2, :op2}
      # New entries added
      assert Log.get(new_log, 3) == {:c1, 3, :op3}
      assert Log.get(new_log, 4) == {:c1, 4, :op4}
      assert Log.newest_op_number(new_log) == 4
    end

    test "appending empty list returns unchanged log" do
      log = InMemoryLog.new()
      {:ok, log} = Log.append(log, 1, {:c1, 1, :op1})

      new_log = Log.append_entries(log, [])

      assert Log.get(new_log, 1) == {:c1, 1, :op1}
      assert Log.newest_op_number(new_log) == 1
    end

    test "overwrites existing entries with same op number" do
      log = InMemoryLog.new()
      {:ok, log} = Log.append(log, 1, {:c1, 1, :old_op})

      new_log = Log.append_entries(log, [{1, {:c1, 1, :new_op}}])

      assert Log.get(new_log, 1) == {:c1, 1, :new_op}
    end

    test "handles non-contiguous entries" do
      log = InMemoryLog.new()
      entries = [{1, {:c1, 1, :op1}}, {5, {:c1, 5, :op5}}, {3, {:c1, 3, :op3}}]

      new_log = Log.append_entries(log, entries)

      assert Log.get(new_log, 1) == {:c1, 1, :op1}
      assert Log.get(new_log, 3) == {:c1, 3, :op3}
      assert Log.get(new_log, 5) == {:c1, 5, :op5}
      assert Log.newest_op_number(new_log) == 5
    end
  end
end
