defmodule Bedrock.ViewstampedReplication.Log.InMemoryLogTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Bedrock.ViewstampedReplication.Log
  alias Bedrock.ViewstampedReplication.Log.InMemoryLog

  describe "new/0" do
    test "creates empty log with defaults" do
      log = InMemoryLog.new()
      assert log.entries == %{}
      assert log.newest_op_number == 0
      assert log.current_view_number == 0
    end
  end

  describe "new/1" do
    test "creates log with specified view number" do
      log = InMemoryLog.new(5)
      assert log.current_view_number == 5
      assert log.newest_op_number == 0
      assert log.entries == %{}
    end
  end

  describe "Log.append/3" do
    test "appends entry at op_number 1 to empty log" do
      log = InMemoryLog.new()
      {:ok, log} = Log.append(log, 1, :entry1)

      assert Log.get(log, 1) == :entry1
      assert Log.newest_op_number(log) == 1
    end

    test "appends sequential entries" do
      log = InMemoryLog.new()
      {:ok, log} = Log.append(log, 1, :entry1)
      {:ok, log} = Log.append(log, 2, :entry2)
      {:ok, log} = Log.append(log, 3, :entry3)

      assert Log.get(log, 1) == :entry1
      assert Log.get(log, 2) == :entry2
      assert Log.get(log, 3) == :entry3
      assert Log.newest_op_number(log) == 3
    end

    test "rejects gap in op numbers" do
      log = InMemoryLog.new()
      {:ok, log} = Log.append(log, 1, :entry1)

      assert {:error, :gap} = Log.append(log, 3, :entry3)
      assert Log.newest_op_number(log) == 1
    end

    test "rejects non-sequential first entry" do
      log = InMemoryLog.new()
      assert {:error, :gap} = Log.append(log, 5, :entry5)
    end
  end

  describe "Log.get/2" do
    test "returns nil for non-existent entry" do
      log = InMemoryLog.new()
      assert nil == Log.get(log, 1)
    end

    test "returns entry at op number" do
      log = InMemoryLog.new()
      {:ok, log} = Log.append(log, 1, {:client, 1, :op1})

      assert {:client, 1, :op1} = Log.get(log, 1)
    end
  end

  describe "Log.entries_from/2" do
    test "returns empty list for empty log" do
      log = InMemoryLog.new()
      assert [] = Log.entries_from(log, 1)
    end

    test "returns entries from specified op number onwards" do
      log = InMemoryLog.new()
      {:ok, log} = Log.append(log, 1, :e1)
      {:ok, log} = Log.append(log, 2, :e2)
      {:ok, log} = Log.append(log, 3, :e3)

      entries = Log.entries_from(log, 2)
      assert [{2, :e2}, {3, :e3}] = entries
    end

    test "returns all entries when starting from 1" do
      log = InMemoryLog.new()
      {:ok, log} = Log.append(log, 1, :e1)
      {:ok, log} = Log.append(log, 2, :e2)

      entries = Log.entries_from(log, 1)
      assert [{1, :e1}, {2, :e2}] = entries
    end
  end

  describe "Log.entries_to/2" do
    test "returns empty list for empty log" do
      log = InMemoryLog.new()
      assert [] = Log.entries_to(log, 5)
    end

    test "returns entries up to specified op number" do
      log = InMemoryLog.new()
      {:ok, log} = Log.append(log, 1, :e1)
      {:ok, log} = Log.append(log, 2, :e2)
      {:ok, log} = Log.append(log, 3, :e3)

      entries = Log.entries_to(log, 2)
      assert [{1, :e1}, {2, :e2}] = entries
    end
  end

  describe "Log.newest_op_number/1" do
    test "returns 0 for empty log" do
      log = InMemoryLog.new()
      assert 0 = Log.newest_op_number(log)
    end

    test "returns highest op number" do
      log = InMemoryLog.new()
      {:ok, log} = Log.append(log, 1, :e1)
      {:ok, log} = Log.append(log, 2, :e2)

      assert 2 = Log.newest_op_number(log)
    end
  end

  describe "Log.truncate_after/2" do
    test "truncates entries after specified op number" do
      log = InMemoryLog.new()
      {:ok, log} = Log.append(log, 1, :e1)
      {:ok, log} = Log.append(log, 2, :e2)
      {:ok, log} = Log.append(log, 3, :e3)

      log = Log.truncate_after(log, 2)

      assert Log.get(log, 1) == :e1
      assert Log.get(log, 2) == :e2
      assert Log.get(log, 3) == nil
      assert Log.newest_op_number(log) == 2
    end

    test "truncates to empty when op_number is 0" do
      log = InMemoryLog.new()
      {:ok, log} = Log.append(log, 1, :e1)
      {:ok, log} = Log.append(log, 2, :e2)

      log = Log.truncate_after(log, 0)

      assert Log.newest_op_number(log) == 0
      assert log.entries == %{}
    end

    test "no-op when truncating after newest" do
      log = InMemoryLog.new()
      {:ok, log} = Log.append(log, 1, :e1)
      {:ok, log} = Log.append(log, 2, :e2)

      log = Log.truncate_after(log, 5)

      assert Log.newest_op_number(log) == 5
      assert Log.get(log, 1) == :e1
      assert Log.get(log, 2) == :e2
    end
  end

  describe "Log.current_view_number/1" do
    test "returns 0 for new log" do
      log = InMemoryLog.new()
      assert 0 = Log.current_view_number(log)
    end

    test "returns configured view number" do
      log = InMemoryLog.new(7)
      assert 7 = Log.current_view_number(log)
    end
  end

  describe "Log.save_current_view_number/2" do
    test "saves view number" do
      log = InMemoryLog.new()
      {:ok, log} = Log.save_current_view_number(log, 10)

      assert 10 = Log.current_view_number(log)
    end

    test "can update view number" do
      log = InMemoryLog.new(5)
      {:ok, log} = Log.save_current_view_number(log, 10)

      assert 10 = Log.current_view_number(log)
    end
  end
end
