defmodule Bedrock.ViewstampedReplication.ClientTableTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Bedrock.ViewstampedReplication.ClientTable

  describe "new/0" do
    test "creates empty client table" do
      ct = ClientTable.new()
      assert ct.table == %{}
    end
  end

  describe "lookup/2" do
    test "returns nil for unknown client" do
      ct = ClientTable.new()
      assert nil == ClientTable.lookup(ct, :unknown_client)
    end

    test "returns entry for known client" do
      ct = ClientTable.new() |> ClientTable.record_pending(:client1, 5)
      assert {5, :pending} = ClientTable.lookup(ct, :client1)
    end
  end

  describe "check_request/3" do
    test "returns :new for unknown client" do
      ct = ClientTable.new()
      assert :new = ClientTable.check_request(ct, :client1, 1)
    end

    test "returns :new for higher request number" do
      ct = ClientTable.new() |> ClientTable.record_pending(:client1, 1)
      assert :new = ClientTable.check_request(ct, :client1, 2)
    end

    test "returns :duplicate for pending request with same number" do
      ct = ClientTable.new() |> ClientTable.record_pending(:client1, 5)
      assert :duplicate = ClientTable.check_request(ct, :client1, 5)
    end

    test "returns :duplicate for old request number" do
      ct = ClientTable.new() |> ClientTable.record_pending(:client1, 10)
      assert :duplicate = ClientTable.check_request(ct, :client1, 5)
    end

    test "returns {:cached, result} for completed request with same number" do
      ct =
        ClientTable.new()
        |> ClientTable.record_result(:client1, 5, :my_result)

      assert {:cached, :my_result} = ClientTable.check_request(ct, :client1, 5)
    end
  end

  describe "record_pending/3" do
    test "records pending request" do
      ct = ClientTable.new() |> ClientTable.record_pending(:client1, 1)
      assert {1, :pending} = ClientTable.lookup(ct, :client1)
    end

    test "overwrites previous entry for same client" do
      ct =
        ClientTable.new()
        |> ClientTable.record_pending(:client1, 1)
        |> ClientTable.record_pending(:client1, 2)

      assert {2, :pending} = ClientTable.lookup(ct, :client1)
    end

    test "tracks multiple clients independently" do
      ct =
        ClientTable.new()
        |> ClientTable.record_pending(:client1, 1)
        |> ClientTable.record_pending(:client2, 5)

      assert {1, :pending} = ClientTable.lookup(ct, :client1)
      assert {5, :pending} = ClientTable.lookup(ct, :client2)
    end
  end

  describe "record_result/4" do
    test "records completed request with result" do
      ct = ClientTable.new() |> ClientTable.record_result(:client1, 1, :result)
      assert {1, {:completed, :result}} = ClientTable.lookup(ct, :client1)
    end

    test "overwrites pending state" do
      ct =
        ClientTable.new()
        |> ClientTable.record_pending(:client1, 1)
        |> ClientTable.record_result(:client1, 1, :result)

      assert {1, {:completed, :result}} = ClientTable.lookup(ct, :client1)
    end
  end

  describe "entries/1" do
    test "returns empty list for empty table" do
      ct = ClientTable.new()
      assert [] = ClientTable.entries(ct)
    end

    test "returns all entries" do
      ct =
        ClientTable.new()
        |> ClientTable.record_pending(:client1, 1)
        |> ClientTable.record_result(:client2, 2, :result2)

      entries = ClientTable.entries(ct)
      assert length(entries) == 2
      assert {:client1, {1, :pending}} in entries
      assert {:client2, {2, {:completed, :result2}}} in entries
    end
  end

  describe "from_entries/1" do
    test "creates client table from entries" do
      entries = [
        {:client1, {1, :pending}},
        {:client2, {2, {:completed, :result2}}}
      ]

      ct = ClientTable.from_entries(entries)

      assert {1, :pending} = ClientTable.lookup(ct, :client1)
      assert {2, {:completed, :result2}} = ClientTable.lookup(ct, :client2)
    end

    test "creates empty table from empty list" do
      ct = ClientTable.from_entries([])
      assert ct.table == %{}
    end
  end

  describe "round-trip entries -> from_entries" do
    test "preserves all data" do
      original =
        ClientTable.new()
        |> ClientTable.record_pending(:a, 1)
        |> ClientTable.record_result(:b, 5, {:ok, "data"})
        |> ClientTable.record_pending(:c, 10)

      entries = ClientTable.entries(original)
      restored = ClientTable.from_entries(entries)

      assert ClientTable.lookup(restored, :a) == ClientTable.lookup(original, :a)
      assert ClientTable.lookup(restored, :b) == ClientTable.lookup(original, :b)
      assert ClientTable.lookup(restored, :c) == ClientTable.lookup(original, :c)
    end
  end
end
