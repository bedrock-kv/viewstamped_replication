defmodule Bedrock.ViewstampedReplication.Mode.Normal.BackupTrackingTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Bedrock.ViewstampedReplication.Mode.Normal.BackupTracking

  describe "new/1" do
    test "creates tracking with all replicas at op 0" do
      tracking = BackupTracking.new([:a, :b, :c])

      assert BackupTracking.get_ack(tracking, :a) == 0
      assert BackupTracking.get_ack(tracking, :b) == 0
      assert BackupTracking.get_ack(tracking, :c) == 0
    end

    test "creates empty tracking for empty replica list" do
      tracking = BackupTracking.new([])
      assert tracking.acks == %{}
    end
  end

  describe "record_ack/3" do
    test "records first acknowledgment" do
      tracking = BackupTracking.new([:a, :b])
      tracking = BackupTracking.record_ack(tracking, :a, 5)

      assert BackupTracking.get_ack(tracking, :a) == 5
      assert BackupTracking.get_ack(tracking, :b) == 0
    end

    test "updates acknowledgment to higher op number" do
      tracking =
        BackupTracking.new([:a])
        |> BackupTracking.record_ack(:a, 5)
        |> BackupTracking.record_ack(:a, 10)

      assert BackupTracking.get_ack(tracking, :a) == 10
    end

    test "ignores lower op number" do
      tracking =
        BackupTracking.new([:a])
        |> BackupTracking.record_ack(:a, 10)
        |> BackupTracking.record_ack(:a, 5)

      assert BackupTracking.get_ack(tracking, :a) == 10
    end

    test "ignores same op number" do
      tracking =
        BackupTracking.new([:a])
        |> BackupTracking.record_ack(:a, 5)
        |> BackupTracking.record_ack(:a, 5)

      assert BackupTracking.get_ack(tracking, :a) == 5
    end

    test "records ack for unknown replica" do
      tracking = BackupTracking.new([:a])
      tracking = BackupTracking.record_ack(tracking, :unknown, 5)

      assert BackupTracking.get_ack(tracking, :unknown) == 5
    end
  end

  describe "count_acks_for/2" do
    test "counts replicas with sufficient acks" do
      tracking =
        BackupTracking.new([:a, :b, :c])
        |> BackupTracking.record_ack(:a, 5)
        |> BackupTracking.record_ack(:b, 5)
        |> BackupTracking.record_ack(:c, 3)

      assert BackupTracking.count_acks_for(tracking, 5) == 2
      assert BackupTracking.count_acks_for(tracking, 3) == 3
      assert BackupTracking.count_acks_for(tracking, 1) == 3
      assert BackupTracking.count_acks_for(tracking, 6) == 0
    end

    test "returns 0 for empty tracking" do
      tracking = BackupTracking.new([])
      assert BackupTracking.count_acks_for(tracking, 1) == 0
    end

    test "counts 0 when all replicas are at 0" do
      tracking = BackupTracking.new([:a, :b])
      assert BackupTracking.count_acks_for(tracking, 1) == 0
      assert BackupTracking.count_acks_for(tracking, 0) == 2
    end
  end

  describe "highest_acked_by_count/2" do
    test "returns highest op acked by at least count replicas" do
      tracking =
        BackupTracking.new([:a, :b, :c])
        |> BackupTracking.record_ack(:a, 10)
        |> BackupTracking.record_ack(:b, 5)
        |> BackupTracking.record_ack(:c, 3)

      # Need 1 replica: highest is 10
      assert BackupTracking.highest_acked_by_count(tracking, 1) == 10

      # Need 2 replicas: 10 and 5, so 5
      assert BackupTracking.highest_acked_by_count(tracking, 2) == 5

      # Need 3 replicas: 10, 5, and 3, so 3
      assert BackupTracking.highest_acked_by_count(tracking, 3) == 3
    end

    test "returns 0 when not enough replicas" do
      tracking = BackupTracking.new([:a, :b])
      assert BackupTracking.highest_acked_by_count(tracking, 5) == 0
    end

    test "returns 0 for empty tracking" do
      tracking = BackupTracking.new([])
      assert BackupTracking.highest_acked_by_count(tracking, 1) == 0
    end
  end

  describe "get_ack/2" do
    test "returns 0 for unknown replica" do
      tracking = BackupTracking.new([:a])
      assert BackupTracking.get_ack(tracking, :unknown) == 0
    end

    test "returns acked op number for known replica" do
      tracking =
        BackupTracking.new([:a])
        |> BackupTracking.record_ack(:a, 7)

      assert BackupTracking.get_ack(tracking, :a) == 7
    end
  end
end
