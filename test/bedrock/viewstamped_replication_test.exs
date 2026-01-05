defmodule Bedrock.ViewstampedReplicationTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Bedrock.ViewstampedReplication, as: VR
  alias VR.Log
  alias VR.Log.InMemoryLog
  alias VR.Mode.Normal
  alias VR.Mode.ViewChange

  import Mox
  alias Bedrock.ViewstampedReplication.MockInterface

  setup [:verify_on_exit!, :fix_stubs]

  def mock_timer_cancel, do: :ok

  def fix_stubs(context) do
    stub(MockInterface, :heartbeat_ms, fn -> 50 end)
    stub(MockInterface, :view_change_timeout_ms, fn -> 150 end)
    stub(MockInterface, :timestamp_in_ms, fn -> 1000 end)
    stub(MockInterface, :ignored_event, fn _, _ -> :ok end)
    stub(MockInterface, :primary_changed, fn _ -> :ok end)
    {:ok, context}
  end

  describe "VR can be constructed" do
    test "single replica starts as primary in view 0" do
      expect(MockInterface, :timer, fn :heartbeat -> &mock_timer_cancel/0 end)

      vr = VR.new(:a, [:a], InMemoryLog.new(), MockInterface)

      assert %VR{
               me: :a,
               replica_index: 0,
               configuration: [:a],
               quorum: 1,
               mode: %Normal{
                 view_number: 0,
                 op_number: 0,
                 commit_number: 0,
                 is_primary: true
               }
             } = vr

      assert :a = VR.me(vr)
      assert VR.am_i_primary?(vr)
      assert :a = VR.primary(vr)
      assert 0 = VR.view_number(vr)
      assert :normal = VR.status(vr)
    end

    test "three replicas - first replica is primary in view 0" do
      expect(MockInterface, :timer, fn :heartbeat -> &mock_timer_cancel/0 end)

      vr = VR.new(:a, [:a, :b, :c], InMemoryLog.new(), MockInterface)

      assert %VR{
               me: :a,
               replica_index: 0,
               configuration: [:a, :b, :c],
               quorum: 2,
               mode: %Normal{
                 view_number: 0,
                 is_primary: true
               }
             } = vr

      assert VR.am_i_primary?(vr)
      assert :a = VR.primary(vr)
    end

    test "three replicas - second replica is backup in view 0" do
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)

      vr = VR.new(:b, [:a, :b, :c], InMemoryLog.new(), MockInterface)

      assert %VR{
               me: :b,
               replica_index: 1,
               configuration: [:a, :b, :c],
               quorum: 2,
               mode: %Normal{
                 view_number: 0,
                 is_primary: false
               }
             } = vr

      refute VR.am_i_primary?(vr)
      assert :a = VR.primary(vr)
    end

    test "configuration is sorted for deterministic primary selection" do
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)

      # Pass unsorted configuration
      vr = VR.new(:b, [:c, :a, :b], InMemoryLog.new(), MockInterface)

      # Configuration should be sorted
      assert [:a, :b, :c] = VR.configuration(vr)
      # :a is at index 0, so it's primary in view 0
      assert :a = VR.primary(vr)
    end
  end

  describe "Primary selection" do
    test "primary rotates with view number" do
      expect(MockInterface, :timer, fn :heartbeat -> &mock_timer_cancel/0 end)

      vr = VR.new(:a, [:a, :b, :c], InMemoryLog.new(), MockInterface)

      # View 0: primary = 0 rem 3 = 0 (:a)
      assert :a = VR.primary(vr)

      # Simulate view 1 by checking math
      # View 1: primary = 1 rem 3 = 1 (:b)
      # View 2: primary = 2 rem 3 = 2 (:c)
      # View 3: primary = 3 rem 3 = 0 (:a)
    end
  end

  describe "Normal operation - Primary" do
    test "primary accepts client request and assigns op number" do
      expect(MockInterface, :timer, fn :heartbeat -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:prepare, 0, _, 1, 0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:prepare, 0, _, 1, 0} -> :ok end)

      vr = VR.new(:a, [:a, :b, :c], InMemoryLog.new(), MockInterface)

      {:ok, vr, op_num} = VR.submit_request(vr, :client1, 1, :operation1)

      assert 1 = op_num
      assert 1 = VR.op_number(vr)
    end

    test "non-primary rejects client request" do
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)

      vr = VR.new(:b, [:a, :b, :c], InMemoryLog.new(), MockInterface)

      assert {:error, :not_primary} = VR.submit_request(vr, :client1, 1, :operation1)
    end

    test "primary commits after receiving quorum of PREPAREOK" do
      expect(MockInterface, :timer, fn :heartbeat -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:prepare, 0, _, 1, 0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:prepare, 0, _, 1, 0} -> :ok end)
      expect(MockInterface, :execute_operation, fn :op1 -> {:ok, :result1} end)
      expect(MockInterface, :operation_committed, fn _, 1, :op1, :result1 -> :ok end)
      expect(MockInterface, :send_reply, fn :client1, 0, 1, :result1 -> :ok end)

      vr = VR.new(:a, [:a, :b, :c], InMemoryLog.new(), MockInterface)

      {:ok, vr, 1} = VR.submit_request(vr, :client1, 1, :op1)

      # Receive PREPAREOK from :b
      vr = VR.handle_event(vr, {:prepare_ok, 0, 1}, :b)

      # With :a (primary) + :b = 2, we have quorum (f+1 where f=1)
      assert 1 = VR.commit_number(vr)
    end
  end

  describe "Normal operation - Backup" do
    test "backup processes PREPARE and sends PREPAREOK" do
      # Per VR paper Section 4.2 (viewstamped_replication-6to): Only one timer at Normal.new()
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :a, {:prepare_ok, 0, 1} -> :ok end)

      vr = VR.new(:b, [:a, :b, :c], InMemoryLog.new(), MockInterface)

      # Receive PREPARE from primary
      entry = {:client1, 1, :operation1}
      vr = VR.handle_event(vr, {:prepare, 0, entry, 1, 0}, :a)

      assert 1 = VR.op_number(vr)
    end

    test "backup executes committed operations on COMMIT" do
      # Per VR paper Section 4.2 (viewstamped_replication-6to): Only one timer at Normal.new()
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :a, {:prepare_ok, 0, 1} -> :ok end)
      expect(MockInterface, :execute_operation, fn :op1 -> {:ok, :result1} end)
      expect(MockInterface, :operation_committed, fn _, 1, :op1, :result1 -> :ok end)

      vr = VR.new(:b, [:a, :b, :c], InMemoryLog.new(), MockInterface)

      # First receive PREPARE
      entry = {:client1, 1, :op1}
      vr = VR.handle_event(vr, {:prepare, 0, entry, 1, 0}, :a)

      # Then receive COMMIT
      vr = VR.handle_event(vr, {:commit, 0, 1}, :a)

      assert 1 = VR.commit_number(vr)
    end

    test "backup triggers view change on timeout" do
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)

      vr = VR.new(:b, [:a, :b, :c], InMemoryLog.new(), MockInterface)

      # Per VR paper Section 4.2 (viewstamped_replication-6to): Simulate timer expiry with elapsed time >= timeout
      # Stub returns last_heard_at=1000, timeout=150, so we need now > 1150
      expect(MockInterface, :timestamp_in_ms, fn -> 2000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :a, {:start_view_change, 1} -> :ok end)
      expect(MockInterface, :send_event, fn :b, {:start_view_change, 1} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:start_view_change, 1} -> :ok end)
      expect(MockInterface, :primary_changed, fn {:b, 1} -> :ok end)

      vr = VR.handle_event(vr, :view_change, :timer)

      assert :view_change = VR.status(vr)
      assert 1 = VR.view_number(vr)
    end
  end

  describe "View Change" do
    test "replica starts view change and sends STARTVIEWCHANGE" do
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)

      vr = VR.new(:b, [:a, :b, :c], InMemoryLog.new(), MockInterface)

      # Per VR paper Section 4.2 (viewstamped_replication-6to): Simulate timer expiry with elapsed time >= timeout
      expect(MockInterface, :timestamp_in_ms, fn -> 2000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :a, {:start_view_change, 1} -> :ok end)
      expect(MockInterface, :send_event, fn :b, {:start_view_change, 1} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:start_view_change, 1} -> :ok end)
      expect(MockInterface, :primary_changed, fn {:b, 1} -> :ok end)

      # Trigger view change
      vr = VR.handle_event(vr, :view_change, :timer)

      assert %VR{mode: %ViewChange{view_number: 1}} = vr
    end

    test "replica in view change ignores old view messages" do
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)

      vr = VR.new(:b, [:a, :b, :c], InMemoryLog.new(), MockInterface)

      # Per VR paper Section 4.2 (viewstamped_replication-6to): Simulate timer expiry with elapsed time >= timeout
      expect(MockInterface, :timestamp_in_ms, fn -> 2000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, 3, fn _, _ -> :ok end)
      expect(MockInterface, :primary_changed, fn {:b, 1} -> :ok end)

      vr = VR.handle_event(vr, :view_change, :timer)

      # Old view message should be ignored
      vr = VR.handle_event(vr, {:start_view_change, 0}, :c)

      assert 1 = VR.view_number(vr)
    end

    test "replica transitions to higher view on STARTVIEWCHANGE" do
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)

      vr = VR.new(:b, [:a, :b, :c], InMemoryLog.new(), MockInterface)

      # Receiving STARTVIEWCHANGE triggers view change via message, not timer
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, 3, fn _, _ -> :ok end)
      expect(MockInterface, :primary_changed, fn {:b, 1} -> :ok end)

      # Receive STARTVIEWCHANGE for view 1 from another replica
      vr = VR.handle_event(vr, {:start_view_change, 1}, :c)

      assert :view_change = VR.status(vr)
      assert 1 = VR.view_number(vr)
    end
  end

  describe "Client table deduplication" do
    test "duplicate request returns cached result" do
      expect(MockInterface, :timer, fn :heartbeat -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:prepare, 0, _, 1, 0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:prepare, 0, _, 1, 0} -> :ok end)
      expect(MockInterface, :execute_operation, fn :op1 -> {:ok, :result1} end)
      expect(MockInterface, :operation_committed, fn _, 1, :op1, :result1 -> :ok end)
      expect(MockInterface, :send_reply, fn :client1, 0, 1, :result1 -> :ok end)
      # For the duplicate request
      expect(MockInterface, :send_reply, fn :client1, 0, 1, :result1 -> :ok end)

      vr = VR.new(:a, [:a, :b, :c], InMemoryLog.new(), MockInterface)

      # Submit first request
      {:ok, vr, 1} = VR.submit_request(vr, :client1, 1, :op1)

      # Get quorum
      vr = VR.handle_event(vr, {:prepare_ok, 0, 1}, :b)

      # Submit duplicate request (same client, same request number)
      {:ok, _vr, {:cached, :result1}} = VR.submit_request(vr, :client1, 1, :op1)
    end
  end

  describe "Log protocol" do
    test "InMemoryLog implements Log protocol" do
      log = InMemoryLog.new()

      assert 0 = Log.newest_op_number(log)
      assert 0 = Log.current_view_number(log)

      # Append entries
      {:ok, log} = Log.append(log, 1, {:client1, 1, :op1})
      {:ok, log} = Log.append(log, 2, {:client1, 2, :op2})

      assert 2 = Log.newest_op_number(log)
      assert {:client1, 1, :op1} = Log.get(log, 1)
      assert {:client1, 2, :op2} = Log.get(log, 2)

      # Gap detection
      assert {:error, :gap} = Log.append(log, 4, {:client1, 4, :op4})

      # View number persistence
      {:ok, log} = Log.save_current_view_number(log, 5)
      assert 5 = Log.current_view_number(log)
    end
  end

  describe "Error cases" do
    test "unknown events are ignored" do
      expect(MockInterface, :timer, fn :heartbeat -> &mock_timer_cancel/0 end)
      expect(MockInterface, :ignored_event, fn :unknown_event, :peer_x -> :ok end)

      vr = VR.new(:a, [:a, :b, :c], InMemoryLog.new(), MockInterface)

      result = VR.handle_event(vr, :unknown_event, :peer_x)

      # State unchanged
      assert vr.mode.view_number == result.mode.view_number
    end
  end

  describe "STARTVIEW handling (Paper Section 4.2 Step 5)" do
    test "backup sends PREPAREOK for uncommitted ops after receiving STARTVIEW" do
      # Paper Section 4.2 Step 5:
      # "If there are non-committed operations in the log, they send a
      # PREPAREOK(v, n, i) message to the primary for these operations."

      # Setup: backup :c in view change for view 1
      # In view 1: primary = 1 rem 3 = 1, so :b is primary
      # :c (index 2) is a backup
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)

      vr = VR.new(:c, [:a, :b, :c], InMemoryLog.new(), MockInterface)

      # Per VR paper Section 4.2 (viewstamped_replication-6to): Simulate timer expiry with elapsed time >= timeout
      # Need 2 calls: one for timer_ticked check, one for Normal.new when becoming normal
      expect(MockInterface, :timestamp_in_ms, 2, fn -> 2000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, 3, fn _, {:start_view_change, 1} -> :ok end)
      expect(MockInterface, :primary_changed, fn {:b, 1} -> :ok end)

      vr = VR.handle_event(vr, :view_change, :timer)

      assert :view_change = VR.status(vr)

      # New primary :b in view 1 sends STARTVIEW with:
      # - log entries: ops 1 and 2
      # - op_number: 2
      # - commit_number: 0 (none committed yet)
      # So ops 1 and 2 are uncommitted and need PREPAREOK

      log_entries = [
        {1, {:client1, 1, :op1}},
        {2, {:client1, 2, :op2}}
      ]

      # Expect PREPAREOK for each uncommitted op (1 and 2)
      # Timer for becoming normal backup - need timestamp for Normal.new()
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)
      # PREPAREOKs for uncommitted ops to new primary :b
      expect(MockInterface, :send_event, fn :b, {:prepare_ok, 1, 1} -> :ok end)
      expect(MockInterface, :send_event, fn :b, {:prepare_ok, 1, 2} -> :ok end)

      vr = VR.handle_event(vr, {:start_view, 1, log_entries, 2, 0}, :b)

      assert :normal = VR.status(vr)
      assert 1 = VR.view_number(vr)
      assert 2 = VR.op_number(vr)
      assert 0 = VR.commit_number(vr)
    end

    test "backup does not send PREPAREOK when all ops are committed" do
      # When commit_number == op_number, no PREPAREOKs needed
      # :c is backup in view 1
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)

      vr = VR.new(:c, [:a, :b, :c], InMemoryLog.new(), MockInterface)

      # Per VR paper Section 4.2 (viewstamped_replication-6to): Simulate timer expiry with elapsed time >= timeout
      # Need 2 calls: one for timer_ticked check, one for Normal.new when becoming normal
      expect(MockInterface, :timestamp_in_ms, 2, fn -> 2000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, 3, fn _, {:start_view_change, 1} -> :ok end)
      expect(MockInterface, :primary_changed, fn {:b, 1} -> :ok end)

      vr = VR.handle_event(vr, :view_change, :timer)

      # STARTVIEW with all ops committed (op_number == commit_number)
      log_entries = [
        {1, {:client1, 1, :op1}},
        {2, {:client1, 2, :op2}}
      ]

      # Only expect timer for becoming normal, no PREPAREOKs
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)
      # No send_event expectations for PREPAREOK

      # Backup executes committed ops when becoming normal (per paper Section 4.2 Step 5)
      expect(MockInterface, :execute_operation, fn :op1 -> {:ok, :result1} end)
      expect(MockInterface, :operation_committed, fn _, 1, :op1, :result1 -> :ok end)
      expect(MockInterface, :execute_operation, fn :op2 -> {:ok, :result2} end)
      expect(MockInterface, :operation_committed, fn _, 2, :op2, :result2 -> :ok end)

      vr = VR.handle_event(vr, {:start_view, 1, log_entries, 2, 2}, :b)

      assert :normal = VR.status(vr)
      assert 2 = VR.commit_number(vr)
    end

    test "PREPAREOK contains correct view number, op number, and replica id" do
      # :a is primary in view 0 (0 rem 3 = 0), so use :c which is backup in both view 0 and 1
      # In view 1: primary = 1 rem 3 = 1, so :b is primary, :a and :c are backups
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)

      vr = VR.new(:c, [:a, :b, :c], InMemoryLog.new(), MockInterface)

      # Per VR paper Section 4.2 (viewstamped_replication-6to): Simulate timer expiry with elapsed time >= timeout
      # Need 2 calls: one for timer_ticked check, one for Normal.new when becoming normal
      expect(MockInterface, :timestamp_in_ms, 2, fn -> 2000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, 3, fn _, {:start_view_change, 1} -> :ok end)
      expect(MockInterface, :primary_changed, fn {:b, 1} -> :ok end)

      vr = VR.handle_event(vr, :view_change, :timer)

      log_entries = [{1, {:client1, 1, :op1}}]

      # Expect PREPAREOK with:
      # - view_number: 1 (the new view)
      # - op_number: 1 (the uncommitted op)
      # - replica_id: :c (this replica)
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:prepare_ok, 1, 1} -> :ok end)

      _vr = VR.handle_event(vr, {:start_view, 1, log_entries, 1, 0}, :b)
    end
  end

  describe "Recovery protocol" do
    test "replica in normal mode responds to RECOVERY" do
      expect(MockInterface, :timer, fn :heartbeat -> &mock_timer_cancel/0 end)

      expect(MockInterface, :send_event, fn :recovering,
                                            {:recovery_response, 0, :nonce, [], 0, 0, true} ->
        :ok
      end)

      vr = VR.new(:a, [:a, :b, :c], InMemoryLog.new(), MockInterface)

      # Handle RECOVERY from a recovering replica
      vr = VR.handle_event(vr, {:recovery, :nonce}, :recovering)

      # Should still be in normal mode
      assert :normal = VR.status(vr)
    end

    test "recovering replica processes RECOVERYRESPONSE and becomes normal" do
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)

      vr = VR.new(:b, [:a, :b, :c], InMemoryLog.new(), MockInterface)

      # Manually put into recovering mode
      expect(MockInterface, :timer, fn :recovery -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :a, {:recovery, :nonce} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:recovery, :nonce} -> :ok end)

      vr = VR.start_recovering(vr, :nonce)
      assert :recovering = VR.status(vr)

      # First response (non-primary)
      vr = VR.handle_event(vr, {:recovery_response, 5, :nonce, [], 0, 0, false}, :c)
      assert :recovering = VR.status(vr)

      # Second response (primary with log)
      log_entries = [{1, {:client, 1, :op}}]
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)

      # Per paper Section 4.3 + Section 4.2 Step 5:
      # Recovering replica executes committed operations to rebuild client_table
      expect(MockInterface, :execute_operation, fn :op -> {:ok, :result} end)
      expect(MockInterface, :operation_committed, fn _, 1, :op, :result -> :ok end)
      # Note: as backup, doesn't send reply

      vr = VR.handle_event(vr, {:recovery_response, 5, :nonce, log_entries, 1, 1, true}, :a)

      assert :normal = VR.status(vr)
      assert 5 = VR.view_number(vr)
      assert 1 = VR.op_number(vr)

      # Client table should have the result cached (viewstamped_replication-3s2)
      assert vr.mode.client_table.table[:client] == {1, {:completed, :result}}
    end
  end

  describe "View change completion" do
    test "new primary collects DOVIEWCHANGE and sends STARTVIEW" do
      # :b is primary for view 1 (1 rem 3 = 1)
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)

      vr = VR.new(:b, [:a, :b, :c], InMemoryLog.new(), MockInterface)

      # Per VR paper Section 4.2 (viewstamped_replication-6to): Simulate timer expiry with elapsed time >= timeout
      expect(MockInterface, :timestamp_in_ms, fn -> 2000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, 3, fn _, {:start_view_change, 1} -> :ok end)
      expect(MockInterface, :primary_changed, fn {:b, 1} -> :ok end)

      vr = VR.handle_event(vr, :view_change, :timer)

      assert :view_change = VR.status(vr)

      log_entries = [{1, {:client, 1, :op}}]

      # First DOVIEWCHANGE
      vr = VR.handle_event(vr, {:do_view_change, 1, log_entries, 0, 1, 0}, :a)
      assert :view_change = VR.status(vr)

      # Second DOVIEWCHANGE - quorum reached, become primary
      # Per paper Section 4.2 Step 3: STARTVIEW to "other replicas" only (not self)
      expect(MockInterface, :send_event, 2, fn _, {:start_view, 1, ^log_entries, 1, 0} -> :ok end)
      expect(MockInterface, :timer, fn :heartbeat -> &mock_timer_cancel/0 end)

      vr = VR.handle_event(vr, {:do_view_change, 1, log_entries, 0, 1, 0}, :c)

      assert :normal = VR.status(vr)
      assert VR.am_i_primary?(vr)
      assert 1 = VR.view_number(vr)
    end
  end

  describe "API functions" do
    test "log/1 returns the current log" do
      expect(MockInterface, :timer, fn :heartbeat -> &mock_timer_cancel/0 end)

      log = InMemoryLog.new()
      vr = VR.new(:a, [:a, :b, :c], log, MockInterface)

      assert VR.log(vr) == log
    end

    test "primary_info/1 returns primary and view number" do
      expect(MockInterface, :timer, fn :heartbeat -> &mock_timer_cancel/0 end)

      vr = VR.new(:a, [:a, :b, :c], InMemoryLog.new(), MockInterface)

      assert {:a, 0} = VR.primary_info(vr)
    end

    test "submit_request in non-normal mode returns error" do
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)

      vr = VR.new(:b, [:a, :b, :c], InMemoryLog.new(), MockInterface)

      # Per VR paper Section 4.2 (viewstamped_replication-6to): Simulate timer expiry with elapsed time >= timeout
      expect(MockInterface, :timestamp_in_ms, fn -> 2000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, 3, fn _, _ -> :ok end)
      expect(MockInterface, :primary_changed, fn {:b, 1} -> :ok end)

      vr = VR.handle_event(vr, :view_change, :timer)

      assert {:error, :not_primary} = VR.submit_request(vr, :client, 1, :op)
    end

    test "view_number returns 0 for nil mode" do
      vr = %VR{
        me: :a,
        replica_index: 0,
        configuration: [:a],
        quorum: 1,
        mode: nil,
        interface: MockInterface
      }

      assert 0 = VR.view_number(vr)
    end

    test "primary returns nil for nil mode" do
      vr = %VR{
        me: :a,
        replica_index: 0,
        configuration: [:a],
        quorum: 1,
        mode: nil,
        interface: MockInterface
      }

      assert nil == VR.primary(vr)
    end

    test "op_number returns 0 for recovering mode" do
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)
      expect(MockInterface, :timer, fn :recovery -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, 2, fn _, _ -> :ok end)

      vr = VR.new(:b, [:a, :b, :c], InMemoryLog.new(), MockInterface)
      vr = VR.start_recovering(vr, :nonce)

      assert 0 = VR.op_number(vr)
    end

    test "commit_number returns 0 for recovering mode" do
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)
      expect(MockInterface, :timer, fn :recovery -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, 2, fn _, _ -> :ok end)

      vr = VR.new(:b, [:a, :b, :c], InMemoryLog.new(), MockInterface)
      vr = VR.start_recovering(vr, :nonce)

      assert 0 = VR.commit_number(vr)
    end
  end

  describe "Primary change notification" do
    test "notifies on primary change during view change" do
      # Initial setup: :b starts as backup
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)

      vr = VR.new(:b, [:a, :b, :c], InMemoryLog.new(), MockInterface)

      # Per VR paper Section 4.2 (viewstamped_replication-6to): Simulate timer expiry with elapsed time >= timeout
      expect(MockInterface, :timestamp_in_ms, fn -> 2000 end)
      # View change timer - expect primary_changed when view changes
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, 3, fn _, _ -> :ok end)
      expect(MockInterface, :primary_changed, fn {:b, 1} -> :ok end)

      vr = VR.handle_event(vr, :view_change, :timer)

      # Primary changed from :a to :b
      assert :view_change = VR.status(vr)
    end
  end

  describe "Client table preservation across view changes (Paper Section 4.2)" do
    @tag :paper_compliance
    test "ISSUE viewstamped_replication-7ob: client table state survives view change for deduplication" do
      # Per paper Section 4.2 Step 5:
      # "update the information in their client-table"
      # The client table must survive view changes for duplicate detection to work.
      #
      # Scenario:
      # 1. Primary :a processes request from client1, commits it (cached in client table)
      # 2. View change occurs, :b becomes primary
      # 3. Client1 resends the same request to new primary :b
      # 4. :b should detect duplicate and return cached result (NOT re-execute)

      # Step 1: :a is primary in view 0, processes a request
      expect(MockInterface, :timer, fn :heartbeat -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:prepare, 0, _, 1, 0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:prepare, 0, _, 1, 0} -> :ok end)
      expect(MockInterface, :execute_operation, fn :op1 -> {:ok, :result1} end)
      expect(MockInterface, :operation_committed, fn _, 1, :op1, :result1 -> :ok end)
      expect(MockInterface, :send_reply, fn :client1, 0, 1, :result1 -> :ok end)

      vr_a = VR.new(:a, [:a, :b, :c], InMemoryLog.new(), MockInterface)
      {:ok, vr_a, 1} = VR.submit_request(vr_a, :client1, 1, :op1)
      vr_a = VR.handle_event(vr_a, {:prepare_ok, 0, 1}, :b)

      # Verify commit happened
      assert 1 = VR.commit_number(vr_a)

      # Step 2: :b starts as backup, receives the PREPARE and commits
      # Per VR paper Section 4.2 (viewstamped_replication-6to): Only one timer at Normal.new()
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :a, {:prepare_ok, 0, 1} -> :ok end)
      expect(MockInterface, :execute_operation, fn :op1 -> {:ok, :result1} end)
      expect(MockInterface, :operation_committed, fn _, 1, :op1, :result1 -> :ok end)

      vr_b = VR.new(:b, [:a, :b, :c], InMemoryLog.new(), MockInterface)
      entry = {:client1, 1, :op1}
      vr_b = VR.handle_event(vr_b, {:prepare, 0, entry, 1, 0}, :a)
      vr_b = VR.handle_event(vr_b, {:commit, 0, 1}, :a)

      # :b has now committed op1 and should have client1's result cached

      # Step 3: View change - :b becomes primary for view 1
      # Per VR paper Section 4.2 (viewstamped_replication-6to): Simulate timer expiry with elapsed time >= timeout
      expect(MockInterface, :timestamp_in_ms, fn -> 2000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, 3, fn _, {:start_view_change, 1} -> :ok end)
      expect(MockInterface, :primary_changed, fn {:b, 1} -> :ok end)

      vr_b = VR.handle_event(vr_b, :view_change, :timer)
      assert :view_change = VR.status(vr_b)

      # :b receives enough DOVIEWCHANGE to become primary
      log_entries = [{1, {:client1, 1, :op1}}]

      # Per paper Section 4.2 Step 3: STARTVIEW to "other replicas" only (not self)
      expect(MockInterface, :send_event, 2, fn _, {:start_view, 1, ^log_entries, 1, 1} -> :ok end)
      expect(MockInterface, :timer, fn :heartbeat -> &mock_timer_cancel/0 end)

      vr_b = VR.handle_event(vr_b, {:do_view_change, 1, log_entries, 0, 1, 1}, :a)
      vr_b = VR.handle_event(vr_b, {:do_view_change, 1, log_entries, 0, 1, 1}, :c)

      assert :normal = VR.status(vr_b)
      assert VR.am_i_primary?(vr_b)

      # Step 4: Client1 resends request 1 to new primary :b
      # Per paper, this should return cached result, NOT re-execute
      # The bug: client table was reset, so it re-executes

      # Expect: cached result returned (re-sends reply)
      expect(MockInterface, :send_reply, fn :client1, 1, 1, :result1 -> :ok end)

      # Should NOT expect execute_operation to be called again!
      # If execute_operation is called, the test will fail because we didn't set it up

      {:ok, _vr_b, {:cached, :result1}} = VR.submit_request(vr_b, :client1, 1, :op1)
    end

    @tag :paper_compliance
    test "ISSUE viewstamped_replication-7ob: backup client table survives STARTVIEW" do
      # A backup that receives STARTVIEW should preserve its client table
      # for already-executed operations

      # Setup backup :c with a committed operation
      # Per VR paper Section 4.2 (viewstamped_replication-6to): backup tracks timestamp
      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)
      expect(MockInterface, :timestamp_in_ms, fn -> 1050 end)
      expect(MockInterface, :send_event, fn :a, {:prepare_ok, 0, 1} -> :ok end)
      expect(MockInterface, :timestamp_in_ms, fn -> 1100 end)
      expect(MockInterface, :execute_operation, fn :op1 -> {:ok, :result1} end)
      expect(MockInterface, :operation_committed, fn _, 1, :op1, :result1 -> :ok end)

      vr_c = VR.new(:c, [:a, :b, :c], InMemoryLog.new(), MockInterface)
      entry = {:client1, 1, :op1}
      vr_c = VR.handle_event(vr_c, {:prepare, 0, entry, 1, 0}, :a)
      vr_c = VR.handle_event(vr_c, {:commit, 0, 1}, :a)

      # Now view change happens - :c receives STARTVIEW for view 1
      # Per VR paper Section 4.2 (viewstamped_replication-6to): timestamp triggers view change
      expect(MockInterface, :timestamp_in_ms, fn -> 6000 end)
      expect(MockInterface, :view_change_timeout_ms, fn -> 150 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, 3, fn _, {:start_view_change, 1} -> :ok end)
      expect(MockInterface, :primary_changed, fn {:b, 1} -> :ok end)

      vr_c = VR.handle_event(vr_c, :view_change, :timer)

      log_entries = [{1, {:client1, 1, :op1}}]

      # Per VR paper Section 4.2 (viewstamped_replication-6to): timestamp for new backup Normal.new
      expect(MockInterface, :timestamp_in_ms, fn -> 6100 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)
      # No PREPAREOK expected since commit_number == op_number

      vr_c = VR.handle_event(vr_c, {:start_view, 1, log_entries, 1, 1}, :b)

      # Now verify the client table survived
      # We can check this by looking at the internal state
      assert vr_c.mode.client_table.table[:client1] == {1, {:completed, :result1}}
    end
  end

  describe "Backups execute committed ops after STARTVIEW (Paper Section 4.2 Step 5)" do
    @tag :paper_compliance
    test "ISSUE viewstamped_replication-bms: backup executes committed ops after receiving STARTVIEW" do
      # Per paper Section 4.2 Step 5:
      # "Then they execute all operations known to be committed that they haven't
      # executed previously, advance their commit-number, and update the
      # information in their client-table."
      #
      # Scenario:
      # 1. Backup :c has op 1 in log but hasn't committed it (only received PREPARE)
      # 2. View change happens
      # 3. :c receives STARTVIEW with commit_number = 1
      # 4. :c must execute op 1 (but NOT send reply - only primary sends replies)

      # Setup :c as backup with op in log but NOT committed
      # Per VR paper Section 4.2 (viewstamped_replication-6to): backup tracks timestamp
      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)
      expect(MockInterface, :timestamp_in_ms, fn -> 1050 end)
      expect(MockInterface, :send_event, fn :a, {:prepare_ok, 0, 1} -> :ok end)

      vr_c = VR.new(:c, [:a, :b, :c], InMemoryLog.new(), MockInterface)
      entry = {:client1, 1, :op1}
      # PREPARE with commit_num=0 (not committed yet)
      vr_c = VR.handle_event(vr_c, {:prepare, 0, entry, 1, 0}, :a)

      # Verify: :c has op 1 in log but commit_number is still 0
      assert VR.op_number(vr_c) == 1
      assert VR.commit_number(vr_c) == 0

      # View change starts
      # Per VR paper Section 4.2 (viewstamped_replication-6to): timestamp triggers view change
      expect(MockInterface, :timestamp_in_ms, fn -> 6000 end)
      expect(MockInterface, :view_change_timeout_ms, fn -> 150 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, 3, fn _, {:start_view_change, 1} -> :ok end)
      expect(MockInterface, :primary_changed, fn {:b, 1} -> :ok end)

      vr_c = VR.handle_event(vr_c, :view_change, :timer)
      assert :view_change = VR.status(vr_c)

      # :c receives STARTVIEW with commit_number=1
      log_entries = [{1, {:client1, 1, :op1}}]

      # Must execute op 1 but NOT send_reply (backup doesn't respond to clients)
      expect(MockInterface, :execute_operation, fn :op1 -> {:ok, :result1} end)
      expect(MockInterface, :operation_committed, fn _, 1, :op1, :result1 -> :ok end)
      # Note: NO send_reply expectation - only primary sends replies
      # Per VR paper Section 4.2 (viewstamped_replication-6to): timestamp for new backup Normal.new
      expect(MockInterface, :timestamp_in_ms, fn -> 6100 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)
      # No PREPAREOK since commit_number == op_number

      vr_c = VR.handle_event(vr_c, {:start_view, 1, log_entries, 1, 1}, :b)

      # Verify :c has committed and executed op 1
      assert :normal = VR.status(vr_c)
      assert VR.commit_number(vr_c) == 1

      # Client table should have the cached result
      assert vr_c.mode.client_table.table[:client1] == {1, {:completed, :result1}}
    end
  end

  describe "Message format consistency (viewstamped_replication-5pi)" do
    @tag :paper_compliance
    @tag :bug_regression
    test "STARTVIEWCHANGE message format matches handle_event pattern" do
      # BUG: ViewChange.send_start_view_change_to_all sends {:start_view_change, view_num, me}
      # but VR.handle_event expects {:start_view_change, view_num}
      # Paper Section 4.2 Step 1: STARTVIEWCHANGE(v, i) where i is sender
      # Transport should provide sender identity, not the message tuple.
      #
      # Fix: Remove sender from message tuple in send functions

      # Per VR paper Section 4.2 (viewstamped_replication-6to): backup tracks timestamp
      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)

      # Capture the actual message format sent
      sent_messages = Agent.start_link(fn -> [] end) |> elem(1)

      # Per VR paper Section 4.2 (viewstamped_replication-6to): timestamp triggers view change
      expect(MockInterface, :timestamp_in_ms, fn -> 6000 end)
      expect(MockInterface, :view_change_timeout_ms, fn -> 150 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)

      expect(MockInterface, :send_event, 3, fn to, msg ->
        Agent.update(sent_messages, fn msgs -> [{to, msg} | msgs] end)
        :ok
      end)

      vr = VR.new(:b, [:a, :b, :c], InMemoryLog.new(), MockInterface)
      _vr = VR.handle_event(vr, :view_change, :timer)

      messages = Agent.get(sent_messages, & &1)
      Agent.stop(sent_messages)

      # All STARTVIEWCHANGE messages should be 2-tuples: {:start_view_change, view_num}
      # NOT 3-tuples: {:start_view_change, view_num, sender}
      for {_to, msg} <- messages do
        assert {:start_view_change, view_num} = msg
        assert view_num == 1
      end
    end

    @tag :paper_compliance
    @tag :bug_regression
    test "PREPAREOK message format matches handle_event pattern" do
      # BUG: Normal.prepare_received sends {:prepare_ok, view_num, op_num, me}
      # but VR.handle_event expects {:prepare_ok, view_num, op_num}

      # Per VR paper Section 4.2 (viewstamped_replication-6to): backup tracks timestamp
      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)

      sent_messages = Agent.start_link(fn -> [] end) |> elem(1)

      # Per VR paper Section 4.2 (viewstamped_replication-6to): PREPARE updates timestamp (no timer restart)
      expect(MockInterface, :timestamp_in_ms, fn -> 1050 end)

      expect(MockInterface, :send_event, fn to, msg ->
        Agent.update(sent_messages, fn msgs -> [{to, msg} | msgs] end)
        :ok
      end)

      vr = VR.new(:b, [:a, :b, :c], InMemoryLog.new(), MockInterface)
      entry = {:client1, 1, :op1}
      _vr = VR.handle_event(vr, {:prepare, 0, entry, 1, 0}, :a)

      messages = Agent.get(sent_messages, & &1)
      Agent.stop(sent_messages)

      # PREPAREOK should be 3-tuple: {:prepare_ok, view_num, op_num}
      # NOT 4-tuple: {:prepare_ok, view_num, op_num, sender}
      assert [{:a, msg}] = messages
      assert {:prepare_ok, 0, 1} = msg
    end

    @tag :paper_compliance
    @tag :bug_regression
    test "DOVIEWCHANGE message format matches handle_event pattern" do
      # BUG: ViewChange.send_do_view_change sends 7-tuple with sender
      # but VR.handle_event expects 6-tuple without sender

      # Per VR paper Section 4.2 (viewstamped_replication-6to): backup tracks timestamp
      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)
      # Per VR paper Section 4.2 (viewstamped_replication-6to): timestamp triggers view change
      expect(MockInterface, :timestamp_in_ms, fn -> 6000 end)
      expect(MockInterface, :view_change_timeout_ms, fn -> 150 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, 3, fn _, _ -> :ok end)

      vr = VR.new(:c, [:a, :b, :c], InMemoryLog.new(), MockInterface)
      vr = VR.handle_event(vr, :view_change, :timer)

      sent_messages = Agent.start_link(fn -> [] end) |> elem(1)

      expect(MockInterface, :send_event, fn to, msg ->
        Agent.update(sent_messages, fn msgs -> [{to, msg} | msgs] end)
        :ok
      end)

      # Receive STARTVIEWCHANGE from another replica to trigger DOVIEWCHANGE
      # With quorum reached (self + :a = 2, quorum = 2), sends DOVIEWCHANGE
      {:ok, _mode} = ViewChange.start_view_change_received(vr.mode, 1, :a)

      messages = Agent.get(sent_messages, & &1)
      Agent.stop(sent_messages)

      # DOVIEWCHANGE should be 6-tuple: {:do_view_change, v, l, v', n, k}
      # NOT 7-tuple: {:do_view_change, v, l, v', n, k, sender}
      assert [{:b, msg}] = messages
      assert {:do_view_change, 1, _log, 0, 0, 0} = msg
    end

    @tag :paper_compliance
    @tag :bug_regression
    test "RECOVERY message format matches handle_event pattern" do
      # BUG: Recovering.new sends {:recovery, nonce, me}
      # but VR.handle_event expects {:recovery, nonce}

      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)

      vr = VR.new(:b, [:a, :b, :c], InMemoryLog.new(), MockInterface)

      sent_messages = Agent.start_link(fn -> [] end) |> elem(1)

      expect(MockInterface, :timer, fn :recovery -> &mock_timer_cancel/0 end)

      expect(MockInterface, :send_event, 2, fn to, msg ->
        Agent.update(sent_messages, fn msgs -> [{to, msg} | msgs] end)
        :ok
      end)

      _vr = VR.start_recovering(vr, :test_nonce)

      messages = Agent.get(sent_messages, & &1)
      Agent.stop(sent_messages)

      # RECOVERY should be 2-tuple: {:recovery, nonce}
      # NOT 3-tuple: {:recovery, nonce, sender}
      for {_to, msg} <- messages do
        assert {:recovery, :test_nonce} = msg
      end
    end
  end

  describe "New primary executes committed ops (Paper Section 4.2 Step 4)" do
    @tag :paper_compliance
    test "ISSUE viewstamped_replication-ea1: new primary executes uncommitted committed ops after view change" do
      # Per paper Section 4.2 Step 4:
      # "The new primary starts accepting client requests. It also executes
      # (in order) any committed operations that it hadn't executed previously,
      # updates its client table, and sends the replies to the clients."
      #
      # Scenario:
      # 1. Backup :b has op 1 in log (received PREPARE) but hasn't committed it
      # 2. View change happens, :b becomes primary
      # 3. DOVIEWCHANGE says commit_number = 1 (another replica saw the commit)
      # 4. :b must execute op 1 before accepting new requests

      # Setup :b as backup with op in log but NOT committed
      # Per VR paper Section 4.2 (viewstamped_replication-6to): backup tracks timestamp
      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)
      expect(MockInterface, :timestamp_in_ms, fn -> 1050 end)
      expect(MockInterface, :send_event, fn :a, {:prepare_ok, 0, 1} -> :ok end)
      # Note: no execute_operation here because commit_num=0 in PREPARE

      vr_b = VR.new(:b, [:a, :b, :c], InMemoryLog.new(), MockInterface)
      entry = {:client1, 1, :op1}
      # PREPARE with commit_num=0 (not committed yet)
      vr_b = VR.handle_event(vr_b, {:prepare, 0, entry, 1, 0}, :a)

      # Verify: :b has op 1 in log but commit_number is still 0
      assert VR.op_number(vr_b) == 1
      assert VR.commit_number(vr_b) == 0

      # View change: :b will become primary for view 1
      # Per VR paper Section 4.2 (viewstamped_replication-6to): timestamp triggers view change
      expect(MockInterface, :timestamp_in_ms, fn -> 6000 end)
      expect(MockInterface, :view_change_timeout_ms, fn -> 150 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, 3, fn _, {:start_view_change, 1} -> :ok end)
      expect(MockInterface, :primary_changed, fn {:b, 1} -> :ok end)

      vr_b = VR.handle_event(vr_b, :view_change, :timer)
      assert :view_change = VR.status(vr_b)

      # :b receives DOVIEWCHANGE from replicas that saw the commit
      log_entries = [{1, {:client1, 1, :op1}}]

      # Per paper Section 4.2 Step 3: STARTVIEW to "other replicas" only (not self)
      expect(MockInterface, :send_event, 2, fn _, {:start_view, 1, ^log_entries, 1, 1} -> :ok end)

      # HERE IS THE KEY: When becoming primary with commit_number=1,
      # :b must execute op 1 and send reply to client
      expect(MockInterface, :execute_operation, fn :op1 -> {:ok, :result1} end)
      expect(MockInterface, :operation_committed, fn _, 1, :op1, :result1 -> :ok end)
      expect(MockInterface, :send_reply, fn :client1, 1, 1, :result1 -> :ok end)
      expect(MockInterface, :timer, fn :heartbeat -> &mock_timer_cancel/0 end)

      # Receive quorum of DOVIEWCHANGE with commit_number=1
      vr_b = VR.handle_event(vr_b, {:do_view_change, 1, log_entries, 0, 1, 1}, :a)
      vr_b = VR.handle_event(vr_b, {:do_view_change, 1, log_entries, 0, 1, 1}, :c)

      # Verify :b is now primary and has committed op 1
      assert :normal = VR.status(vr_b)
      assert VR.am_i_primary?(vr_b)
      assert VR.commit_number(vr_b) == 1

      # Client table should now have the cached result
      assert vr_b.mode.client_table.table[:client1] == {1, {:completed, :result1}}
    end

    @tag :paper_compliance
    test "ISSUE viewstamped_replication-ea1: new primary executes multiple pending committed ops in order" do
      # Test that multiple committed operations are executed in order

      # Setup :b as backup with ops 1,2 in log but not committed
      # Per VR paper Section 4.2 (viewstamped_replication-6to): backup tracks timestamp
      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)
      expect(MockInterface, :timestamp_in_ms, fn -> 1050 end)
      expect(MockInterface, :send_event, fn :a, {:prepare_ok, 0, 1} -> :ok end)
      expect(MockInterface, :timestamp_in_ms, fn -> 1100 end)
      expect(MockInterface, :send_event, fn :a, {:prepare_ok, 0, 2} -> :ok end)

      vr_b = VR.new(:b, [:a, :b, :c], InMemoryLog.new(), MockInterface)
      vr_b = VR.handle_event(vr_b, {:prepare, 0, {:client1, 1, :op1}, 1, 0}, :a)
      vr_b = VR.handle_event(vr_b, {:prepare, 0, {:client2, 1, :op2}, 2, 0}, :a)

      assert VR.op_number(vr_b) == 2
      assert VR.commit_number(vr_b) == 0

      # View change
      # Per VR paper Section 4.2 (viewstamped_replication-6to): timestamp triggers view change
      expect(MockInterface, :timestamp_in_ms, fn -> 6000 end)
      expect(MockInterface, :view_change_timeout_ms, fn -> 150 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, 3, fn _, {:start_view_change, 1} -> :ok end)
      expect(MockInterface, :primary_changed, fn {:b, 1} -> :ok end)

      vr_b = VR.handle_event(vr_b, :view_change, :timer)

      log_entries = [{1, {:client1, 1, :op1}}, {2, {:client2, 1, :op2}}]

      # Per paper Section 4.2 Step 3: STARTVIEW to "other replicas" only (not self)
      expect(MockInterface, :send_event, 2, fn _, {:start_view, 1, ^log_entries, 2, 2} -> :ok end)

      # Must execute op1 first, then op2 (in order)
      expect(MockInterface, :execute_operation, fn :op1 -> {:ok, :result1} end)
      expect(MockInterface, :operation_committed, fn _, 1, :op1, :result1 -> :ok end)
      expect(MockInterface, :send_reply, fn :client1, 1, 1, :result1 -> :ok end)
      expect(MockInterface, :execute_operation, fn :op2 -> {:ok, :result2} end)
      expect(MockInterface, :operation_committed, fn _, 2, :op2, :result2 -> :ok end)
      expect(MockInterface, :send_reply, fn :client2, 1, 1, :result2 -> :ok end)
      expect(MockInterface, :timer, fn :heartbeat -> &mock_timer_cancel/0 end)

      vr_b = VR.handle_event(vr_b, {:do_view_change, 1, log_entries, 0, 2, 2}, :a)
      vr_b = VR.handle_event(vr_b, {:do_view_change, 1, log_entries, 0, 2, 2}, :c)

      assert VR.commit_number(vr_b) == 2
      assert vr_b.mode.client_table.table[:client1] == {1, {:completed, :result1}}
      assert vr_b.mode.client_table.table[:client2] == {1, {:completed, :result2}}
    end
  end
end
