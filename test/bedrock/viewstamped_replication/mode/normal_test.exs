defmodule Bedrock.ViewstampedReplication.Mode.NormalTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Bedrock.ViewstampedReplication.Mode.Normal
  alias Bedrock.ViewstampedReplication.StateStore
  alias Bedrock.ViewstampedReplication.StateStore.InMemoryLog

  import Mox

  alias Bedrock.ViewstampedReplication.MockInterface

  setup :verify_on_exit!

  def mock_cancel, do: :ok

  describe "new/9 as primary" do
    test "creates primary mode with heartbeat timer" do
      expect(MockInterface, :timer, fn :heartbeat -> &mock_cancel/0 end)

      log = InMemoryLog.new()

      mode =
        Normal.new(
          0,
          0,
          0,
          log,
          [:a, :b, :c],
          0,
          2,
          MockInterface,
          true
        )

      assert mode.is_primary == true
      assert mode.view_number == 0
      assert mode.op_number == 0
      assert mode.commit_number == 0
      assert mode.backup_tracking != nil
    end
  end

  describe "new/9 as backup" do
    test "creates backup mode with view_change timer" do
      # Per VR paper Section 4.2 (viewstamped_replication-6to): Backup records last_heard_at on creation
      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_cancel/0 end)

      log = InMemoryLog.new()

      mode =
        Normal.new(
          0,
          0,
          0,
          log,
          [:a, :b, :c],
          1,
          2,
          MockInterface,
          false
        )

      assert mode.is_primary == false
      assert mode.backup_tracking == nil
      assert mode.last_heard_at == 1000
    end
  end

  describe "timer_ticked/2 as primary" do
    test "sends commit to all backups on heartbeat" do
      expect(MockInterface, :timer, fn :heartbeat -> &mock_cancel/0 end)

      log = InMemoryLog.new()
      mode = Normal.new(0, 5, 3, log, [:a, :b, :c], 0, 2, MockInterface, true)

      # Expect COMMIT to be sent to backups :b and :c
      expect(MockInterface, :send_event, fn :b, {:commit, 0, 3} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:commit, 0, 3} -> :ok end)
      expect(MockInterface, :timer, fn :heartbeat -> &mock_cancel/0 end)

      {:ok, updated_mode} = Normal.timer_ticked(mode, :heartbeat)
      assert updated_mode.cancel_timer_fn != nil
    end
  end

  describe "timer_ticked/2 as backup" do
    test "triggers view change when timeout exceeded (viewstamped_replication-6to)" do
      # Per VR paper Section 4.2: Check at timeout time whether primary was heard from recently
      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_cancel/0 end)

      log = InMemoryLog.new()
      mode = Normal.new(0, 0, 0, log, [:a, :b, :c], 1, 2, MockInterface, false)

      # Simulate timeout check after 6 seconds (timeout is 5s)
      expect(MockInterface, :timestamp_in_ms, fn -> 7000 end)
      expect(MockInterface, :view_change_timeout_ms, fn -> 5000 end)

      assert :start_view_change = Normal.timer_ticked(mode, :view_change)
    end

    test "restarts timer when heard from primary recently (viewstamped_replication-6to)" do
      # Per VR paper Section 4.2: If we heard from primary within timeout, just restart timer
      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_cancel/0 end)

      log = InMemoryLog.new()
      mode = Normal.new(0, 0, 0, log, [:a, :b, :c], 1, 2, MockInterface, false)

      # Simulate timeout check after 3 seconds (timeout is 5s) - should restart timer
      expect(MockInterface, :timestamp_in_ms, fn -> 4000 end)
      expect(MockInterface, :view_change_timeout_ms, fn -> 5000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_cancel/0 end)

      {:ok, updated_mode} = Normal.timer_ticked(mode, :view_change)
      assert updated_mode.cancel_timer_fn != nil
    end
  end

  describe "request_received/4" do
    test "non-primary rejects request" do
      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_cancel/0 end)

      log = InMemoryLog.new()
      mode = Normal.new(0, 0, 0, log, [:a, :b, :c], 1, 2, MockInterface, false)

      assert {:error, :not_primary} = Normal.request_received(mode, :client, 1, :op)
    end

    test "primary processes new request" do
      expect(MockInterface, :timer, fn :heartbeat -> &mock_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:prepare, 0, {:client, 1, :op}, 1, 0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:prepare, 0, {:client, 1, :op}, 1, 0} -> :ok end)

      log = InMemoryLog.new()
      mode = Normal.new(0, 0, 0, log, [:a, :b, :c], 0, 2, MockInterface, true)

      {:ok, updated_mode, op_num} = Normal.request_received(mode, :client, 1, :op)

      assert op_num == 1
      assert updated_mode.op_number == 1
    end

    test "primary returns cached result for duplicate" do
      expect(MockInterface, :timer, fn :heartbeat -> &mock_cancel/0 end)
      expect(MockInterface, :send_event, 2, fn _, _ -> :ok end)
      expect(MockInterface, :execute_operation, fn :op -> {:ok, :result} end)
      expect(MockInterface, :operation_committed, fn _, 1, :op, :result -> :ok end)
      expect(MockInterface, :send_reply, fn :client, 0, 1, :result -> :ok end)

      log = InMemoryLog.new()
      mode = Normal.new(0, 0, 0, log, [:a, :b, :c], 0, 2, MockInterface, true)

      # Submit request
      {:ok, mode, 1} = Normal.request_received(mode, :client, 1, :op)

      # Simulate quorum ack to commit
      {:ok, mode} = Normal.prepare_ok_received(mode, 0, 1, :b)

      # Duplicate request returns cached result
      expect(MockInterface, :send_reply, fn :client, 0, 1, :result -> :ok end)
      {:ok, _mode, {:cached, :result}} = Normal.request_received(mode, :client, 1, :op)
    end

    test "primary drops duplicate pending request" do
      expect(MockInterface, :timer, fn :heartbeat -> &mock_cancel/0 end)
      expect(MockInterface, :send_event, 2, fn _, _ -> :ok end)

      log = InMemoryLog.new()
      mode = Normal.new(0, 0, 0, log, [:a, :b, :c], 0, 2, MockInterface, true)

      {:ok, mode, 1} = Normal.request_received(mode, :client, 1, :op)

      # Same request again (still pending) - returns same op_number
      {:ok, _mode, 1} = Normal.request_received(mode, :client, 1, :op)
    end
  end

  describe "prepare_received/6" do
    test "ignores prepare from old view" do
      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_cancel/0 end)

      log = InMemoryLog.new()
      {:ok, log} = StateStore.save_view_number(log, 5)
      mode = Normal.new(5, 0, 0, log, [:a, :b, :c], 1, 2, MockInterface, false)

      {:ok, same_mode} = Normal.prepare_received(mode, 3, :msg, 1, 0, :a)
      assert same_mode.op_number == 0
    end

    test "triggers view change for newer view" do
      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_cancel/0 end)

      log = InMemoryLog.new()
      mode = Normal.new(0, 0, 0, log, [:a, :b, :c], 1, 2, MockInterface, false)

      result = Normal.prepare_received(mode, 5, :msg, 1, 0, :a)
      assert {:start_view_change, 5} = result
    end

    test "primary ignores prepare" do
      expect(MockInterface, :timer, fn :heartbeat -> &mock_cancel/0 end)

      log = InMemoryLog.new()
      mode = Normal.new(0, 0, 0, log, [:a, :b, :c], 0, 2, MockInterface, true)

      {:ok, same_mode} = Normal.prepare_received(mode, 0, :msg, 1, 0, :b)
      assert same_mode.op_number == 0
    end

    test "backup processes prepare and sends prepareok" do
      # Per VR paper Section 4.2 (viewstamped_replication-6to): Only one timer at Normal.new(),
      # message handlers update last_heard_at instead of restarting timer
      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_cancel/0 end)
      # Per paper Section 4.1 Step 4: PREPAREOK(v, n, i) where i is sender
      # Sender identity is provided by transport layer, not the message tuple
      expect(MockInterface, :timestamp_in_ms, fn -> 2000 end)
      expect(MockInterface, :send_event, fn :a, {:prepare_ok, 0, 1} -> :ok end)

      log = InMemoryLog.new()
      mode = Normal.new(0, 0, 0, log, [:a, :b, :c], 1, 2, MockInterface, false)

      entry = {:client, 1, :op}
      {:ok, updated_mode} = Normal.prepare_received(mode, 0, entry, 1, 0, :a)

      assert updated_mode.op_number == 1
      # last_heard_at should be updated to 2000
      assert updated_mode.last_heard_at == 2000
    end

    test "backup updates client-table on PREPARE per paper Section 4.1 (viewstamped_replication-bpj)" do
      # Paper Section 4.1 Step 4:
      # "Then it increments its op-number, adds the request to the end of its log,
      # updates the client's information in the client-table, and sends a PREPAREOK..."
      # Per VR paper Section 4.2 (viewstamped_replication-6to): Only one timer at Normal.new()
      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_cancel/0 end)
      # Per paper Section 4.1 Step 4: PREPAREOK(v, n, i) where i is sender
      expect(MockInterface, :timestamp_in_ms, fn -> 2000 end)
      expect(MockInterface, :send_event, fn :a, {:prepare_ok, 0, 1} -> :ok end)

      log = InMemoryLog.new()
      mode = Normal.new(0, 0, 0, log, [:a, :b, :c], 1, 2, MockInterface, false)

      # Initial client table should have no record of this client
      alias Bedrock.ViewstampedReplication.ClientTable
      assert ClientTable.check_request(mode.client_table, :client1, 1) == :new

      entry = {:client1, 1, :op1}
      {:ok, updated_mode} = Normal.prepare_received(mode, 0, entry, 1, 0, :a)

      # Client table should now have pending request for client1, request 1
      # A duplicate request should be detected
      assert ClientTable.check_request(updated_mode.client_table, :client1, 1) == :duplicate
    end

    test "backup sends GETSTATE when prepare has gap per paper Section 4.1 (viewstamped_replication-38v)" do
      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_cancel/0 end)

      log = InMemoryLog.new()
      # Backup :b is at replica_index 1, primary :a is at index 0
      mode = Normal.new(0, 0, 0, log, [:a, :b, :c], 1, 2, MockInterface, false)

      # Skip op 1, try to append op 2 - backup should request state transfer
      entry = {:client, 2, :op}

      # Update last_heard_at since we heard from primary, then request state transfer
      expect(MockInterface, :timestamp_in_ms, fn -> 2000 end)
      # Expect GETSTATE sent to primary :a, requesting entries from op 1
      expect(MockInterface, :send_event, fn :a, {:get_state, 0, 1} -> :ok end)

      {:ok, same_mode} = Normal.prepare_received(mode, 0, entry, 2, 0, :a)

      # Op number should still be 0 - we haven't received the missing entries yet
      assert same_mode.op_number == 0
    end
  end

  describe "prepare_ok_received/4" do
    test "ignores prepareok from wrong view" do
      expect(MockInterface, :timer, fn :heartbeat -> &mock_cancel/0 end)

      log = InMemoryLog.new()
      mode = Normal.new(5, 0, 0, log, [:a, :b, :c], 0, 2, MockInterface, true)

      {:ok, same_mode} = Normal.prepare_ok_received(mode, 3, 1, :b)
      assert same_mode == mode
    end

    test "backup ignores prepareok" do
      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_cancel/0 end)

      log = InMemoryLog.new()
      mode = Normal.new(0, 0, 0, log, [:a, :b, :c], 1, 2, MockInterface, false)

      {:ok, same_mode} = Normal.prepare_ok_received(mode, 0, 1, :a)
      assert same_mode == mode
    end

    test "primary commits when quorum reached" do
      expect(MockInterface, :timer, fn :heartbeat -> &mock_cancel/0 end)
      expect(MockInterface, :send_event, 2, fn _, _ -> :ok end)
      expect(MockInterface, :execute_operation, fn :op -> {:ok, :result} end)
      expect(MockInterface, :operation_committed, fn _, 1, :op, :result -> :ok end)
      expect(MockInterface, :send_reply, fn :client, 0, 1, :result -> :ok end)

      log = InMemoryLog.new()
      mode = Normal.new(0, 0, 0, log, [:a, :b, :c], 0, 2, MockInterface, true)

      {:ok, mode, 1} = Normal.request_received(mode, :client, 1, :op)
      {:ok, mode} = Normal.prepare_ok_received(mode, 0, 1, :b)

      assert mode.commit_number == 1
    end
  end

  describe "commit_received/3" do
    test "ignores commit from old view" do
      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_cancel/0 end)

      log = InMemoryLog.new()
      {:ok, log} = StateStore.save_view_number(log, 5)
      mode = Normal.new(5, 0, 0, log, [:a, :b, :c], 1, 2, MockInterface, false)

      {:ok, same_mode} = Normal.commit_received(mode, 3, 1)
      assert same_mode.commit_number == 0
    end

    test "triggers view change for newer view" do
      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_cancel/0 end)

      log = InMemoryLog.new()
      mode = Normal.new(0, 0, 0, log, [:a, :b, :c], 1, 2, MockInterface, false)

      result = Normal.commit_received(mode, 5, 1)
      assert {:start_view_change, 5} = result
    end

    test "primary ignores commit" do
      expect(MockInterface, :timer, fn :heartbeat -> &mock_cancel/0 end)

      log = InMemoryLog.new()
      mode = Normal.new(0, 0, 0, log, [:a, :b, :c], 0, 2, MockInterface, true)

      {:ok, same_mode} = Normal.commit_received(mode, 0, 1)
      assert same_mode == mode
    end

    test "backup executes committed operations" do
      # Per VR paper Section 4.2 (viewstamped_replication-6to): Only one timer at Normal.new()
      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_cancel/0 end)
      # Per paper Section 4.1 Step 4: PREPAREOK(v, n, i) where i is sender
      expect(MockInterface, :timestamp_in_ms, fn -> 2000 end)
      expect(MockInterface, :send_event, fn :a, {:prepare_ok, 0, 1} -> :ok end)
      expect(MockInterface, :timestamp_in_ms, fn -> 3000 end)
      expect(MockInterface, :execute_operation, fn :op -> {:ok, :result} end)
      expect(MockInterface, :operation_committed, fn _, 1, :op, :result -> :ok end)

      log = InMemoryLog.new()
      mode = Normal.new(0, 0, 0, log, [:a, :b, :c], 1, 2, MockInterface, false)

      # Receive prepare
      entry = {:client, 1, :op}
      {:ok, mode} = Normal.prepare_received(mode, 0, entry, 1, 0, :a)

      # Receive commit
      {:ok, mode} = Normal.commit_received(mode, 0, 1)

      assert mode.commit_number == 1
    end
  end

  describe "start_view_change_received/3" do
    test "ignores old view" do
      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_cancel/0 end)

      log = InMemoryLog.new()
      {:ok, log} = StateStore.save_view_number(log, 5)
      mode = Normal.new(5, 0, 0, log, [:a, :b, :c], 1, 2, MockInterface, false)

      {:ok, same_mode} = Normal.start_view_change_received(mode, 3, :c)
      assert same_mode == mode
    end

    test "triggers view change for newer view" do
      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_cancel/0 end)

      log = InMemoryLog.new()
      mode = Normal.new(0, 0, 0, log, [:a, :b, :c], 1, 2, MockInterface, false)

      result = Normal.start_view_change_received(mode, 5, :c)
      assert {:start_view_change, 5} = result
    end
  end

  describe "start_view_received/5" do
    test "ignores old view" do
      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_cancel/0 end)

      log = InMemoryLog.new()
      {:ok, log} = StateStore.save_view_number(log, 5)
      mode = Normal.new(5, 0, 0, log, [:a, :b, :c], 1, 2, MockInterface, false)

      {:ok, same_mode} = Normal.start_view_received(mode, 3, [], 0, 0)
      assert same_mode == mode
    end

    test "becomes normal with new log" do
      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_cancel/0 end)

      log = InMemoryLog.new()
      mode = Normal.new(0, 0, 0, log, [:a, :b, :c], 1, 2, MockInterface, false)

      log_entries = [{1, {:client, 1, :op}}]
      result = Normal.start_view_received(mode, 5, log_entries, 1, 1)

      assert {:become_normal, 5, 1, 1, _new_log} = result
    end
  end

  describe "recovery_received/3" do
    test "responds with recovery response" do
      expect(MockInterface, :timer, fn :heartbeat -> &mock_cancel/0 end)

      expect(MockInterface, :send_event, fn :recovering,
                                            {:recovery_response, 0, :nonce, {:incremental, []}, 0,
                                             0, true} ->
        :ok
      end)

      log = InMemoryLog.new()
      mode = Normal.new(0, 0, 0, log, [:a, :b, :c], 0, 2, MockInterface, true)

      {:ok, same_mode} = Normal.recovery_received(mode, :nonce, :recovering)
      assert same_mode == mode
    end

    test "backup sends nil for log, op_number, and commit_number per paper Section 4.3 (viewstamped_replication-aqx)" do
      # Paper Section 4.3 Step 2:
      # "If j is the primary of its view, l is its log, n is its op-number, and k
      # is the commit-number; otherwise these values are nil."
      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_cancel/0 end)

      expect(MockInterface, :send_event, fn :recovering,
                                            {:recovery_response, 0, :nonce, nil, nil, nil, false} ->
        :ok
      end)

      log = InMemoryLog.new()
      mode = Normal.new(0, 0, 0, log, [:a, :b, :c], 1, 2, MockInterface, false)

      {:ok, _mode} = Normal.recovery_received(mode, :nonce, :recovering)
    end
  end

  describe "do_view_change_received/7" do
    test "normal mode ignores do_view_change" do
      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_cancel/0 end)

      log = InMemoryLog.new()
      mode = Normal.new(0, 0, 0, log, [:a, :b, :c], 1, 2, MockInterface, false)

      {:ok, same_mode} = Normal.do_view_change_received(mode, 1, [], 0, 0, 0, :c)
      assert same_mode == mode
    end
  end

  describe "recovery_response_received/8" do
    test "normal mode ignores recovery_response" do
      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_cancel/0 end)

      log = InMemoryLog.new()
      mode = Normal.new(0, 0, 0, log, [:a, :b, :c], 1, 2, MockInterface, false)

      {:ok, same_mode} = Normal.recovery_response_received(mode, 0, :nonce, [], 0, 0, false, :c)
      assert same_mode == mode
    end
  end

  describe "get_state_received/4 (State Transfer - Section 5.2)" do
    test "primary responds with log entries from requested op number" do
      expect(MockInterface, :timer, fn :heartbeat -> &mock_cancel/0 end)

      log = InMemoryLog.new()
      {:ok, log} = StateStore.record_pending(log, 1, {:c1, 1, :op1})
      {:ok, log} = StateStore.record_pending(log, 2, {:c1, 2, :op2})
      {:ok, log} = StateStore.record_pending(log, 3, {:c1, 3, :op3})

      # New primary executes committed ops 1 and 2 (per paper Section 4.2 Step 4)
      expect(MockInterface, :execute_operation, fn :op1 -> {:ok, :result1} end)
      expect(MockInterface, :operation_committed, fn _, 1, :op1, :result1 -> :ok end)
      expect(MockInterface, :send_reply, fn :c1, 0, 1, :result1 -> :ok end)
      expect(MockInterface, :execute_operation, fn :op2 -> {:ok, :result2} end)
      expect(MockInterface, :operation_committed, fn _, 2, :op2, :result2 -> :ok end)
      expect(MockInterface, :send_reply, fn :c1, 0, 2, :result2 -> :ok end)

      mode = Normal.new(0, 3, 2, log, [:a, :b, :c], 0, 2, MockInterface, true)

      # Expect primary to send NEWSTATE with entries from op 2 onwards
      expect(MockInterface, :send_event, fn :backup,
                                            {:new_state, 0,
                                             {:incremental,
                                              [{2, {:c1, 2, :op2}}, {3, {:c1, 3, :op3}}]}, 3,
                                             2} ->
        :ok
      end)

      {:ok, same_mode} = Normal.get_state_received(mode, 0, 2, :backup)
      assert same_mode == mode
    end

    test "backup responds to GETSTATE per paper Section 5.2 (viewstamped_replication-orh)" do
      # Paper Section 5.2:
      # "A replica responds to a GETSTATE message only if its status is normal
      # and it is currently in view v."
      # Note: ANY replica can respond, not just primary
      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_cancel/0 end)

      log = InMemoryLog.new()
      {:ok, log} = StateStore.record_pending(log, 1, {:c1, 1, :op1})
      {:ok, log} = StateStore.record_pending(log, 2, {:c1, 2, :op2})

      # Backup executes committed op 1 (per paper Section 4.2 Step 5)
      expect(MockInterface, :execute_operation, fn :op1 -> {:ok, :result1} end)
      expect(MockInterface, :operation_committed, fn _, 1, :op1, :result1 -> :ok end)
      # Note: backup does NOT send reply

      mode = Normal.new(0, 2, 1, log, [:a, :b, :c], 1, 2, MockInterface, false)

      # Backup should respond with NEWSTATE
      expect(MockInterface, :send_event, fn :requester,
                                            {:new_state, 0, {:incremental, [{2, {:c1, 2, :op2}}]},
                                             2, 1} ->
        :ok
      end)

      {:ok, same_mode} = Normal.get_state_received(mode, 0, 2, :requester)
      assert same_mode == mode
    end

    test "ignores GETSTATE from old view" do
      expect(MockInterface, :timer, fn :heartbeat -> &mock_cancel/0 end)

      log = InMemoryLog.new()
      mode = Normal.new(5, 0, 0, log, [:a, :b, :c], 0, 2, MockInterface, true)

      # No send_event expected - old view
      {:ok, same_mode} = Normal.get_state_received(mode, 3, 1, :requester)
      assert same_mode == mode
    end

    test "triggers view change for GETSTATE from newer view" do
      expect(MockInterface, :timer, fn :heartbeat -> &mock_cancel/0 end)

      log = InMemoryLog.new()
      mode = Normal.new(0, 0, 0, log, [:a, :b, :c], 0, 2, MockInterface, true)

      result = Normal.get_state_received(mode, 5, 1, :requester)
      assert {:start_view_change, 5} = result
    end
  end

  describe "new_state_received/6 (State Transfer - Section 5.2)" do
    test "backup merges entries and executes committed operations" do
      # Per VR paper Section 4.2 (viewstamped_replication-6to): Only one timer at Normal.new()
      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_cancel/0 end)

      # Backup starts with only op 1
      log = InMemoryLog.new()
      {:ok, log} = StateStore.record_pending(log, 1, {:c1, 1, :op1})

      # Backup executes committed op 1 on creation (per paper Section 4.2 Step 5)
      expect(MockInterface, :execute_operation, fn :op1 -> {:ok, :result1} end)
      expect(MockInterface, :operation_committed, fn _, 1, :op1, :result1 -> :ok end)

      mode = Normal.new(0, 1, 1, log, [:a, :b, :c], 1, 2, MockInterface, false)

      # Receives NEWSTATE with ops 2 and 3
      new_entries = [{2, {:c1, 2, :op2}}, {3, {:c1, 3, :op3}}]

      # Update last_heard_at since we heard from another replica, then execute new ops
      expect(MockInterface, :timestamp_in_ms, fn -> 2000 end)
      # Expect execution of newly committed operations
      expect(MockInterface, :execute_operation, fn :op2 -> {:ok, :result2} end)
      expect(MockInterface, :operation_committed, fn _, 2, :op2, :result2 -> :ok end)
      expect(MockInterface, :execute_operation, fn :op3 -> {:ok, :result3} end)
      expect(MockInterface, :operation_committed, fn _, 3, :op3, :result3 -> :ok end)

      {:ok, updated_mode} = Normal.new_state_received(mode, 0, new_entries, 3, 3, :primary)

      assert updated_mode.op_number == 3
      assert updated_mode.commit_number == 3
    end

    test "primary ignores NEWSTATE" do
      expect(MockInterface, :timer, fn :heartbeat -> &mock_cancel/0 end)

      log = InMemoryLog.new()
      mode = Normal.new(0, 0, 0, log, [:a, :b, :c], 0, 2, MockInterface, true)

      {:ok, same_mode} = Normal.new_state_received(mode, 0, [], 0, 0, :sender)
      assert same_mode == mode
    end

    test "ignores NEWSTATE from old view" do
      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_cancel/0 end)

      log = InMemoryLog.new()
      mode = Normal.new(5, 0, 0, log, [:a, :b, :c], 1, 2, MockInterface, false)

      {:ok, same_mode} = Normal.new_state_received(mode, 3, [], 0, 0, :sender)
      assert same_mode == mode
    end

    test "triggers view change for NEWSTATE from newer view" do
      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_cancel/0 end)

      log = InMemoryLog.new()
      mode = Normal.new(0, 0, 0, log, [:a, :b, :c], 1, 2, MockInterface, false)

      result = Normal.new_state_received(mode, 5, [], 0, 0, :sender)
      assert {:start_view_change, 5} = result
    end

    @tag :paper_compliance
    test "state transfer preserves existing log entries (Vanlightly 2022 bug fix)" do
      # This test documents an intentional deviation from the VR Revisited paper.
      #
      # The paper's Section 5.2 state transfer protocol has a bug discovered by
      # Jack Vanlightly (2022): if the lagging replica has committed entries that
      # the primary doesn't include in NEWSTATE, those entries would be lost when
      # the paper's algorithm replaces the log.
      #
      # Our implementation MERGES entries instead of replacing, preserving any
      # entries the backup already has. This is safe because:
      # 1. Committed entries are durable and must not be lost
      # 2. The primary sends entries the backup is missing
      # 3. Merging ensures no committed data is dropped
      #
      # Reference: https://jack-vanlightly.com/analyses/2022/12/28/paper-vr-revisited-state-transfer-part-3

      # Per VR paper Section 4.2 (viewstamped_replication-6to): Only one timer at Normal.new()
      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :view_change -> &mock_cancel/0 end)

      # Backup has entries 1-2 committed
      log = InMemoryLog.new()
      {:ok, log} = StateStore.record_pending(log, 1, {:c1, 1, :op1})
      {:ok, log} = StateStore.record_pending(log, 2, {:c1, 2, :op2})

      # Execute committed ops on creation
      expect(MockInterface, :execute_operation, fn :op1 -> {:ok, :result1} end)
      expect(MockInterface, :operation_committed, fn _, 1, :op1, :result1 -> :ok end)
      expect(MockInterface, :execute_operation, fn :op2 -> {:ok, :result2} end)
      expect(MockInterface, :operation_committed, fn _, 2, :op2, :result2 -> :ok end)

      mode = Normal.new(0, 2, 2, log, [:a, :b, :c], 1, 2, MockInterface, false)

      # Primary sends NEWSTATE with only entry 3 (doesn't resend 1-2)
      # Paper's algorithm would REPLACE log, losing entries 1-2
      # Our implementation MERGES, preserving 1-2
      new_entries = [{3, {:c1, 3, :op3}}]

      # Update last_heard_at since we heard from another replica
      expect(MockInterface, :timestamp_in_ms, fn -> 2000 end)
      expect(MockInterface, :execute_operation, fn :op3 -> {:ok, :result3} end)
      expect(MockInterface, :operation_committed, fn _, 3, :op3, :result3 -> :ok end)

      {:ok, updated_mode} = Normal.new_state_received(mode, 0, new_entries, 3, 3, :primary)

      # Verify original entries are preserved (not lost per paper's bug)
      # Using InMemoryLog's internal entries map directly for verification
      store = updated_mode.store
      assert Map.get(store.entries, 1) == {:c1, 1, :op1}
      assert Map.get(store.entries, 2) == {:c1, 2, :op2}
      assert Map.get(store.entries, 3) == {:c1, 3, :op3}
    end
  end
end
