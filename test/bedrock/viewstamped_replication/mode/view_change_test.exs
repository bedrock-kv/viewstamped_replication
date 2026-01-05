defmodule Bedrock.ViewstampedReplication.Mode.ViewChangeTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Bedrock.ViewstampedReplication.ClientTable
  alias Bedrock.ViewstampedReplication.Mode.ViewChange
  alias Bedrock.ViewstampedReplication.StateStore.InMemoryLog

  import Mox

  alias Bedrock.ViewstampedReplication.MockInterface

  setup :verify_on_exit!

  def mock_cancel, do: :ok

  describe "new/10" do
    test "creates view change mode and sends STARTVIEWCHANGE to all replicas" do
      expect(MockInterface, :timer, fn :view_change -> &mock_cancel/0 end)
      # Per paper Section 4.2 Step 1: STARTVIEWCHANGE(v, i) where i is sender
      # Sender identity is provided by transport layer, not the message tuple
      expect(MockInterface, :send_event, fn :a, {:start_view_change, 1} -> :ok end)
      expect(MockInterface, :send_event, fn :b, {:start_view_change, 1} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:start_view_change, 1} -> :ok end)

      log = InMemoryLog.new()

      mode =
        ViewChange.new(
          1,
          0,
          0,
          0,
          log,
          ClientTable.new(),
          [:a, :b, :c],
          1,
          2,
          MockInterface
        )

      assert %ViewChange{
               view_number: 1,
               last_normal_view: 0,
               op_number: 0,
               commit_number: 0,
               configuration: [:a, :b, :c],
               replica_index: 1,
               quorum: 2,
               am_new_primary: true,
               start_view_change_from: %MapSet{},
               do_view_change_messages: %{},
               sent_do_view_change: false
             } = mode
    end

    test "correctly identifies when this replica is the new primary" do
      expect(MockInterface, :timer, fn :view_change -> &mock_cancel/0 end)
      expect(MockInterface, :send_event, 3, fn _, _ -> :ok end)

      log = InMemoryLog.new()

      # View 1: primary = 1 rem 3 = 1, so :b (index 1) is primary
      mode = ViewChange.new(1, 0, 0, 0, log, ClientTable.new(), [:a, :b, :c], 1, 2, MockInterface)
      assert mode.am_new_primary == true

      # View 1: :c (index 2) is not primary
      expect(MockInterface, :timer, fn :view_change -> &mock_cancel/0 end)
      expect(MockInterface, :send_event, 3, fn _, _ -> :ok end)

      mode2 =
        ViewChange.new(1, 0, 0, 0, log, ClientTable.new(), [:a, :b, :c], 2, 2, MockInterface)

      assert mode2.am_new_primary == false
    end
  end

  describe "timer_ticked/2" do
    test "view change timeout triggers new view change with higher view" do
      expect(MockInterface, :timer, fn :view_change -> &mock_cancel/0 end)
      expect(MockInterface, :send_event, 3, fn _, _ -> :ok end)

      log = InMemoryLog.new()
      mode = ViewChange.new(1, 0, 0, 0, log, ClientTable.new(), [:a, :b, :c], 1, 2, MockInterface)

      assert :start_view_change = ViewChange.timer_ticked(mode, :view_change)
    end

    test "ignores unknown timer types" do
      expect(MockInterface, :timer, fn :view_change -> &mock_cancel/0 end)
      expect(MockInterface, :send_event, 3, fn _, _ -> :ok end)

      log = InMemoryLog.new()
      mode = ViewChange.new(1, 0, 0, 0, log, ClientTable.new(), [:a, :b, :c], 1, 2, MockInterface)

      {:ok, same_mode} = ViewChange.timer_ticked(mode, :unknown)
      assert same_mode == mode
    end
  end

  describe "start_view_change_received/3" do
    setup do
      expect(MockInterface, :timer, fn :view_change -> &mock_cancel/0 end)
      expect(MockInterface, :send_event, 3, fn _, _ -> :ok end)

      log = InMemoryLog.new()
      # Replica :c (index 2) - not the new primary for view 1
      mode = ViewChange.new(1, 0, 0, 0, log, ClientTable.new(), [:a, :b, :c], 2, 2, MockInterface)
      {:ok, mode: mode}
    end

    test "ignores old view messages", %{mode: mode} do
      {:ok, same_mode} = ViewChange.start_view_change_received(mode, 0, :a)
      assert same_mode.start_view_change_from == MapSet.new()
    end

    test "triggers new view change for higher view", %{mode: mode} do
      result = ViewChange.start_view_change_received(mode, 2, :a)
      assert {:start_view_change, 2} = result
    end

    test "records STARTVIEWCHANGE from current view", %{mode: mode} do
      # With quorum=2, receiving one STARTVIEWCHANGE (quorum-1=1) triggers DOVIEWCHANGE
      # Per paper Section 4.2 Step 2: DOVIEWCHANGE(v, l, v', n, k, i) where i is sender
      # Sender identity is provided by transport layer, not the message tuple
      expect(MockInterface, :send_event, fn :b,
                                            {:do_view_change, 1, {:incremental, []}, 0, 0, 0} ->
        :ok
      end)

      {:ok, updated_mode} = ViewChange.start_view_change_received(mode, 1, :a)
      assert MapSet.member?(updated_mode.start_view_change_from, :a)
    end

    test "ignores STARTVIEWCHANGE from self (paper requires f from OTHER replicas)", %{mode: mode} do
      # Per paper Section 4.2 Step 2: "from f other replicas"
      # Self-messages must not be counted towards STARTVIEWCHANGE quorum

      # :c is this replica (index 2), quorum=2
      # If self is counted, receiving our own STARTVIEWCHANGE would reach quorum-1=1
      # and trigger DOVIEWCHANGE, which is incorrect

      # Should NOT trigger DOVIEWCHANGE when receiving from self
      {:ok, updated_mode} = ViewChange.start_view_change_received(mode, 1, :c)
      assert updated_mode.sent_do_view_change == false
      assert MapSet.size(updated_mode.start_view_change_from) == 0
    end

    test "sends DOVIEWCHANGE when quorum-1 STARTVIEWCHANGE received", %{mode: mode} do
      # Need quorum-1 = 2-1 = 1 STARTVIEWCHANGE to send DOVIEWCHANGE
      # Primary is :b for view 1
      # Per paper Section 4.2 Step 2: DOVIEWCHANGE(v, l, v', n, k, i) where i is sender
      expect(MockInterface, :send_event, fn :b,
                                            {:do_view_change, 1, {:incremental, []}, 0, 0, 0} ->
        :ok
      end)

      {:ok, updated_mode} = ViewChange.start_view_change_received(mode, 1, :a)
      assert updated_mode.sent_do_view_change == true
    end

    test "does not send DOVIEWCHANGE twice", %{mode: mode} do
      expect(MockInterface, :send_event, fn :b,
                                            {:do_view_change, 1, {:incremental, []}, 0, 0, 0} ->
        :ok
      end)

      {:ok, mode} = ViewChange.start_view_change_received(mode, 1, :a)

      # Second STARTVIEWCHANGE should not trigger another DOVIEWCHANGE
      {:ok, mode} = ViewChange.start_view_change_received(mode, 1, :other)
      assert mode.sent_do_view_change == true
    end
  end

  describe "do_view_change_received/7" do
    setup do
      expect(MockInterface, :timer, fn :view_change -> &mock_cancel/0 end)
      expect(MockInterface, :send_event, 3, fn _, _ -> :ok end)

      log = InMemoryLog.new()
      # Replica :b (index 1) is the new primary for view 1
      mode = ViewChange.new(1, 0, 0, 0, log, ClientTable.new(), [:a, :b, :c], 1, 2, MockInterface)
      {:ok, mode: mode}
    end

    test "ignores DOVIEWCHANGE for wrong view", %{mode: mode} do
      {:ok, same_mode} = ViewChange.do_view_change_received(mode, 2, [], 0, 0, 0, :a)
      assert same_mode.do_view_change_messages == %{}
    end

    test "ignores DOVIEWCHANGE if not new primary" do
      expect(MockInterface, :timer, fn :view_change -> &mock_cancel/0 end)
      expect(MockInterface, :send_event, 3, fn _, _ -> :ok end)

      log = InMemoryLog.new()
      # :c is not primary for view 1
      mode = ViewChange.new(1, 0, 0, 0, log, ClientTable.new(), [:a, :b, :c], 2, 2, MockInterface)

      {:ok, same_mode} = ViewChange.do_view_change_received(mode, 1, [], 0, 0, 0, :a)
      assert same_mode.do_view_change_messages == %{}
    end

    test "records valid DOVIEWCHANGE message", %{mode: mode} do
      log_entries = [{1, {:client, 1, :op1}}]

      {:ok, updated_mode} =
        ViewChange.do_view_change_received(mode, 1, log_entries, 0, 1, 0, :a)

      assert map_size(updated_mode.do_view_change_messages) == 1
    end

    test "filters duplicate DOVIEWCHANGE from same replica", %{mode: mode} do
      {:ok, mode} = ViewChange.do_view_change_received(mode, 1, [], 0, 0, 0, :a)
      {:ok, mode} = ViewChange.do_view_change_received(mode, 1, [], 0, 0, 0, :a)

      assert map_size(mode.do_view_change_messages) == 1
    end

    test "becomes primary when quorum DOVIEWCHANGE received", %{mode: mode} do
      log_entries = [{1, {:client, 1, :op1}}]
      state_data = {:incremental, log_entries}

      # First DOVIEWCHANGE
      {:ok, mode} = ViewChange.do_view_change_received(mode, 1, log_entries, 0, 1, 0, :a)

      # Per paper Section 4.2 Step 3: STARTVIEW sent to "other replicas" (not self)
      # :b (replica_index 1) is the new primary, so only :a and :c receive STARTVIEW
      expect(MockInterface, :send_event, fn :a, {:start_view, 1, ^state_data, 1, 0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:start_view, 1, ^state_data, 1, 0} -> :ok end)

      # Second DOVIEWCHANGE - reaches quorum
      result = ViewChange.do_view_change_received(mode, 1, log_entries, 0, 1, 0, :c)

      assert {:become_primary, 1, 1, 0, _new_store, _client_table} = result
    end

    test "selects log with highest last_normal_view", %{mode: mode} do
      old_log = [{1, {:client, 1, :old_op}}]
      new_log = [{1, {:client, 1, :new_op}}]
      new_state_data = {:incremental, new_log}

      {:ok, mode} = ViewChange.do_view_change_received(mode, 1, old_log, 0, 1, 0, :a)

      # STARTVIEW to other replicas only (not :b which is self)
      expect(MockInterface, :send_event, 2, fn _, {:start_view, 1, ^new_state_data, 1, 0} ->
        :ok
      end)

      # Higher last_normal_view wins
      result = ViewChange.do_view_change_received(mode, 1, new_log, 1, 1, 0, :c)

      assert {:become_primary, 1, 1, 0, _, _client_table} = result
    end

    test "uses max commit_number from all messages", %{mode: mode} do
      log_entries = [{1, {:client, 1, :op1}}, {2, {:client, 1, :op2}}]
      state_data = {:incremental, log_entries}

      {:ok, mode} = ViewChange.do_view_change_received(mode, 1, log_entries, 0, 2, 1, :a)

      # STARTVIEW to other replicas only (not :b which is self)
      expect(MockInterface, :send_event, 2, fn _, {:start_view, 1, ^state_data, 2, 2} -> :ok end)

      # Higher commit_num
      result = ViewChange.do_view_change_received(mode, 1, log_entries, 0, 2, 2, :c)

      assert {:become_primary, 1, 2, 2, _, _client_table} = result
    end
  end

  describe "start_view_received/5" do
    setup do
      expect(MockInterface, :timer, fn :view_change -> &mock_cancel/0 end)
      expect(MockInterface, :send_event, 3, fn _, _ -> :ok end)

      log = InMemoryLog.new()
      # :c is backup for view 1
      mode = ViewChange.new(1, 0, 0, 0, log, ClientTable.new(), [:a, :b, :c], 2, 2, MockInterface)
      {:ok, mode: mode}
    end

    test "ignores old view STARTVIEW", %{mode: mode} do
      {:ok, same_mode} = ViewChange.start_view_received(mode, 0, [], 0, 0)
      assert same_mode == mode
    end

    test "becomes normal on valid STARTVIEW", %{mode: mode} do
      log_entries = [{1, {:client, 1, :op1}}]

      # Backup :c should send PREPAREOK for uncommitted op 1 to primary :b
      # Per paper Section 4.2 Step 5: PREPAREOK(v, n, i) where i is sender
      expect(MockInterface, :send_event, fn :b, {:prepare_ok, 1, 1} -> :ok end)

      result = ViewChange.start_view_received(mode, 1, log_entries, 1, 0)

      assert {:become_normal, 1, 1, 0, _new_log, _client_table} = result
    end

    test "sends PREPAREOK for each uncommitted operation", %{mode: mode} do
      log_entries = [{1, {:client, 1, :op1}}, {2, {:client, 1, :op2}}, {3, {:client, 1, :op3}}]

      # commit_num = 1, op_num = 3, so ops 2 and 3 are uncommitted
      # Per paper Section 4.2 Step 5: PREPAREOK(v, n, i) where i is sender
      expect(MockInterface, :send_event, fn :b, {:prepare_ok, 1, 2} -> :ok end)
      expect(MockInterface, :send_event, fn :b, {:prepare_ok, 1, 3} -> :ok end)

      result = ViewChange.start_view_received(mode, 1, log_entries, 3, 1)

      assert {:become_normal, 1, 3, 1, _, _client_table} = result
    end

    test "does not send PREPAREOK when all ops committed", %{mode: mode} do
      log_entries = [{1, {:client, 1, :op1}}]

      # All committed (commit_num == op_num), no PREPAREOKs sent
      result = ViewChange.start_view_received(mode, 1, log_entries, 1, 1)

      assert {:become_normal, 1, 1, 1, _, _client_table} = result
    end

    test "new primary does not send PREPAREOK to itself" do
      expect(MockInterface, :timer, fn :view_change -> &mock_cancel/0 end)
      expect(MockInterface, :send_event, 3, fn _, _ -> :ok end)

      log = InMemoryLog.new()
      # :b is primary for view 1
      mode = ViewChange.new(1, 0, 0, 0, log, ClientTable.new(), [:a, :b, :c], 1, 2, MockInterface)

      log_entries = [{1, {:client, 1, :op1}}]

      # No PREPAREOK should be sent since :b is the primary
      result = ViewChange.start_view_received(mode, 1, log_entries, 1, 0)

      assert {:become_normal, 1, 1, 0, _, _client_table} = result
    end
  end

  describe "ignored messages during view change" do
    setup do
      expect(MockInterface, :timer, fn :view_change -> &mock_cancel/0 end)
      expect(MockInterface, :send_event, 3, fn _, _ -> :ok end)

      log = InMemoryLog.new()
      mode = ViewChange.new(1, 0, 0, 0, log, ClientTable.new(), [:a, :b, :c], 1, 2, MockInterface)
      {:ok, mode: mode}
    end

    test "request_received returns not_primary", %{mode: mode} do
      assert {:error, :not_primary} = ViewChange.request_received(mode, :client, 1, :op)
    end

    test "prepare_received is ignored", %{mode: mode} do
      {:ok, same_mode} = ViewChange.prepare_received(mode, 0, :msg, 1, 0, :a)
      assert same_mode == mode
    end

    test "prepare_ok_received is ignored", %{mode: mode} do
      {:ok, same_mode} = ViewChange.prepare_ok_received(mode, 0, 1, :a)
      assert same_mode == mode
    end

    test "commit_received is ignored", %{mode: mode} do
      {:ok, same_mode} = ViewChange.commit_received(mode, 0, 1)
      assert same_mode == mode
    end

    test "recovery_received is ignored", %{mode: mode} do
      {:ok, same_mode} = ViewChange.recovery_received(mode, :nonce, :a)
      assert same_mode == mode
    end

    test "recovery_response_received is ignored", %{mode: mode} do
      {:ok, same_mode} =
        ViewChange.recovery_response_received(mode, 0, :nonce, [], 0, 0, false, :a)

      assert same_mode == mode
    end
  end
end
