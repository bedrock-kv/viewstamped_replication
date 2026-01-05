defmodule Bedrock.ViewstampedReplication.Mode.RecoveringTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Bedrock.ViewstampedReplication.Mode.Recovering
  alias Bedrock.ViewstampedReplication.StateStore.InMemoryLog

  import Mox

  alias Bedrock.ViewstampedReplication.MockInterface

  setup :verify_on_exit!

  def mock_cancel, do: :ok

  describe "new/6" do
    test "creates recovering mode and sends RECOVERY to all other replicas" do
      expect(MockInterface, :timer, fn :recovery -> &mock_cancel/0 end)
      # Per paper Section 4.3 Step 1: RECOVERY(i, x) - sender provided by transport
      expect(MockInterface, :send_event, fn :a, {:recovery, :nonce123} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:recovery, :nonce123} -> :ok end)

      log = InMemoryLog.new()
      mode = Recovering.new(:nonce123, log, [:a, :b, :c], 1, 2, MockInterface)

      assert %Recovering{
               nonce: :nonce123,
               configuration: [:a, :b, :c],
               replica_index: 1,
               quorum: 2,
               responses: %{}
             } = mode
    end

    test "does not send RECOVERY to itself" do
      expect(MockInterface, :timer, fn :recovery -> &mock_cancel/0 end)
      # Only sends to :b and :c, not :a (itself)
      expect(MockInterface, :send_event, fn :b, {:recovery, :nonce} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:recovery, :nonce} -> :ok end)

      log = InMemoryLog.new()
      _mode = Recovering.new(:nonce, log, [:a, :b, :c], 0, 2, MockInterface)
    end
  end

  describe "timer_ticked/2" do
    test "recovery timeout retries by sending RECOVERY again" do
      expect(MockInterface, :timer, fn :recovery -> &mock_cancel/0 end)
      expect(MockInterface, :send_event, 2, fn _, {:recovery, :nonce} -> :ok end)

      log = InMemoryLog.new()
      mode = Recovering.new(:nonce, log, [:a, :b, :c], 1, 2, MockInterface)

      # Expect retry sends and new timer
      expect(MockInterface, :send_event, fn :a, {:recovery, :nonce} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:recovery, :nonce} -> :ok end)
      expect(MockInterface, :timer, fn :recovery -> &mock_cancel/0 end)

      {:ok, updated_mode} = Recovering.timer_ticked(mode, :recovery)
      assert updated_mode.cancel_timer_fn != nil
    end

    test "ignores unknown timer types" do
      expect(MockInterface, :timer, fn :recovery -> &mock_cancel/0 end)
      expect(MockInterface, :send_event, 2, fn _, _ -> :ok end)

      log = InMemoryLog.new()
      mode = Recovering.new(:nonce, log, [:a, :b, :c], 1, 2, MockInterface)

      {:ok, same_mode} = Recovering.timer_ticked(mode, :unknown)
      assert same_mode == mode
    end
  end

  describe "recovery_response_received/8" do
    setup do
      expect(MockInterface, :timer, fn :recovery -> &mock_cancel/0 end)
      expect(MockInterface, :send_event, 2, fn _, _ -> :ok end)

      log = InMemoryLog.new()
      mode = Recovering.new(:nonce, log, [:a, :b, :c], 1, 2, MockInterface)
      {:ok, mode: mode}
    end

    test "ignores responses with wrong nonce", %{mode: mode} do
      {:ok, same_mode} =
        Recovering.recovery_response_received(
          mode,
          0,
          :wrong_nonce,
          [],
          0,
          0,
          true,
          :a
        )

      assert same_mode.responses == %{}
    end

    test "records valid response", %{mode: mode} do
      {:ok, updated_mode} =
        Recovering.recovery_response_received(
          mode,
          0,
          :nonce,
          [],
          0,
          0,
          false,
          :a
        )

      assert map_size(updated_mode.responses) == 1
      assert updated_mode.responses.a.from == :a
    end

    test "filters duplicate responses from same replica", %{mode: mode} do
      {:ok, mode} =
        Recovering.recovery_response_received(mode, 0, :nonce, [], 0, 0, false, :a)

      {:ok, mode} =
        Recovering.recovery_response_received(mode, 0, :nonce, [], 0, 0, false, :a)

      assert map_size(mode.responses) == 1
    end

    test "becomes normal when quorum reached with primary response", %{mode: mode} do
      log_entries = [{1, {:client1, 1, :op1}}]

      # First response from non-primary
      {:ok, mode} =
        Recovering.recovery_response_received(mode, 5, :nonce, [], 0, 0, false, :a)

      # Second response from primary with log
      result =
        Recovering.recovery_response_received(mode, 5, :nonce, log_entries, 1, 1, true, :c)

      assert {:become_normal, 5, 1, 1, _new_log} = result
    end

    test "does not become normal without primary response", %{mode: mode} do
      # Two non-primary responses (quorum met but no primary)
      {:ok, mode} =
        Recovering.recovery_response_received(mode, 0, :nonce, [], 0, 0, false, :a)

      {:ok, mode} =
        Recovering.recovery_response_received(mode, 0, :nonce, [], 0, 0, false, :c)

      # Still in recovery mode despite quorum
      assert map_size(mode.responses) == 2
    end

    test "does not become normal without quorum", %{mode: mode} do
      # Only one response (primary but not quorum)
      {:ok, mode} =
        Recovering.recovery_response_received(mode, 0, :nonce, [], 0, 0, true, :a)

      assert map_size(mode.responses) == 1
    end
  end

  describe "ignored messages during recovery" do
    setup do
      expect(MockInterface, :timer, fn :recovery -> &mock_cancel/0 end)
      expect(MockInterface, :send_event, 2, fn _, _ -> :ok end)

      log = InMemoryLog.new()
      mode = Recovering.new(:nonce, log, [:a, :b, :c], 1, 2, MockInterface)
      {:ok, mode: mode}
    end

    test "request_received returns not_primary", %{mode: mode} do
      assert {:error, :not_primary} = Recovering.request_received(mode, :client, 1, :op)
    end

    test "prepare_received is ignored", %{mode: mode} do
      {:ok, same_mode} = Recovering.prepare_received(mode, 0, :msg, 1, 0, :a)
      assert same_mode == mode
    end

    test "prepare_ok_received is ignored", %{mode: mode} do
      {:ok, same_mode} = Recovering.prepare_ok_received(mode, 0, 1, :a)
      assert same_mode == mode
    end

    test "commit_received is ignored", %{mode: mode} do
      {:ok, same_mode} = Recovering.commit_received(mode, 0, 1)
      assert same_mode == mode
    end

    test "start_view_change_received is ignored", %{mode: mode} do
      {:ok, same_mode} = Recovering.start_view_change_received(mode, 1, :a)
      assert same_mode == mode
    end

    test "do_view_change_received is ignored", %{mode: mode} do
      {:ok, same_mode} = Recovering.do_view_change_received(mode, 1, [], 0, 0, 0, :a)
      assert same_mode == mode
    end

    test "start_view_received is ignored", %{mode: mode} do
      {:ok, same_mode} = Recovering.start_view_received(mode, 1, [], 0, 0)
      assert same_mode == mode
    end

    test "recovery_received is ignored (can't help while recovering)", %{mode: mode} do
      {:ok, same_mode} = Recovering.recovery_received(mode, :other_nonce, :a)
      assert same_mode == mode
    end
  end

  describe "view_number tracking during recovery (viewstamped_replication-6hf)" do
    setup do
      expect(MockInterface, :timer, fn :recovery -> &mock_cancel/0 end)
      expect(MockInterface, :send_event, 2, fn _, _ -> :ok end)

      log = InMemoryLog.new()
      mode = Recovering.new(:nonce, log, [:a, :b, :c], 1, 2, MockInterface)
      {:ok, mode: mode}
    end

    @tag :paper_compliance
    test "mode.view_number starts at 0", %{mode: mode} do
      assert mode.view_number == 0
    end

    @tag :paper_compliance
    test "mode.view_number tracks highest view from responses (viewstamped_replication-6hf)", %{
      mode: mode
    } do
      # Per paper Section 4.3 Step 3:
      # "including one from the primary of the **latest view** it learns of in these messages"
      # The mode should track the highest view_number seen from responses

      # First response from view 5
      {:ok, mode} =
        Recovering.recovery_response_received(mode, 5, :nonce, [], 0, 0, false, :a)

      assert mode.view_number == 5

      # Second response from view 3 (older) - should not decrease
      {:ok, mode} =
        Recovering.recovery_response_received(mode, 3, :nonce, [], 0, 0, false, :c)

      assert mode.view_number == 5
    end

    @tag :paper_compliance
    test "mode.view_number increases with higher view response", %{mode: mode} do
      {:ok, mode} =
        Recovering.recovery_response_received(mode, 5, :nonce, [], 0, 0, false, :a)

      assert mode.view_number == 5

      # Higher view response from same cluster (no transition yet - quorum not met)
      {:ok, mode} =
        Recovering.recovery_response_received(mode, 8, :nonce, [], 0, 0, false, :c)

      assert mode.view_number == 8
    end
  end

  describe "recovery uses primary from latest view (viewstamped_replication-loe)" do
    setup do
      expect(MockInterface, :timer, fn :recovery -> &mock_cancel/0 end)
      expect(MockInterface, :send_event, 2, fn _, _ -> :ok end)

      log = InMemoryLog.new()
      mode = Recovering.new(:nonce, log, [:a, :b, :c], 1, 2, MockInterface)
      {:ok, mode: mode}
    end

    test "uses primary from highest view, not first primary seen", %{mode: mode} do
      # First response: old primary from view 5 with stale data
      {:ok, mode} =
        Recovering.recovery_response_received(
          mode,
          # view_num - old view
          5,
          :nonce,
          # stale log
          [{1, {:c1, 1, :old_op}}],
          # stale op_number
          1,
          # stale commit_number
          1,
          # is_primary (was primary in view 5)
          true,
          :a
        )

      # Second response: new primary from view 6 with current data
      result =
        Recovering.recovery_response_received(
          mode,
          # view_num - newer view!
          6,
          :nonce,
          # current log
          [{1, {:c1, 1, :new_op}}, {2, {:c1, 2, :new_op2}}],
          # current op_number
          2,
          # current commit_number
          2,
          # is_primary (current primary in view 6)
          true,
          :c
        )

      # Should recover using view 6's primary data, NOT view 5's stale data
      assert {:become_normal, 6, 2, 2, _new_log} = result
    end

    test "waits for primary from latest view even if quorum met", %{mode: mode} do
      # Response from old primary in view 5
      {:ok, mode} =
        Recovering.recovery_response_received(
          mode,
          5,
          :nonce,
          [{1, {:c1, 1, :op}}],
          1,
          1,
          # was primary in view 5
          true,
          :a
        )

      # Response from backup in view 6 (not primary)
      {:ok, mode} =
        Recovering.recovery_response_received(
          mode,
          6,
          :nonce,
          # backup sends empty log per paper
          [],
          0,
          0,
          # not primary
          false,
          :c
        )

      # Should NOT recover yet - have quorum (2) and have a primary (from view 5),
      # but don't have primary from latest view (6)
      # The old primary from view 5 is stale!
      assert map_size(mode.responses) == 2
    end

    test "recovers when primary from latest view responds", %{mode: mode} do
      # Response from backup in view 6
      {:ok, mode} =
        Recovering.recovery_response_received(
          mode,
          6,
          :nonce,
          [],
          0,
          0,
          false,
          :a
        )

      # Response from primary in view 6
      result =
        Recovering.recovery_response_received(
          mode,
          6,
          :nonce,
          [{1, {:c1, 1, :op}}],
          1,
          1,
          true,
          :c
        )

      # Now should recover - have quorum and primary from latest view (6)
      assert {:become_normal, 6, 1, 1, _} = result
    end
  end
end
