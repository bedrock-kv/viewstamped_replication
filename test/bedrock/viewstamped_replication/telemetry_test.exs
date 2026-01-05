defmodule Bedrock.ViewstampedReplication.TelemetryTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Bedrock.ViewstampedReplication.Telemetry

  # Module function to avoid telemetry warning about anonymous handlers
  def handle_event(event_name, measurements, metadata, test_pid) do
    send(test_pid, {:telemetry_event, event_name, measurements, metadata})
  end

  setup do
    events = [
      [:bedrock, :vr, :ignored_event],
      [:bedrock, :vr, :mode_change],
      [:bedrock, :vr, :primary_change],
      [:bedrock, :vr, :prepare_sent],
      [:bedrock, :vr, :prepare_ok_sent],
      [:bedrock, :vr, :prepare_ok_received],
      [:bedrock, :vr, :commit_sent],
      [:bedrock, :vr, :operation_committed],
      [:bedrock, :vr, :start_view_change_sent],
      [:bedrock, :vr, :do_view_change_sent],
      [:bedrock, :vr, :start_view_sent],
      [:bedrock, :vr, :recovery_sent],
      [:bedrock, :vr, :recovery_response_received]
    ]

    handler_id = make_ref()
    :telemetry.attach_many(handler_id, events, &__MODULE__.handle_event/4, self())

    on_exit(fn ->
      :telemetry.detach(handler_id)
    end)

    :ok
  end

  describe "track_ignored_event/2" do
    test "emits ignored_event telemetry with event and from metadata" do
      Telemetry.track_ignored_event(:some_event, :replica_1)

      assert_receive {:telemetry_event, [:bedrock, :vr, :ignored_event], measurements, metadata}
      assert is_integer(measurements.at)
      assert metadata.event == :some_event
      assert metadata.from == :replica_1
    end

    test "accepts :timer as from" do
      Telemetry.track_ignored_event(:timeout, :timer)

      assert_receive {:telemetry_event, [:bedrock, :vr, :ignored_event], _measurements, metadata}
      assert metadata.from == :timer
    end
  end

  describe "track_became_normal/3" do
    test "emits mode_change telemetry for primary" do
      Telemetry.track_became_normal(5, true, :replica_1)

      assert_receive {:telemetry_event, [:bedrock, :vr, :mode_change], measurements, metadata}
      assert is_integer(measurements.at)
      assert metadata.mode == :normal
      assert metadata.role == :primary
      assert metadata.view_number == 5
      assert metadata.replica == :replica_1
    end

    test "emits mode_change telemetry for backup" do
      Telemetry.track_became_normal(3, false, :replica_2)

      assert_receive {:telemetry_event, [:bedrock, :vr, :mode_change], _measurements, metadata}
      assert metadata.mode == :normal
      assert metadata.role == :backup
      assert metadata.view_number == 3
      assert metadata.replica == :replica_2
    end
  end

  describe "track_started_view_change/2" do
    test "emits mode_change telemetry for view_change mode" do
      Telemetry.track_started_view_change(7, :replica_3)

      assert_receive {:telemetry_event, [:bedrock, :vr, :mode_change], measurements, metadata}
      assert is_integer(measurements.at)
      assert metadata.mode == :view_change
      assert metadata.view_number == 7
      assert metadata.replica == :replica_3
    end
  end

  describe "track_started_recovering/1" do
    test "emits mode_change telemetry for recovering mode" do
      Telemetry.track_started_recovering(:replica_4)

      assert_receive {:telemetry_event, [:bedrock, :vr, :mode_change], measurements, metadata}
      assert is_integer(measurements.at)
      assert metadata.mode == :recovering
      assert metadata.replica == :replica_4
    end
  end

  describe "track_primary_change/2" do
    test "emits primary_change telemetry" do
      Telemetry.track_primary_change(:new_primary, 10)

      assert_receive {:telemetry_event, [:bedrock, :vr, :primary_change], measurements, metadata}
      assert is_integer(measurements.at)
      assert metadata.primary == :new_primary
      assert metadata.view_number == 10
    end
  end

  describe "track_prepare_sent/3" do
    test "emits prepare_sent telemetry" do
      Telemetry.track_prepare_sent(5, 100, 95)

      assert_receive {:telemetry_event, [:bedrock, :vr, :prepare_sent], measurements, metadata}
      assert is_integer(measurements.at)
      assert metadata.view_number == 5
      assert metadata.op_number == 100
      assert metadata.commit_number == 95
    end
  end

  describe "track_prepare_ok_sent/3" do
    test "emits prepare_ok_sent telemetry" do
      Telemetry.track_prepare_ok_sent(5, 100, :replica_1)

      assert_receive {:telemetry_event, [:bedrock, :vr, :prepare_ok_sent], measurements, metadata}
      assert is_integer(measurements.at)
      assert metadata.view_number == 5
      assert metadata.op_number == 100
      assert metadata.replica == :replica_1
    end
  end

  describe "track_prepare_ok_received/3" do
    test "emits prepare_ok_received telemetry" do
      Telemetry.track_prepare_ok_received(5, 100, :backup_1)

      assert_receive {:telemetry_event, [:bedrock, :vr, :prepare_ok_received], measurements, metadata}
      assert is_integer(measurements.at)
      assert metadata.view_number == 5
      assert metadata.op_number == 100
      assert metadata.from == :backup_1
    end
  end

  describe "track_commit_sent/2" do
    test "emits commit_sent telemetry" do
      Telemetry.track_commit_sent(5, 95)

      assert_receive {:telemetry_event, [:bedrock, :vr, :commit_sent], measurements, metadata}
      assert is_integer(measurements.at)
      assert metadata.view_number == 5
      assert metadata.commit_number == 95
    end
  end

  describe "track_operation_committed/2" do
    test "emits operation_committed telemetry" do
      Telemetry.track_operation_committed(42, {:put, :key, :value})

      assert_receive {:telemetry_event, [:bedrock, :vr, :operation_committed], measurements, metadata}
      assert is_integer(measurements.at)
      assert metadata.op_number == 42
      assert metadata.operation == {:put, :key, :value}
    end
  end

  describe "track_start_view_change_sent/2" do
    test "emits start_view_change_sent telemetry" do
      Telemetry.track_start_view_change_sent(8, :replica_1)

      assert_receive {:telemetry_event, [:bedrock, :vr, :start_view_change_sent], measurements, metadata}
      assert is_integer(measurements.at)
      assert metadata.view_number == 8
      assert metadata.replica == :replica_1
    end
  end

  describe "track_do_view_change_sent/2" do
    test "emits do_view_change_sent telemetry" do
      Telemetry.track_do_view_change_sent(8, :new_primary)

      assert_receive {:telemetry_event, [:bedrock, :vr, :do_view_change_sent], measurements, metadata}
      assert is_integer(measurements.at)
      assert metadata.view_number == 8
      assert metadata.to == :new_primary
    end
  end

  describe "track_start_view_sent/3" do
    test "emits start_view_sent telemetry" do
      Telemetry.track_start_view_sent(8, 50, 45)

      assert_receive {:telemetry_event, [:bedrock, :vr, :start_view_sent], measurements, metadata}
      assert is_integer(measurements.at)
      assert metadata.view_number == 8
      assert metadata.op_number == 50
      assert metadata.commit_number == 45
    end
  end

  describe "track_recovery_sent/2" do
    test "emits recovery_sent telemetry" do
      Telemetry.track_recovery_sent(:nonce_123, :replica_1)

      assert_receive {:telemetry_event, [:bedrock, :vr, :recovery_sent], measurements, metadata}
      assert is_integer(measurements.at)
      assert metadata.nonce == :nonce_123
      assert metadata.replica == :replica_1
    end
  end

  describe "track_recovery_response_received/2" do
    test "emits recovery_response_received telemetry" do
      Telemetry.track_recovery_response_received(10, :replica_2)

      assert_receive {:telemetry_event, [:bedrock, :vr, :recovery_response_received], measurements, metadata}
      assert is_integer(measurements.at)
      assert metadata.view_number == 10
      assert metadata.from == :replica_2
    end
  end
end
