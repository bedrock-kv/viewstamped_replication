defmodule Bedrock.ViewstampedReplication.Telemetry do
  @moduledoc """
  Telemetry events for Viewstamped Replication protocol observability.
  """

  alias Bedrock.ViewstampedReplication, as: VR

  @spec track_ignored_event(event :: term(), from :: VR.replica_id() | :timer) :: :ok
  def track_ignored_event(event, from) do
    :telemetry.execute([:bedrock, :vr, :ignored_event], %{at: now()}, %{
      event: event,
      from: from
    })
  end

  @spec track_became_normal(VR.view_number(), is_primary :: boolean(), VR.replica_id()) :: :ok
  def track_became_normal(view_number, is_primary, replica) do
    :telemetry.execute([:bedrock, :vr, :mode_change], %{at: now()}, %{
      mode: :normal,
      role: if(is_primary, do: :primary, else: :backup),
      view_number: view_number,
      replica: replica
    })
  end

  @spec track_started_view_change(VR.view_number(), VR.replica_id()) :: :ok
  def track_started_view_change(view_number, replica) do
    :telemetry.execute([:bedrock, :vr, :mode_change], %{at: now()}, %{
      mode: :view_change,
      view_number: view_number,
      replica: replica
    })
  end

  @spec track_started_recovering(VR.replica_id()) :: :ok
  def track_started_recovering(replica) do
    :telemetry.execute([:bedrock, :vr, :mode_change], %{at: now()}, %{
      mode: :recovering,
      replica: replica
    })
  end

  @spec track_primary_change(VR.replica_id(), VR.view_number()) :: :ok
  def track_primary_change(primary, view_number) do
    :telemetry.execute([:bedrock, :vr, :primary_change], %{at: now()}, %{
      primary: primary,
      view_number: view_number
    })
  end

  @spec track_prepare_sent(VR.view_number(), VR.op_number(), VR.commit_number()) :: :ok
  def track_prepare_sent(view_number, op_number, commit_number) do
    :telemetry.execute([:bedrock, :vr, :prepare_sent], %{at: now()}, %{
      view_number: view_number,
      op_number: op_number,
      commit_number: commit_number
    })
  end

  @spec track_prepare_ok_sent(VR.view_number(), VR.op_number(), VR.replica_id()) :: :ok
  def track_prepare_ok_sent(view_number, op_number, replica) do
    :telemetry.execute([:bedrock, :vr, :prepare_ok_sent], %{at: now()}, %{
      view_number: view_number,
      op_number: op_number,
      replica: replica
    })
  end

  @spec track_prepare_ok_received(VR.view_number(), VR.op_number(), VR.replica_id()) :: :ok
  def track_prepare_ok_received(view_number, op_number, from) do
    :telemetry.execute([:bedrock, :vr, :prepare_ok_received], %{at: now()}, %{
      view_number: view_number,
      op_number: op_number,
      from: from
    })
  end

  @spec track_commit_sent(VR.view_number(), VR.commit_number()) :: :ok
  def track_commit_sent(view_number, commit_number) do
    :telemetry.execute([:bedrock, :vr, :commit_sent], %{at: now()}, %{
      view_number: view_number,
      commit_number: commit_number
    })
  end

  @spec track_operation_committed(VR.op_number(), term()) :: :ok
  def track_operation_committed(op_number, operation) do
    :telemetry.execute([:bedrock, :vr, :operation_committed], %{at: now()}, %{
      op_number: op_number,
      operation: operation
    })
  end

  @spec track_start_view_change_sent(VR.view_number(), VR.replica_id()) :: :ok
  def track_start_view_change_sent(view_number, replica) do
    :telemetry.execute([:bedrock, :vr, :start_view_change_sent], %{at: now()}, %{
      view_number: view_number,
      replica: replica
    })
  end

  @spec track_do_view_change_sent(VR.view_number(), VR.replica_id()) :: :ok
  def track_do_view_change_sent(view_number, to) do
    :telemetry.execute([:bedrock, :vr, :do_view_change_sent], %{at: now()}, %{
      view_number: view_number,
      to: to
    })
  end

  @spec track_start_view_sent(VR.view_number(), VR.op_number(), VR.commit_number()) :: :ok
  def track_start_view_sent(view_number, op_number, commit_number) do
    :telemetry.execute([:bedrock, :vr, :start_view_sent], %{at: now()}, %{
      view_number: view_number,
      op_number: op_number,
      commit_number: commit_number
    })
  end

  @spec track_recovery_sent(nonce :: term(), VR.replica_id()) :: :ok
  def track_recovery_sent(nonce, replica) do
    :telemetry.execute([:bedrock, :vr, :recovery_sent], %{at: now()}, %{
      nonce: nonce,
      replica: replica
    })
  end

  @spec track_recovery_response_received(VR.view_number(), VR.replica_id()) :: :ok
  def track_recovery_response_received(view_number, from) do
    :telemetry.execute([:bedrock, :vr, :recovery_response_received], %{at: now()}, %{
      view_number: view_number,
      from: from
    })
  end

  defp now, do: :os.system_time(:millisecond)
end
