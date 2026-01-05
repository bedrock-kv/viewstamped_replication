defmodule Bedrock.ViewstampedReplication.Mode.Normal.BackupTracking do
  @moduledoc """
  Tracks PREPAREOK acknowledgments from backup replicas.

  The primary uses this to determine when a quorum of replicas
  have acknowledged an operation, allowing it to be committed.
  """

  alias Bedrock.ViewstampedReplication, as: VR

  @type t :: %__MODULE__{
          # Map from replica_id to their highest acknowledged op_number
          acks: %{VR.replica_id() => VR.op_number()}
        }

  defstruct acks: %{}

  @doc """
  Create a new BackupTracking instance for the given replicas.
  All replicas start with op_number 0 (no acknowledgments yet).
  """
  @spec new([VR.replica_id()]) :: t()
  def new(replicas) do
    acks = Map.new(replicas, fn replica -> {replica, 0} end)
    %__MODULE__{acks: acks}
  end

  @doc """
  Record an acknowledgment from a replica for the given op_number.
  Only updates if the new op_number is higher than previously recorded.
  """
  @spec record_ack(t(), VR.replica_id(), VR.op_number()) :: t()
  def record_ack(%__MODULE__{acks: acks} = tracking, replica, op_number) do
    current = Map.get(acks, replica, 0)

    if op_number > current do
      %{tracking | acks: Map.put(acks, replica, op_number)}
    else
      tracking
    end
  end

  @doc """
  Count how many replicas have acknowledged at least the given op_number.
  """
  @spec count_acks_for(t(), VR.op_number()) :: non_neg_integer()
  def count_acks_for(%__MODULE__{acks: acks}, op_number),
    do: Enum.count(acks, fn {_replica, acked_op} -> acked_op >= op_number end)

  @doc """
  Get the highest op_number that at least `count` replicas have acknowledged.
  """
  @spec highest_acked_by_count(t(), non_neg_integer()) :: VR.op_number()
  def highest_acked_by_count(%__MODULE__{acks: acks}, count) when map_size(acks) < count, do: 0

  def highest_acked_by_count(%__MODULE__{acks: acks}, count) do
    acks
    |> Map.values()
    |> Enum.sort(:desc)
    |> Enum.at(count - 1, 0)
  end

  @doc """
  Get the acknowledgment status for a specific replica.
  """
  @spec get_ack(t(), VR.replica_id()) :: VR.op_number()
  def get_ack(%__MODULE__{acks: acks}, replica), do: Map.get(acks, replica, 0)
end
