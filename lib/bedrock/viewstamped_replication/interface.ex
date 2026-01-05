defmodule Bedrock.ViewstampedReplication.Interface do
  @moduledoc """
  Behavior defining the external integration interface for Viewstamped Replication.

  Implementations of this behaviour provide functions that the VR consensus
  system needs to communicate, set timers, and handle events. This allows
  the system to be used in a variety of contexts and gives the user full
  control over how messages are sent and received.
  """

  alias Bedrock.ViewstampedReplication, as: VR

  defmacro __using__(_opts) do
    quote do
      @behaviour Bedrock.ViewstampedReplication.Interface
    end
  end

  @type cancel_timer_fn :: (-> :ok)

  @doc """
  Returns the heartbeat interval in milliseconds.
  The primary sends COMMIT messages at this interval when idle.
  """
  @callback heartbeat_ms() :: non_neg_integer()

  @doc """
  Returns the view change timeout in milliseconds.
  Backups trigger view change if no messages received within this time.
  """
  @callback view_change_timeout_ms() :: non_neg_integer()

  @doc """
  Returns the current timestamp in milliseconds.
  """
  @callback timestamp_in_ms() :: non_neg_integer()

  @doc """
  Set a timer for the VR protocol. Returns a function to cancel the timer.
  Timer types:
  - :heartbeat - Primary sends periodic COMMIT messages
  - :view_change - Backup triggers view change on timeout
  - :recovery - Recovery timeout
  """
  @callback timer(:heartbeat | :view_change | :recovery) :: cancel_timer_fn()

  @doc """
  Send an event to another replica.
  """
  @callback send_event(to :: VR.replica_id(), event :: any()) :: :ok

  @doc """
  Send a reply to a client.
  """
  @callback send_reply(
              client_id :: VR.client_id(),
              view_number :: VR.view_number(),
              request_number :: VR.request_number(),
              result :: term()
            ) :: :ok

  @doc """
  Execute a client operation and return the result.
  This is called when an operation is committed.
  """
  @callback execute_operation(operation :: term()) :: {:ok, result :: term()}

  @doc """
  Called when an operation has been committed.
  """
  @callback operation_committed(
              store :: VR.StateStore.t(),
              op_number :: VR.op_number(),
              operation :: term(),
              result :: term()
            ) :: :ok

  @doc """
  Called when the primary changes.
  """
  @callback primary_changed(VR.primary_info()) :: :ok

  @doc """
  Called when an unhandled event is received.
  """
  @callback ignored_event(event :: any(), from :: VR.replica_id() | :timer) :: :ok
end
