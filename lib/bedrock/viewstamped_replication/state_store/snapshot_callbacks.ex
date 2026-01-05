defmodule Bedrock.ViewstampedReplication.StateStore.SnapshotCallbacks do
  @moduledoc """
  Behaviour for snapshot-based state management callbacks.

  Implement this behaviour to use `SnapshotStore` for VR state storage.

  ## Example

      defmodule MyApp.SnapshotHandler do
        @behaviour Bedrock.ViewstampedReplication.StateStore.SnapshotCallbacks

        @impl true
        def get_snapshot do
          {:ok, MyApp.StateMachine.get_state()}
        end

        @impl true
        def apply_snapshot(snapshot) do
          MyApp.StateMachine.set_state(snapshot)
          :ok
        end

        @impl true
        def persist_view_number(view_number) do
          :ok = MyApp.Persistence.save(:view_number, view_number)
        end

        @impl true
        def get_persisted_view_number do
          MyApp.Persistence.get(:view_number, 0)
        end
      end
  """

  @doc """
  Get the current application state snapshot.
  """
  @callback get_snapshot() :: {:ok, snapshot :: term()}

  @doc """
  Apply a received snapshot to restore application state.
  """
  @callback apply_snapshot(snapshot :: term()) :: :ok

  @doc """
  Persist the view number for crash recovery.
  """
  @callback persist_view_number(view_number :: non_neg_integer()) :: :ok

  @doc """
  Get the persisted view number (0 if none).
  """
  @callback get_persisted_view_number() :: non_neg_integer()
end
