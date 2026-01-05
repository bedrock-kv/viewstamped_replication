defmodule Bedrock.ViewstampedReplication.Mode do
  @moduledoc false

  alias Bedrock.ViewstampedReplication, as: VR
  alias VR.StateStore

  @doc """
  Called when a timer fires.
  """
  @callback timer_ticked(any(), :heartbeat | :view_change | :recovery) ::
              {:ok, any()}
              | :start_view_change
              | {:start_view_change, VR.view_number()}
              | {:become_normal, VR.view_number(), VR.op_number(), VR.commit_number(),
                 StateStore.t()}

  @doc """
  Called when this replica (as primary) receives a client request.
  """
  @callback request_received(any(), VR.client_id(), VR.request_number(), operation :: term()) ::
              {:ok, any(), VR.op_number()}
              | {:ok, any(), {:cached, term()}}
              | {:error, :not_primary}

  @doc """
  Called when a PREPARE message is received from the primary.
  """
  @callback prepare_received(
              any(),
              VR.view_number(),
              message :: term(),
              VR.op_number(),
              VR.commit_number(),
              from :: VR.replica_id()
            ) ::
              {:ok, any()}
              | :start_view_change
              | {:start_view_change, VR.view_number()}

  @doc """
  Called when a PREPAREOK message is received from a backup.
  """
  @callback prepare_ok_received(any(), VR.view_number(), VR.op_number(), from :: VR.replica_id()) ::
              {:ok, any()}
              | :start_view_change
              | {:start_view_change, VR.view_number()}

  @doc """
  Called when a COMMIT message is received from the primary.
  """
  @callback commit_received(any(), VR.view_number(), VR.commit_number()) ::
              {:ok, any()}
              | :start_view_change
              | {:start_view_change, VR.view_number()}

  @doc """
  Called when a STARTVIEWCHANGE message is received.
  """
  @callback start_view_change_received(any(), VR.view_number(), from :: VR.replica_id()) ::
              {:ok, any()}
              | :start_view_change
              | {:start_view_change, VR.view_number()}

  @doc """
  Called when a DOVIEWCHANGE message is received by the new primary.
  """
  @callback do_view_change_received(
              any(),
              VR.view_number(),
              log_entries :: list(),
              last_normal_view :: VR.view_number(),
              VR.op_number(),
              VR.commit_number(),
              from :: VR.replica_id()
            ) ::
              {:ok, any()}
              | {:become_normal, VR.view_number(), VR.op_number(), VR.commit_number(),
                 StateStore.t(), VR.ClientTable.t()}
              | {:become_primary, VR.view_number(), VR.op_number(), VR.commit_number(),
                 StateStore.t(), VR.ClientTable.t()}

  @doc """
  Called when a STARTVIEW message is received from the new primary.
  """
  @callback start_view_received(
              any(),
              VR.view_number(),
              log_entries :: list(),
              VR.op_number(),
              VR.commit_number()
            ) ::
              {:ok, any()}
              | {:become_normal, VR.view_number(), VR.op_number(), VR.commit_number(),
                 StateStore.t()}
              | {:become_normal, VR.view_number(), VR.op_number(), VR.commit_number(),
                 StateStore.t(), VR.ClientTable.t()}

  @doc """
  Called when a RECOVERY message is received.
  """
  @callback recovery_received(any(), nonce :: term(), from :: VR.replica_id()) ::
              {:ok, any()}

  @doc """
  Called when a RECOVERYRESPONSE message is received by a recovering replica.
  """
  @callback recovery_response_received(
              any(),
              VR.view_number(),
              nonce :: term(),
              log_entries :: list(),
              VR.op_number(),
              VR.commit_number(),
              is_primary :: boolean(),
              from :: VR.replica_id()
            ) ::
              {:ok, any()}
              | {:become_normal, VR.view_number(), VR.op_number(), VR.commit_number(),
                 StateStore.t()}

  @doc """
  Called when a GETSTATE message is received requesting state transfer.
  Per paper Section 5.2.
  """
  @callback get_state_received(
              any(),
              VR.view_number(),
              from_op_num :: VR.op_number(),
              from :: VR.replica_id()
            ) ::
              {:ok, any()}
              | :start_view_change
              | {:start_view_change, VR.view_number()}

  @doc """
  Called when a NEWSTATE message is received with state transfer data.
  Per paper Section 5.2.
  """
  @callback new_state_received(
              any(),
              VR.view_number(),
              log_entries :: list(),
              VR.op_number(),
              VR.commit_number(),
              from :: VR.replica_id()
            ) ::
              {:ok, any()}
              | :start_view_change
              | {:start_view_change, VR.view_number()}
end
