defmodule Bedrock.ViewstampedReplication.Mode.Recovering do
  @moduledoc """
  Recovery mode for Viewstamped Replication.

  A replica enters this mode after a crash to recover its state from
  other replicas. The protocol:

  1. Generate a unique nonce
  2. Send RECOVERY message with nonce to all replicas
  3. Wait for f+1 RECOVERYRESPONSE messages with matching nonce
  4. At least one response must be from the current primary (includes log)
  5. Update state from primary's response and become normal
  """

  alias Bedrock.ViewstampedReplication, as: VR
  alias VR.Log

  @behaviour VR.Mode

  @type recovery_response :: %{
          view_number: VR.view_number(),
          nonce: term(),
          log_entries: list(),
          op_number: VR.op_number(),
          commit_number: VR.commit_number(),
          is_primary: boolean(),
          from: VR.replica_id()
        }

  @type t :: %__MODULE__{
          view_number: VR.view_number(),
          nonce: term(),
          log: Log.t(),
          configuration: [VR.replica_id()],
          replica_index: non_neg_integer(),
          quorum: VR.quorum(),
          responses: %{VR.replica_id() => recovery_response()},
          cancel_timer_fn: (-> :ok) | nil,
          interface: module(),
          # Cached value for O(1) access (viewstamped_replication-mnw)
          me: VR.replica_id()
        }

  defstruct ~w[
    view_number
    nonce
    log
    configuration
    replica_index
    quorum
    responses
    cancel_timer_fn
    interface
    me
  ]a

  @doc """
  Create a new Recovering mode instance.
  """
  @spec new(
          term(),
          Log.t(),
          [VR.replica_id()],
          non_neg_integer(),
          VR.quorum(),
          module()
        ) :: t()
  def new(nonce, log, configuration, replica_index, quorum, interface) do
    cancel_fn = interface.timer(:recovery)
    me = Enum.at(configuration, replica_index)

    mode = %__MODULE__{
      view_number: 0,
      nonce: nonce,
      log: log,
      configuration: configuration,
      replica_index: replica_index,
      quorum: quorum,
      responses: %{},
      cancel_timer_fn: cancel_fn,
      interface: interface,
      me: me
    }

    # Send RECOVERY to all replicas
    # Per paper Section 4.3 Step 1: RECOVERY(i, x) where i is sender, x is nonce
    # The sender identity is provided by the transport layer, not the message tuple
    Enum.each(configuration, fn replica ->
      if replica != me do
        interface.send_event(replica, {:recovery, nonce})
      end
    end)

    mode
  end

  @impl VR.Mode
  def timer_ticked(mode, :recovery) do
    # Recovery timeout - retry
    # Per paper Section 4.3 Step 1: RECOVERY(i, x) where i is sender, x is nonce
    # The sender identity is provided by the transport layer, not the message tuple
    me = mode.me

    Enum.each(mode.configuration, fn replica ->
      if replica != me do
        mode.interface.send_event(replica, {:recovery, mode.nonce})
      end
    end)

    cancel_fn = mode.interface.timer(:recovery)
    {:ok, %{mode | cancel_timer_fn: cancel_fn}}
  end

  def timer_ticked(mode, _timer_type), do: {:ok, mode}

  @impl VR.Mode
  def request_received(_mode, _client_id, _request_num, _operation), do: {:error, :not_primary}

  @impl VR.Mode
  def prepare_received(mode, _view_num, _message, _op_num, _commit_num, _from), do: {:ok, mode}

  @impl VR.Mode
  def prepare_ok_received(mode, _view_num, _op_num, _from), do: {:ok, mode}

  @impl VR.Mode
  def commit_received(mode, _view_num, _commit_num), do: {:ok, mode}

  @impl VR.Mode
  def start_view_change_received(mode, _view_num, _from), do: {:ok, mode}

  @impl VR.Mode
  def do_view_change_received(
        mode,
        _view_num,
        _log_entries,
        _last_normal_view,
        _op_num,
        _commit_num,
        _from
      ),
      do: {:ok, mode}

  @impl VR.Mode
  def start_view_received(mode, _view_num, _log_entries, _op_num, _commit_num), do: {:ok, mode}

  @impl VR.Mode
  # Can't help others while recovering
  def recovery_received(mode, _nonce, _from), do: {:ok, mode}

  @impl VR.Mode
  # Can't help others while recovering
  def get_state_received(mode, _view_num, _from_op_num, _from), do: {:ok, mode}

  @impl VR.Mode
  # Ignore during recovery
  def new_state_received(mode, _view_num, _log_entries, _op_num, _commit_num, _from),
    do: {:ok, mode}

  @impl VR.Mode
  def recovery_response_received(
        mode,
        view_num,
        nonce,
        log_entries,
        op_num,
        commit_num,
        is_primary,
        from
      )

  # Wrong nonce, ignore
  def recovery_response_received(
        mode,
        _view_num,
        nonce,
        _log_entries,
        _op_num,
        _commit_num,
        _is_primary,
        _from
      )
      when nonce != mode.nonce,
      do: {:ok, mode}

  def recovery_response_received(
        mode,
        view_num,
        _nonce,
        log_entries,
        op_num,
        commit_num,
        is_primary,
        from
      ) do
    response = %{
      view_number: view_num,
      log_entries: log_entries,
      op_number: op_num,
      commit_number: commit_num,
      is_primary: is_primary,
      from: from
    }

    # O(1) duplicate check and insert (viewstamped_replication-q88)
    new_responses =
      if Map.has_key?(mode.responses, from) do
        mode.responses
      else
        Map.put(mode.responses, from, response)
      end

    # Per paper Section 4.3 Step 3: track highest view_number seen (viewstamped_replication-6hf)
    new_view_number = max(mode.view_number, view_num)

    new_mode = %{mode | responses: new_responses, view_number: new_view_number}

    # Check if we have quorum
    has_quorum = map_size(new_responses) >= mode.quorum

    # Per paper Section 4.3 Step 3:
    # "including one from the primary of the latest view it learns of in these messages"
    # Find the highest view number among all responses
    response_values = Map.values(new_responses)
    latest_view = Enum.max_by(response_values, & &1.view_number).view_number

    # Find the primary response from that latest view
    primary_from_latest_view =
      Enum.find(response_values, fn r ->
        r.is_primary and r.view_number == latest_view
      end)

    if has_quorum and primary_from_latest_view != nil do
      new_log =
        new_mode.log
        |> Log.truncate_after(0)
        |> Log.append_entries(primary_from_latest_view.log_entries)

      {:become_normal, primary_from_latest_view.view_number, primary_from_latest_view.op_number,
       primary_from_latest_view.commit_number, new_log}
    else
      {:ok, new_mode}
    end
  end
end
