defmodule Bedrock.ViewstampedReplication.Mode.ViewChange do
  @moduledoc """
  View change mode for Viewstamped Replication.

  This mode handles the two-phase view change protocol:
  1. Collect STARTVIEWCHANGE messages from f replicas
  2. Send DOVIEWCHANGE to new primary
  3. New primary collects f+1 DOVIEWCHANGE messages
  4. New primary sends STARTVIEW to all replicas

  The view change protocol ensures that the new primary has the most
  up-to-date log by selecting the log with the highest (last_normal_view, op_number).
  """

  alias Bedrock.ViewstampedReplication, as: VR
  alias VR.ClientTable
  alias VR.Log

  @behaviour VR.Mode

  @type do_view_change_msg :: %{
          view_number: VR.view_number(),
          log_entries: list(),
          last_normal_view: VR.view_number(),
          op_number: VR.op_number(),
          commit_number: VR.commit_number(),
          from: VR.replica_id()
        }

  @type t :: %__MODULE__{
          view_number: VR.view_number(),
          last_normal_view: VR.view_number(),
          op_number: VR.op_number(),
          commit_number: VR.commit_number(),
          log: Log.t(),
          client_table: ClientTable.t(),
          configuration: [VR.replica_id()],
          replica_index: non_neg_integer(),
          quorum: VR.quorum(),
          am_new_primary: boolean(),
          start_view_change_from: MapSet.t(),
          do_view_change_messages: %{VR.replica_id() => do_view_change_msg()},
          sent_do_view_change: boolean(),
          cancel_timer_fn: (-> :ok) | nil,
          interface: module(),
          # Cached values for O(1) access (viewstamped_replication-73q, viewstamped_replication-mnw)
          num_replicas: non_neg_integer(),
          me: VR.replica_id()
        }

  defstruct ~w[
    view_number
    last_normal_view
    op_number
    commit_number
    log
    client_table
    configuration
    replica_index
    quorum
    am_new_primary
    start_view_change_from
    do_view_change_messages
    sent_do_view_change
    cancel_timer_fn
    interface
    num_replicas
    me
  ]a

  @doc """
  Create a new ViewChange mode instance.
  """
  @spec new(
          VR.view_number(),
          VR.view_number(),
          VR.op_number(),
          VR.commit_number(),
          Log.t(),
          ClientTable.t(),
          [VR.replica_id()],
          non_neg_integer(),
          VR.quorum(),
          module()
        ) :: t()
  # credo:disable-for-next-line Credo.Check.Refactor.FunctionArity
  def new(
        view_number,
        last_normal_view,
        op_number,
        commit_number,
        log,
        client_table,
        configuration,
        replica_index,
        quorum,
        interface
      ) do
    cancel_fn = interface.timer(:view_change)
    # Cache commonly accessed values (viewstamped_replication-73q, viewstamped_replication-mnw)
    num_replicas = length(configuration)
    me = Enum.at(configuration, replica_index)
    new_primary_index = rem(view_number, num_replicas)
    am_new_primary = replica_index == new_primary_index

    mode = %__MODULE__{
      view_number: view_number,
      last_normal_view: last_normal_view,
      op_number: op_number,
      commit_number: commit_number,
      log: log,
      client_table: client_table,
      configuration: configuration,
      replica_index: replica_index,
      quorum: quorum,
      am_new_primary: am_new_primary,
      start_view_change_from: MapSet.new(),
      do_view_change_messages: %{},
      sent_do_view_change: false,
      cancel_timer_fn: cancel_fn,
      interface: interface,
      num_replicas: num_replicas,
      me: me
    }

    # Send STARTVIEWCHANGE to all replicas
    send_start_view_change_to_all(mode)

    mode
  end

  @impl VR.Mode
  # View change timeout - restart with higher view number
  def timer_ticked(_mode, :view_change), do: :start_view_change

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
  def start_view_change_received(mode, view_num, from)

  def start_view_change_received(mode, view_num, _from) when view_num < mode.view_number,
    do: {:ok, mode}

  def start_view_change_received(mode, view_num, _from) when view_num > mode.view_number,
    do: {:start_view_change, view_num}

  def start_view_change_received(mode, _view_num, from) do
    # Per paper Section 4.2 Step 2: "from f other replicas"
    # Must be from OTHER replicas, not self
    me = mode.me

    if from == me do
      # Ignore self-messages
      {:ok, mode}
    else
      new_svc_from = MapSet.put(mode.start_view_change_from, from)
      new_mode = %{mode | start_view_change_from: new_svc_from}

      # Check if we have f STARTVIEWCHANGE messages (quorum - 1, since quorum = f + 1)
      if MapSet.size(new_svc_from) >= mode.quorum - 1 and not mode.sent_do_view_change do
        # Send DOVIEWCHANGE to new primary
        send_do_view_change(new_mode)
        {:ok, %{new_mode | sent_do_view_change: true}}
      else
        {:ok, new_mode}
      end
    end
  end

  @impl VR.Mode
  def do_view_change_received(
        mode,
        view_num,
        log_entries,
        last_normal_view,
        op_num,
        commit_num,
        from
      )

  def do_view_change_received(
        mode,
        view_num,
        _log_entries,
        _last_normal_view,
        _op_num,
        _commit_num,
        _from
      )
      when view_num != mode.view_number,
      do: {:ok, mode}

  def do_view_change_received(
        %{am_new_primary: false} = mode,
        _view_num,
        _log_entries,
        _last_normal_view,
        _op_num,
        _commit_num,
        _from
      ),
      do: {:ok, mode}

  def do_view_change_received(
        mode,
        _view_num,
        log_entries,
        last_normal_view,
        op_num,
        commit_num,
        from
      ) do
    msg = %{
      log_entries: log_entries,
      last_normal_view: last_normal_view,
      op_number: op_num,
      commit_number: commit_num,
      from: from
    }

    # O(1) duplicate check and insert (viewstamped_replication-q88)
    new_messages =
      if Map.has_key?(mode.do_view_change_messages, from) do
        mode.do_view_change_messages
      else
        Map.put(mode.do_view_change_messages, from, msg)
      end

    new_mode = %{mode | do_view_change_messages: new_messages}

    # Check if we have quorum (f+1) DOVIEWCHANGE messages
    if map_size(new_messages) >= mode.quorum do
      {best_uncommitted_entries, best_op_num, max_commit_num} =
        select_best_log(Map.values(new_messages))

      # Keep committed prefix, adopt best uncommitted suffix
      truncated_log = Log.truncate_after(mode.log, mode.commit_number)
      new_log = Log.append_entries(truncated_log, best_uncommitted_entries)

      full_log_entries = Log.entries_from(new_log, 1)
      send_start_view_to_all(new_mode, full_log_entries, best_op_num, max_commit_num)

      # Paper Section 4.2 Step 4: preserve client_table across view change
      {:become_primary, mode.view_number, best_op_num, max_commit_num, new_log, mode.client_table}
    else
      {:ok, new_mode}
    end
  end

  @impl VR.Mode
  def start_view_received(mode, view_num, log_entries, op_num, commit_num)

  def start_view_received(mode, view_num, _log_entries, _op_num, _commit_num)
      when view_num < mode.view_number,
      do: {:ok, mode}

  def start_view_received(mode, view_num, log_entries, op_num, commit_num) do
    new_log =
      mode.log
      |> Log.truncate_after(0)
      |> Log.append_entries(log_entries)

    # Per paper Section 4.2 Step 5:
    # "If there are non-committed operations in the log, they send a
    # PREPAREOK(v, n, i) message to the primary for these operations."
    # Only backups send PREPAREOKs, not the new primary
    num_replicas = mode.num_replicas
    new_primary_index = rem(view_num, num_replicas)
    am_new_primary = mode.replica_index == new_primary_index

    unless am_new_primary do
      send_prepare_oks_for_uncommitted(mode, view_num, commit_num, op_num)
    end

    # Paper Section 4.2 Step 5: preserve client_table across view change
    {:become_normal, view_num, op_num, commit_num, new_log, mode.client_table}
  end

  @impl VR.Mode
  # Don't respond during view change
  def recovery_received(mode, _nonce, _from), do: {:ok, mode}

  @impl VR.Mode
  def recovery_response_received(
        mode,
        _view_num,
        _nonce,
        _log_entries,
        _op_num,
        _commit_num,
        _is_primary,
        _from
      ),
      do: {:ok, mode}

  @impl VR.Mode
  # Don't respond during view change
  def get_state_received(mode, _view_num, _from_op_num, _from), do: {:ok, mode}

  @impl VR.Mode
  # Ignore during view change
  def new_state_received(mode, _view_num, _log_entries, _op_num, _commit_num, _from),
    do: {:ok, mode}

  # Private helpers

  # Per paper Section 4.2 Step 1: STARTVIEWCHANGE(v, i) where i is sender
  # The sender identity is provided by the transport layer, not the message tuple
  defp send_start_view_change_to_all(mode) do
    Enum.each(mode.configuration, fn replica ->
      mode.interface.send_event(replica, {:start_view_change, mode.view_number})
    end)
  end

  # Per paper Section 4.2 Step 2: DOVIEWCHANGE(v, l, v', n, k, i) where i is sender
  # The sender identity is provided by the transport layer, not the message tuple
  # Optimization: Send only uncommitted entries (commit_number + 1 onwards)
  # Committed entries are identical across replicas per VR invariant
  defp send_do_view_change(mode) do
    new_primary_index = rem(mode.view_number, mode.num_replicas)
    new_primary = Enum.at(mode.configuration, new_primary_index)

    log_entries = Log.entries_from(mode.log, mode.commit_number + 1)

    mode.interface.send_event(new_primary, {
      :do_view_change,
      mode.view_number,
      log_entries,
      mode.last_normal_view,
      mode.op_number,
      mode.commit_number
    })
  end

  # Per paper Section 4.2 Step 3: STARTVIEW sent to "other replicas" (not self) (viewstamped_replication-nfk)
  defp send_start_view_to_all(mode, log_entries, op_num, commit_num) do
    me = mode.me

    mode.configuration
    |> Enum.reject(&(&1 == me))
    |> Enum.each(fn replica ->
      mode.interface.send_event(
        replica,
        {:start_view, mode.view_number, log_entries, op_num, commit_num}
      )
    end)
  end

  defp select_best_log(messages) do
    # Select log with highest (last_normal_view, op_number)
    best = Enum.max_by(messages, fn msg -> {msg.last_normal_view, msg.op_number} end)

    # Use max commit_number from all messages
    max_commit = Enum.max_by(messages, & &1.commit_number).commit_number

    {best.log_entries, best.op_number, max_commit}
  end

  # Per paper Section 4.2 Step 5: PREPAREOK(v, n, i) where i is sender
  # The sender identity is provided by the transport layer, not the message tuple
  defp send_prepare_oks_for_uncommitted(mode, view_num, commit_num, op_num) do
    # Send PREPAREOK for each uncommitted operation (commit_num+1 to op_num)
    if op_num > commit_num do
      new_primary_index = rem(view_num, mode.num_replicas)
      new_primary = Enum.at(mode.configuration, new_primary_index)

      Enum.each((commit_num + 1)..op_num, fn n ->
        mode.interface.send_event(new_primary, {:prepare_ok, view_num, n})
      end)
    end
  end
end
