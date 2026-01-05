defmodule Bedrock.ViewstampedReplication.Mode.Normal do
  @moduledoc """
  Normal operation mode for Viewstamped Replication.

  In this mode, the replica is either operating as a primary (accepting client
  requests and coordinating replication) or as a backup (receiving and
  acknowledging PREPARE messages from the primary).

  The role is determined by whether replica_index == view_number rem num_replicas.
  """

  alias Bedrock.ViewstampedReplication, as: VR
  alias VR.ClientTable
  alias VR.Log
  alias VR.Mode.Normal.BackupTracking

  @behaviour VR.Mode

  @type t :: %__MODULE__{
          view_number: VR.view_number(),
          op_number: VR.op_number(),
          commit_number: VR.commit_number(),
          log: Log.t(),
          client_table: ClientTable.t(),
          configuration: [VR.replica_id()],
          replica_index: non_neg_integer(),
          quorum: VR.quorum(),
          is_primary: boolean(),
          backup_tracking: BackupTracking.t() | nil,
          cancel_timer_fn: (-> :ok) | nil,
          interface: module(),
          # Cached values for O(1) access (viewstamped_replication-73q, viewstamped_replication-mnw)
          num_replicas: non_neg_integer(),
          me: VR.replica_id(),
          # Per VR paper Section 4.2 (viewstamped_replication-6to): Track when we last heard from primary
          # Timer checks elapsed time at timeout instead of restarting on every message
          last_heard_at: non_neg_integer() | nil
        }

  defstruct ~w[
    view_number
    op_number
    commit_number
    log
    client_table
    configuration
    replica_index
    quorum
    is_primary
    backup_tracking
    cancel_timer_fn
    interface
    num_replicas
    me
    last_heard_at
  ]a

  @doc """
  Create a new Normal mode instance.

  The client_table parameter allows preserving client state across view changes.
  Pass `nil` to create a fresh client table (for initial startup).
  """
  @spec new(
          VR.view_number(),
          VR.op_number(),
          VR.commit_number(),
          Log.t(),
          [VR.replica_id()],
          non_neg_integer(),
          VR.quorum(),
          module(),
          boolean(),
          ClientTable.t() | nil
        ) :: t()
  # credo:disable-for-next-line Credo.Check.Refactor.FunctionArity
  def new(
        view_number,
        op_number,
        commit_number,
        log,
        configuration,
        replica_index,
        quorum,
        interface,
        is_primary,
        client_table \\ nil
      ) do
    timer_type = if is_primary, do: :heartbeat, else: :view_change

    # Per VR paper Section 4.2 (viewstamped_replication-6to): Backups track last_heard_at timestamp
    # Timer checks elapsed time at timeout instead of restarting on every message
    last_heard_at = if is_primary, do: nil, else: interface.timestamp_in_ms()

    cancel_fn = interface.timer(timer_type)

    backup_tracking =
      if is_primary do
        other_replicas =
          configuration
          |> Enum.with_index()
          |> Enum.reject(fn {_replica, idx} -> idx == replica_index end)
          |> Enum.map(fn {replica, _idx} -> replica end)

        BackupTracking.new(other_replicas)
      else
        nil
      end

    # Per paper Section 4.2 Step 5: preserve client_table across view changes
    actual_client_table = client_table || ClientTable.new()

    # Per paper Section 4.2 Steps 4 & 5:
    # Step 4 (primary): "executes (in order) any committed operations that it
    # hadn't executed previously, updates its client table, and sends the
    # replies to the clients."
    # Step 5 (backups): "Then they execute all operations known to be committed
    # that they haven't executed previously, advance their commit-number, and
    # update the information in their client-table."
    #
    # We need to find which operations are committed but not yet executed.
    # An operation is executed if it has a completed entry in the client table.
    # Primary sends replies to clients; backups do not.
    {final_log, final_client_table} =
      if commit_number > 0 do
        execute_pending_committed_operations(
          log,
          actual_client_table,
          commit_number,
          view_number,
          interface,
          is_primary
        )
      else
        {log, actual_client_table}
      end

    # Cache commonly accessed values (viewstamped_replication-73q, viewstamped_replication-mnw)
    num_replicas = length(configuration)
    me = Enum.at(configuration, replica_index)

    %__MODULE__{
      view_number: view_number,
      op_number: op_number,
      commit_number: commit_number,
      log: final_log,
      client_table: final_client_table,
      configuration: configuration,
      replica_index: replica_index,
      quorum: quorum,
      is_primary: is_primary,
      backup_tracking: backup_tracking,
      cancel_timer_fn: cancel_fn,
      interface: interface,
      num_replicas: num_replicas,
      me: me,
      last_heard_at: last_heard_at
    }
  end

  # Mode callbacks - to be implemented
  @impl VR.Mode
  def timer_ticked(mode, timer_type)

  def timer_ticked(%{is_primary: true} = mode, :heartbeat) do
    # Primary sends COMMIT to all backups
    send_commit_to_all(mode)
    cancel_fn = mode.interface.timer(:heartbeat)
    {:ok, %{mode | cancel_timer_fn: cancel_fn}}
  end

  def timer_ticked(%{is_primary: false} = mode, :view_change) do
    # Per VR paper Section 4.2 (viewstamped_replication-6to): Check at timeout time whether
    # we've heard from the primary recently, instead of restarting timer
    # on every message from primary
    now = mode.interface.timestamp_in_ms()
    timeout = mode.interface.view_change_timeout_ms()
    elapsed = now - mode.last_heard_at

    if elapsed >= timeout do
      # Haven't heard from primary within timeout, start view change
      :start_view_change
    else
      # Heard from primary recently, restart timer
      cancel_fn = mode.interface.timer(:view_change)
      {:ok, %{mode | cancel_timer_fn: cancel_fn}}
    end
  end

  def timer_ticked(mode, _timer_type), do: {:ok, mode}

  @impl VR.Mode
  def request_received(%{is_primary: false}, _client_id, _request_num, _operation),
    do: {:error, :not_primary}

  def request_received(mode, client_id, request_num, operation) do
    case ClientTable.check_request(mode.client_table, client_id, request_num) do
      :new ->
        # Assign new op number and add to log
        new_op_number = mode.op_number + 1

        entry = {client_id, request_num, operation}
        {:ok, new_log} = Log.append(mode.log, new_op_number, entry)

        # Update client table to track pending request
        new_client_table = ClientTable.record_pending(mode.client_table, client_id, request_num)

        # Send PREPARE to all backups
        send_prepare_to_all(mode, new_op_number, entry)

        new_mode = %{
          mode
          | op_number: new_op_number,
            log: new_log,
            client_table: new_client_table
        }

        {:ok, new_mode, new_op_number}

      :duplicate ->
        # Already processing this request, drop it
        {:ok, mode, mode.op_number}

      {:cached, result} ->
        # Re-send cached response
        mode.interface.send_reply(client_id, mode.view_number, request_num, result)
        {:ok, mode, {:cached, result}}
    end
  end

  @impl VR.Mode
  def prepare_received(mode, view_num, message, op_num, commit_num, from)

  # Old view, ignore
  def prepare_received(mode, view_num, _message, _op_num, _commit_num, _from)
      when view_num < mode.view_number,
      do: {:ok, mode}

  # Newer view, need to catch up via view change
  def prepare_received(mode, view_num, _message, _op_num, _commit_num, _from)
      when view_num > mode.view_number,
      do: {:start_view_change, view_num}

  # Primary shouldn't receive PREPARE
  def prepare_received(
        %{is_primary: true} = mode,
        _view_num,
        _message,
        _op_num,
        _commit_num,
        _from
      ),
      do: {:ok, mode}

  def prepare_received(
        mode,
        _view_num,
        {client_id, request_num, operation},
        op_num,
        commit_num,
        _from
      ) do
    # Per VR paper Section 4.2 (viewstamped_replication-6to): Update last_heard_at instead of restarting timer
    last_heard_at = mode.interface.timestamp_in_ms()

    # Check if we can append (no gap)
    if op_num == mode.op_number + 1 do
      entry = {client_id, request_num, operation}
      {:ok, new_log} = Log.append(mode.log, op_num, entry)

      # Per paper Section 4.1 Step 4:
      # "updates the client's information in the client-table"
      # Record the pending request in client table
      updated_client_table = ClientTable.record_pending(mode.client_table, client_id, request_num)

      # Execute any newly committed operations
      {executed_log, new_client_table} =
        execute_committed_operations(
          new_log,
          updated_client_table,
          mode.commit_number,
          commit_num,
          mode.interface
        )

      # Send PREPAREOK to primary
      # Per paper Section 4.1 Step 4: PREPAREOK(v, n, i) where i is sender
      # The sender identity is provided by the transport layer, not the message tuple
      primary = Enum.at(mode.configuration, primary_index(mode.view_number, mode.num_replicas))
      mode.interface.send_event(primary, {:prepare_ok, mode.view_number, op_num})

      new_mode = %{
        mode
        | op_number: op_num,
          commit_number: max(mode.commit_number, commit_num),
          log: executed_log,
          client_table: new_client_table,
          last_heard_at: last_heard_at
      }

      {:ok, new_mode}
    else
      # Gap in log - request state transfer per Section 4.1 Step 4
      # "it waits until it has entries in its log for all earlier requests
      # (doing state transfer if necessary to get the missing information)"
      primary = Enum.at(mode.configuration, primary_index(mode.view_number, mode.num_replicas))

      # Request missing entries starting from our next expected op
      mode.interface.send_event(primary, {:get_state, mode.view_number, mode.op_number + 1})

      {:ok, %{mode | last_heard_at: last_heard_at}}
    end
  end

  @impl VR.Mode
  def prepare_ok_received(mode, view_num, op_num, from)

  def prepare_ok_received(mode, view_num, _op_num, _from) when view_num != mode.view_number,
    do: {:ok, mode}

  def prepare_ok_received(%{is_primary: false} = mode, _view_num, _op_num, _from),
    do: {:ok, mode}

  def prepare_ok_received(mode, _view_num, op_num, from) do
    new_tracking = BackupTracking.record_ack(mode.backup_tracking, from, op_num)
    ack_count = BackupTracking.count_acks_for(new_tracking, op_num)

    # Check if we have quorum (including ourselves)
    new_mode =
      if ack_count + 1 >= mode.quorum and op_num > mode.commit_number do
        # Commit operations from commit_number+1 to op_num
        {executed_log, new_client_table} =
          execute_and_reply(
            mode.log,
            mode.client_table,
            mode.commit_number,
            op_num,
            mode.view_number,
            mode.interface
          )

        %{
          mode
          | commit_number: op_num,
            log: executed_log,
            client_table: new_client_table,
            backup_tracking: new_tracking
        }
      else
        %{mode | backup_tracking: new_tracking}
      end

    {:ok, new_mode}
  end

  @impl VR.Mode
  def commit_received(mode, view_num, commit_num)

  def commit_received(mode, view_num, _commit_num) when view_num < mode.view_number,
    do: {:ok, mode}

  def commit_received(mode, view_num, _commit_num) when view_num > mode.view_number,
    do: {:start_view_change, view_num}

  def commit_received(%{is_primary: true} = mode, _view_num, _commit_num), do: {:ok, mode}

  def commit_received(mode, _view_num, commit_num) do
    # Per VR paper Section 4.2 (viewstamped_replication-6to): Update last_heard_at instead of restarting timer
    last_heard_at = mode.interface.timestamp_in_ms()

    if commit_num > mode.commit_number do
      {executed_log, new_client_table} =
        execute_committed_operations(
          mode.log,
          mode.client_table,
          mode.commit_number,
          commit_num,
          mode.interface
        )

      {:ok,
       %{
         mode
         | commit_number: commit_num,
           log: executed_log,
           client_table: new_client_table,
           last_heard_at: last_heard_at
       }}
    else
      {:ok, %{mode | last_heard_at: last_heard_at}}
    end
  end

  @impl VR.Mode
  def start_view_change_received(mode, view_num, _from) when view_num <= mode.view_number,
    do: {:ok, mode}

  def start_view_change_received(_mode, view_num, _from), do: {:start_view_change, view_num}

  @impl VR.Mode
  # Normal mode shouldn't process DOVIEWCHANGE
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
  def start_view_received(mode, view_num, log_entries, op_num, commit_num)

  def start_view_received(mode, view_num, _log_entries, _op_num, _commit_num)
      when view_num <= mode.view_number,
      do: {:ok, mode}

  def start_view_received(mode, view_num, log_entries, op_num, commit_num) do
    new_log =
      mode.log
      |> Log.truncate_after(0)
      |> Log.append_entries(log_entries)

    {:become_normal, view_num, op_num, commit_num, new_log}
  end

  @impl VR.Mode
  def recovery_received(mode, nonce, from) do
    # Only respond if we're in normal mode
    # Per paper Section 4.3 Step 2:
    # "If j is the primary of its view, l is its log, n is its op-number, and k
    # is the commit-number; otherwise these values are nil."
    is_primary = mode.is_primary

    {log_entries, op_num, commit_num} =
      if is_primary do
        {Log.entries_from(mode.log, 1), mode.op_number, mode.commit_number}
      else
        {nil, nil, nil}
      end

    mode.interface.send_event(from, {
      :recovery_response,
      mode.view_number,
      nonce,
      log_entries,
      op_num,
      commit_num,
      is_primary
    })

    {:ok, mode}
  end

  @impl VR.Mode
  # Normal mode shouldn't process RECOVERYRESPONSE
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
  def get_state_received(mode, view_num, from_op_num, from)

  def get_state_received(mode, view_num, _from_op_num, _from)
      when view_num < mode.view_number,
      do: {:ok, mode}

  def get_state_received(mode, view_num, _from_op_num, _from)
      when view_num > mode.view_number,
      do: {:start_view_change, view_num}

  # Per paper Section 5.2:
  # "A replica responds to a GETSTATE message only if its status is normal
  # and it is currently in view v."
  # Note: ANY normal replica can respond, not just primary. This allows
  # faster state transfer by distributing load across all replicas.
  def get_state_received(mode, _view_num, from_op_num, from) do
    # Respond with log entries from requested op number onwards
    log_entries = Log.entries_from(mode.log, from_op_num)

    mode.interface.send_event(from, {
      :new_state,
      mode.view_number,
      log_entries,
      mode.op_number,
      mode.commit_number
    })

    {:ok, mode}
  end

  @impl VR.Mode
  def new_state_received(mode, view_num, log_entries, op_num, commit_num, from)

  def new_state_received(mode, view_num, _log_entries, _op_num, _commit_num, _from)
      when view_num < mode.view_number,
      do: {:ok, mode}

  def new_state_received(mode, view_num, _log_entries, _op_num, _commit_num, _from)
      when view_num > mode.view_number,
      do: {:start_view_change, view_num}

  # Primary shouldn't receive NEWSTATE
  def new_state_received(
        %{is_primary: true} = mode,
        _view_num,
        _log_entries,
        _op_num,
        _commit_num,
        _from
      ),
      do: {:ok, mode}

  def new_state_received(mode, _view_num, log_entries, op_num, commit_num, _from) do
    # Per VR paper Section 4.2 (viewstamped_replication-6to): Update last_heard_at instead of restarting timer
    last_heard_at = mode.interface.timestamp_in_ms()

    # INTENTIONAL DEVIATION from paper Section 5.2 (Vanlightly 2022 bug fix):
    # The paper's algorithm replaces the log, which can lose committed entries
    # if the backup has entries the primary doesn't send. We MERGE instead,
    # preserving existing entries. See: viewstamped_replication-k2g, normal_test.exs:577
    new_log = Log.append_entries(mode.log, log_entries)

    # Execute newly committed operations
    {executed_log, new_client_table} =
      execute_committed_operations(
        new_log,
        mode.client_table,
        mode.commit_number,
        commit_num,
        mode.interface
      )

    new_mode = %{
      mode
      | log: executed_log,
        op_number: max(mode.op_number, op_num),
        commit_number: max(mode.commit_number, commit_num),
        client_table: new_client_table,
        last_heard_at: last_heard_at
    }

    {:ok, new_mode}
  end

  # Private helpers

  defp primary_index(view_num, num_replicas), do: rem(view_num, num_replicas)

  defp send_prepare_to_all(mode, op_num, entry) do
    primary_idx = primary_index(mode.view_number, mode.num_replicas)

    mode.configuration
    |> Enum.with_index()
    |> Enum.reject(fn {_replica, idx} -> idx == primary_idx end)
    |> Enum.each(fn {replica, _idx} ->
      mode.interface.send_event(
        replica,
        {:prepare, mode.view_number, entry, op_num, mode.commit_number}
      )
    end)
  end

  defp send_commit_to_all(mode) do
    primary_idx = primary_index(mode.view_number, mode.num_replicas)

    mode.configuration
    |> Enum.with_index()
    |> Enum.reject(fn {_replica, idx} -> idx == primary_idx end)
    |> Enum.each(fn {replica, _idx} ->
      mode.interface.send_event(replica, {:commit, mode.view_number, mode.commit_number})
    end)
  end

  defp execute_committed_operations(log, client_table, old_commit, new_commit, interface) do
    if new_commit > old_commit do
      Enum.reduce((old_commit + 1)..new_commit, {log, client_table}, fn op_num, acc ->
        execute_single_operation(acc, Log.get(log, op_num), op_num, interface)
      end)
    else
      {log, client_table}
    end
  end

  defp execute_single_operation({l, ct}, {client_id, request_num, operation}, op_num, interface) do
    {:ok, result} = interface.execute_operation(operation)
    interface.operation_committed(l, op_num, operation, result)
    {l, ClientTable.record_result(ct, client_id, request_num, result)}
  end

  defp execute_single_operation(acc, nil, _op_num, _interface), do: acc

  defp execute_and_reply(log, client_table, old_commit, new_commit, view_number, interface) do
    if new_commit > old_commit do
      Enum.reduce((old_commit + 1)..new_commit, {log, client_table}, fn op_num, acc ->
        execute_and_reply_single(acc, Log.get(log, op_num), op_num, view_number, interface)
      end)
    else
      {log, client_table}
    end
  end

  defp execute_and_reply_single({l, ct}, {client_id, request_num, op}, op_num, view, interface) do
    {:ok, result} = interface.execute_operation(op)
    interface.operation_committed(l, op_num, op, result)
    interface.send_reply(client_id, view, request_num, result)
    {l, ClientTable.record_result(ct, client_id, request_num, result)}
  end

  defp execute_and_reply_single(acc, nil, _op_num, _view, _interface), do: acc

  # Per paper Section 4.2 Steps 4 & 5:
  # Execute any committed operations that haven't been executed yet.
  # This is called when replicas transition to normal mode after a view change.
  # Primary sends replies to clients; backups do not.
  defp execute_pending_committed_operations(log, ct, commit, view, interface, send_replies) do
    if commit > 0 do
      Enum.reduce(1..commit, {log, ct}, fn op_num, acc ->
        execute_pending_single(acc, Log.get(log, op_num), op_num, view, interface, send_replies)
      end)
    else
      {log, ct}
    end
  end

  defp execute_pending_single({l, ct}, {client_id, req_num, op}, op_num, view, iface, send?) do
    case ClientTable.check_request(ct, client_id, req_num) do
      {:cached, _result} ->
        {l, ct}

      _ ->
        execute_pending_operation({l, ct}, {client_id, req_num, op}, op_num, view, iface, send?)
    end
  end

  defp execute_pending_single(acc, nil, _op_num, _view, _iface, _send?), do: acc

  defp execute_pending_operation({l, ct}, {client_id, req_num, op}, op_num, view, iface, send?) do
    {:ok, result} = iface.execute_operation(op)
    iface.operation_committed(l, op_num, op, result)
    if send?, do: iface.send_reply(client_id, view, req_num, result)
    {l, ClientTable.record_result(ct, client_id, req_num, result)}
  end
end
