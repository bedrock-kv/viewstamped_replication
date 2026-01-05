defmodule Bedrock.ViewstampedReplication do
  @moduledoc """
  The Viewstamped Replication (VR) consensus protocol. This module implements
  the state machine for the protocol. Externalities like timers and message
  transport are left as details to the user of this module, allowing it to be
  used in a variety of contexts.

  VR is a replication protocol that uses a primary replica to order client
  requests. The primary is selected deterministically based on the view number,
  eliminating the need for leader election. When the primary fails, replicas
  perform a view change to select a new primary.

  This implementation follows the protocol as described in:
  "Viewstamped Replication Revisited" by Liskov & Cowling (2012)
  http://pmg.csail.mit.edu/papers/vr-revisited.pdf

  ## Key Concepts

  - **View Number**: A monotonically increasing number that identifies the
    current configuration. The primary is determined by `view_number rem n`
    where n is the number of replicas.

  - **Op Number**: A monotonically increasing number assigned to each client
    request by the primary.

  - **Commit Number**: The op number of the most recently committed operation.

  - **Client Table**: Tracks the most recent request from each client along
    with its cached response for deduplication.

  ## Usage

  Alias internally as:

      alias Bedrock.ViewstampedReplication, as: VR

  """

  alias Bedrock.ViewstampedReplication, as: VR
  alias VR.ClientTable
  alias VR.Mode.Normal
  alias VR.Mode.Recovering
  alias VR.Mode.ViewChange
  alias VR.StateStore

  import VR.Telemetry,
    only: [
      track_became_normal: 3,
      track_started_view_change: 2,
      track_started_recovering: 1,
      track_primary_change: 2,
      track_ignored_event: 2
    ]

  @type view_number :: non_neg_integer()
  @type op_number :: non_neg_integer()
  @type commit_number :: non_neg_integer()
  @type replica_id :: atom() | pid() | {atom(), replica_id()}
  @type client_id :: term()
  @type request_number :: non_neg_integer()
  @type status :: :normal | :view_change | :recovering
  @type quorum :: pos_integer()
  @type primary_info :: {primary :: replica_id(), view_number()}

  @type t :: %__MODULE__{
          me: replica_id(),
          replica_index: non_neg_integer(),
          configuration: [replica_id()],
          quorum: quorum(),
          mode: Normal.t() | ViewChange.t() | Recovering.t() | nil,
          interface: module()
        }

  defstruct ~w[
    me
    replica_index
    configuration
    quorum
    mode
    interface
  ]a

  @doc """
  Create a new Viewstamped Replication protocol instance.

  ## Parameters

  - `me` - The identifier for this replica
  - `configuration` - A sorted list of all replica identifiers (must include `me`)
  - `store` - The StateStore implementation to use
  - `interface` - The module implementing the Interface behavior

  ## Returns

  A new VR instance that starts in normal mode as a backup, waiting for
  the primary to send messages. If this replica is the primary (determined
  by view_number rem length(configuration)), it will start as the primary.
  """
  @spec new(
          me :: replica_id(),
          configuration :: [replica_id()],
          store :: StateStore.t(),
          interface :: module()
        ) :: t()
  def new(me, configuration, store, interface) do
    # Configuration must be sorted for deterministic primary selection
    sorted_config = Enum.sort(configuration)

    # Find our position in the configuration
    replica_index = Enum.find_index(sorted_config, &(&1 == me))

    unless replica_index do
      raise ArgumentError, "me (#{inspect(me)}) must be in configuration"
    end

    # For 2f+1 replicas, quorum is f+1
    quorum = calculate_quorum(length(sorted_config))

    # Restore view number from persistent storage
    view_number = StateStore.get_view_number(store)

    %__MODULE__{
      me: me,
      replica_index: replica_index,
      configuration: sorted_config,
      quorum: quorum,
      interface: interface
    }
    |> become_normal(view_number, 0, 0, store, nil)
  end

  @doc """
  Return the state store for this protocol instance.
  """
  @spec store(t()) :: StateStore.t()
  def store(t), do: t.mode.store

  @doc """
  Return the replica identifier for this instance.
  """
  @spec me(t()) :: replica_id()
  def me(%{me: me}), do: me

  @doc """
  Return true if this replica is the primary in the current view.
  """
  @spec am_i_primary?(t()) :: boolean()
  def am_i_primary?(t),
    do: t.replica_index == primary_index(view_number(t), length(t.configuration))

  @doc """
  Return the current primary of the cluster.
  """
  @spec primary(t()) :: replica_id() | nil
  def primary(%{mode: nil}), do: nil

  def primary(t),
    do: Enum.at(t.configuration, primary_index(view_number(t), length(t.configuration)))

  @doc """
  Return the current view number.
  """
  @spec view_number(t()) :: view_number()
  def view_number(%{mode: nil}), do: 0

  def view_number(%{mode: %mode{}} = t)
      when mode in [Normal, ViewChange, Recovering],
      do: t.mode.view_number

  @doc """
  Return the current op number.
  """
  @spec op_number(t()) :: op_number()
  def op_number(%{mode: %mode{}} = t)
      when mode in [Normal, ViewChange],
      do: t.mode.op_number

  def op_number(%{mode: %Recovering{}}), do: 0

  @doc """
  Return the current commit number.
  """
  @spec commit_number(t()) :: commit_number()
  def commit_number(%{mode: %mode{}} = t)
      when mode in [Normal, ViewChange],
      do: t.mode.commit_number

  def commit_number(%{mode: %Recovering{}}), do: 0

  @doc """
  Return the current status of this replica.
  """
  @spec status(t()) :: status()
  def status(%{mode: %Normal{}}), do: :normal
  def status(%{mode: %ViewChange{}}), do: :view_change
  def status(%{mode: %Recovering{}}), do: :recovering

  @doc """
  Return the current primary and view number.
  """
  @spec primary_info(t()) :: primary_info()
  def primary_info(t), do: {primary(t), view_number(t)}

  @doc """
  Return all replicas in the configuration.
  """
  @spec configuration(t()) :: [replica_id()]
  def configuration(t), do: t.configuration

  @doc """
  Submit a client request. This is only valid when this replica is the primary.

  Returns `{:ok, t, op_number}` if the request was accepted and assigned an
  op number. Returns `{:error, :not_primary}` if this replica is not the
  primary. Returns `{:ok, t, {:cached, result}}` if this is a duplicate
  request and the result is cached.
  """
  @spec submit_request(t(), client_id(), request_number(), operation :: term()) ::
          {:ok, t(), op_number()} | {:error, :not_primary} | {:ok, t(), {:cached, term()}}
  def submit_request(%{mode: %Normal{} = mode} = t, client_id, request_number, operation) do
    if am_i_primary?(t) do
      mode
      |> Normal.request_received(client_id, request_number, operation)
      |> case do
        {:ok, %Normal{} = new_mode, result} ->
          {:ok, %{t | mode: new_mode}, result}

        {:error, _} = error ->
          error
      end
    else
      {:error, :not_primary}
    end
  end

  def submit_request(_t, _client_id, _request_number, _operation),
    do: {:error, :not_primary}

  @doc """
  Handle an event from outside the protocol. Events can come from other
  replicas or from a timer.
  """
  @spec handle_event(t(), event :: term(), source :: replica_id() | :timer) :: t()

  # Timer events
  def handle_event(%{mode: %mode{}} = t, timer_name, :timer) when mode in [Normal, ViewChange] do
    mode.timer_ticked(t.mode, timer_name)
    |> handle_mode_result(t)
  end

  # PREPARE message from primary
  def handle_event(
        %{mode: %Normal{} = mode} = t,
        {:prepare, view_num, message, op_num, commit_num},
        from
      ) do
    mode
    |> Normal.prepare_received(view_num, message, op_num, commit_num, from)
    |> handle_mode_result(t)
  end

  # PREPAREOK message from backup
  def handle_event(
        %{mode: %Normal{} = mode} = t,
        {:prepare_ok, view_num, op_num},
        from
      ) do
    mode
    |> Normal.prepare_ok_received(view_num, op_num, from)
    |> handle_mode_result(t)
  end

  # COMMIT message from primary
  def handle_event(
        %{mode: %Normal{} = mode} = t,
        {:commit, view_num, commit_num},
        _from
      ) do
    mode
    |> Normal.commit_received(view_num, commit_num)
    |> handle_mode_result(t)
  end

  # STARTVIEWCHANGE message
  def handle_event(
        %{mode: %mode{}} = t,
        {:start_view_change, view_num},
        from
      )
      when mode in [Normal, ViewChange] do
    mode.start_view_change_received(t.mode, view_num, from)
    |> handle_mode_result(t)
  end

  # DOVIEWCHANGE message (only processed by new primary during view change)
  def handle_event(
        %{mode: %ViewChange{} = mode} = t,
        {:do_view_change, view_num, state_data, last_normal_view, op_num, commit_num},
        from
      ) do
    mode
    |> ViewChange.do_view_change_received(
      view_num,
      state_data,
      last_normal_view,
      op_num,
      commit_num,
      from
    )
    |> handle_mode_result(t)
  end

  # STARTVIEW message from new primary
  def handle_event(
        %{mode: %mode{}} = t,
        {:start_view, view_num, state_data, op_num, commit_num},
        _from
      )
      when mode in [Normal, ViewChange] do
    mode.start_view_received(t.mode, view_num, state_data, op_num, commit_num)
    |> handle_mode_result(t)
  end

  # RECOVERY message
  def handle_event(
        %{mode: %Normal{} = mode} = t,
        {:recovery, nonce},
        from
      ) do
    mode
    |> Normal.recovery_received(nonce, from)
    |> handle_mode_result(t)
  end

  # RECOVERYRESPONSE message
  def handle_event(
        %{mode: %Recovering{} = mode} = t,
        {:recovery_response, view_num, nonce, state_data, op_num, commit_num, is_primary},
        from
      ) do
    mode
    |> Recovering.recovery_response_received(
      view_num,
      nonce,
      state_data,
      op_num,
      commit_num,
      is_primary,
      from
    )
    |> handle_mode_result(t)
  end

  # GETSTATE message - backup requesting state transfer (Section 5.2)
  def handle_event(
        %{mode: %Normal{} = mode} = t,
        {:get_state, view_num, from_op_num},
        from
      ) do
    mode
    |> Normal.get_state_received(view_num, from_op_num, from)
    |> handle_mode_result(t)
  end

  # NEWSTATE message - replica responding with state transfer (Section 5.2)
  def handle_event(
        %{mode: %Normal{} = mode} = t,
        {:new_state, view_num, state_data, op_num, commit_num},
        from
      ) do
    mode
    |> Normal.new_state_received(view_num, state_data, op_num, commit_num, from)
    |> handle_mode_result(t)
  end

  # Unknown or ignored events
  def handle_event(t, event, from) do
    track_ignored_event(event, from)
    t.interface.ignored_event(event, from)
    t
  end

  # Handle mode transition results
  defp handle_mode_result({:ok, mode}, t), do: %{t | mode: mode}

  defp handle_mode_result(:start_view_change, t),
    do: start_view_change(t, view_number(t) + 1)

  defp handle_mode_result({:start_view_change, new_view_num}, t),
    do: start_view_change(t, new_view_num)

  defp handle_mode_result({:become_normal, view_num, op_num, commit_num, new_store}, t),
    do: become_normal(t, view_num, op_num, commit_num, new_store, nil)

  # With client_table preservation (Paper Section 4.2 Step 5)
  defp handle_mode_result({:become_normal, view_num, op_num, commit_num, new_store, client_table}, t),
    do: become_normal(t, view_num, op_num, commit_num, new_store, client_table)

  defp handle_mode_result({:become_primary, view_num, op_num, commit_num, new_store}, t),
    do: become_normal(t, view_num, op_num, commit_num, new_store, nil)

  # With client_table preservation (Paper Section 4.2 Step 4)
  defp handle_mode_result({:become_primary, view_num, op_num, commit_num, new_store, client_table}, t),
    do: become_normal(t, view_num, op_num, commit_num, new_store, client_table)

  # State transitions
  defp become_normal(t, view_num, op_num, commit_num, new_store, client_table) do
    is_primary = t.replica_index == primary_index(view_num, length(t.configuration))

    track_became_normal(view_num, is_primary, t.me)

    # Persist view number if it changed
    updated_store =
      if view_num > StateStore.get_view_number(new_store) do
        {:ok, saved_store} = StateStore.save_view_number(new_store, view_num)
        saved_store
      else
        new_store
      end

    mode =
      Normal.new(
        view_num,
        op_num,
        commit_num,
        updated_store,
        t.configuration,
        t.replica_index,
        t.quorum,
        t.interface,
        is_primary,
        client_table
      )

    old_primary = primary(t)

    new_t = %{t | mode: mode}

    notify_primary_change(new_t, old_primary)
  end

  defp start_view_change(t, new_view_num) do
    last_normal_view =
      case t.mode do
        %Normal{} -> view_number(t)
        %ViewChange{last_normal_view: lnv} -> lnv
        _ -> 0
      end

    # Per paper Section 4.2: preserve client_table across view change
    client_table =
      case t.mode do
        %Normal{client_table: ct} -> ct
        %ViewChange{client_table: ct} -> ct
        _ -> ClientTable.new()
      end

    track_started_view_change(new_view_num, t.me)

    # Persist the new view number
    {:ok, updated_store} = StateStore.save_view_number(store(t), new_view_num)

    mode =
      ViewChange.new(
        new_view_num,
        last_normal_view,
        op_number(t),
        commit_number(t),
        updated_store,
        client_table,
        t.configuration,
        t.replica_index,
        t.quorum,
        t.interface
      )

    old_primary = primary(t)

    new_t = %{t | mode: mode}

    notify_primary_change(new_t, old_primary)
  end

  @doc false
  def start_recovering(t, nonce) do
    track_started_recovering(t.me)

    mode =
      Recovering.new(
        nonce,
        store(t),
        t.configuration,
        t.replica_index,
        t.quorum,
        t.interface
      )

    %{t | mode: mode}
  end

  # Calculate the primary index for a given view number
  defp primary_index(view_num, num_replicas), do: rem(view_num, num_replicas)

  # Calculate quorum: f+1 where 2f+1 = num_replicas
  defp calculate_quorum(num_replicas) do
    f = div(num_replicas - 1, 2)
    f + 1
  end

  defp notify_primary_change(t, old_primary) do
    current_primary = primary(t)

    if current_primary != old_primary do
      current_view = view_number(t)
      t.interface.primary_changed({current_primary, current_view})
      track_primary_change(current_primary, current_view)
    end

    t
  end
end
