defprotocol Bedrock.ViewstampedReplication.StateStore do
  @moduledoc """
  Protocol for state storage in Viewstamped Replication.

  Abstracts over both log-based (full history) and snapshot-based
  (current state) storage strategies. This allows VR to work with
  different storage backends without knowing the details.

  ## Log-based vs Snapshot-based

  - **Log-based**: Stores full mutation history. Supports incremental
    state transfer via `get_state_from/2` returning `{:incremental, entries}`.

  - **Snapshot-based**: Stores current state only. Returns `{:full, state}`
    from `get_state_from/2` since incremental transfer isn't possible.

  Both approaches track pending (uncommitted) mutations separately.
  """

  @type op_number :: non_neg_integer()
  @type mutation :: term()

  # === Committed State ===

  @doc """
  Get full committed state for recovery.
  Returns all committed state in a format suitable for transfer.
  """
  @spec get_state(t) :: term()
  def get_state(store)

  @doc """
  Get state from op_number onwards for incremental transfer.

  Returns:
  - `{:incremental, entries}` - log-based stores return entries from op onwards
  - `{:full, state}` - snapshot-based stores return full state (can't do incremental)
  """
  @spec get_state_from(t, op_number) :: {:incremental, [{op_number, mutation()}]} | {:full, term()}
  def get_state_from(store, from_op)

  @doc """
  Apply received state (full replacement).
  Used when receiving full state from another replica.
  """
  @spec apply_state(t, term()) :: t
  def apply_state(store, state)

  @doc """
  Apply incremental mutations.
  Used when receiving incremental updates from another replica.
  Preserves existing entries and merges new ones.
  """
  @spec apply_mutations(t, [{op_number, mutation()}]) :: t
  def apply_mutations(store, mutations)

  # === Pending Mutations ===

  @doc """
  Get pending (uncommitted) mutations.
  Returns list of `{op_number, mutation}` tuples in order.
  """
  @spec get_pending(t) :: [{op_number, mutation()}]
  def get_pending(store)

  @doc """
  Record a new pending mutation.
  Returns `{:error, :gap}` if op_number would create a gap.
  """
  @spec record_pending(t, op_number, mutation()) :: {:ok, t} | {:error, :gap}
  def record_pending(store, op_number, mutation)

  @doc """
  Commit mutations up to op_number (inclusive).
  Calls the execute function for each mutation being committed.
  Returns the updated store and list of `{op_number, result}` tuples.
  """
  @spec commit_through(t, op_number, (mutation() -> {:ok, result :: term()})) ::
          {:ok, t, [{op_number, result :: term()}]}
  def commit_through(store, op_number, execute_fn)

  # === Metadata ===

  @doc """
  Get the highest op_number (committed + pending).
  Returns 0 if empty.
  """
  @spec op_number(t) :: op_number
  def op_number(store)

  @doc """
  Get the commit_number (highest committed op).
  Returns 0 if nothing committed.
  """
  @spec commit_number(t) :: op_number
  def commit_number(store)

  # === View Number Persistence ===

  @doc """
  Get the persisted view number.
  Required for crash recovery.
  """
  @spec get_view_number(t) :: non_neg_integer()
  def get_view_number(store)

  @doc """
  Persist the view number.
  Required for crash recovery.
  """
  @spec save_view_number(t, non_neg_integer()) :: {:ok, t}
  def save_view_number(store, view_number)
end
