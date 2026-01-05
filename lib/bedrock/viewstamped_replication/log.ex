defprotocol Bedrock.ViewstampedReplication.Log do
  @moduledoc """
  Protocol for log operations in Viewstamped Replication.

  The log stores client operations assigned op-numbers by the primary.
  Unlike viewstamped_replication, VR doesn't store terms in log entries since view changes
  use a different mechanism for log selection.
  """

  @type op_number :: non_neg_integer()
  @type entry :: {client_id :: term(), request_number :: non_neg_integer(), operation :: term()}

  @doc """
  Append an entry at the given op number.
  Returns {:error, :gap} if there would be a gap in the log.
  """
  @spec append(t, op_number, entry) :: {:ok, t} | {:error, :gap}
  def append(log, op_number, entry)

  @doc """
  Get the entry at the given op number, or nil if not present.
  """
  @spec get(t, op_number) :: entry | nil
  def get(log, op_number)

  @doc """
  Get all entries from the given op number (inclusive) to the end.
  """
  @spec entries_from(t, op_number) :: [{op_number, entry}]
  def entries_from(log, op_number)

  @doc """
  Get all entries from the beginning up to the given op number (inclusive).
  """
  @spec entries_to(t, op_number) :: [{op_number, entry}]
  def entries_to(log, op_number)

  @doc """
  Get the highest op number in the log, or 0 if empty.
  """
  @spec newest_op_number(t) :: op_number
  def newest_op_number(log)

  @doc """
  Remove all entries after the given op number.
  """
  @spec truncate_after(t, op_number) :: t
  def truncate_after(log, op_number)

  @doc """
  Get all entries as a list for view change log transfer.
  """
  @spec to_list(t) :: [{op_number, entry}]
  def to_list(log)

  @doc """
  Create a log from a list of entries (for STARTVIEW processing).
  """
  @spec from_list(t, [{op_number, entry}]) :: t
  def from_list(log, entries)

  @doc """
  Append multiple entries to the log efficiently.
  Preserves existing entries and merges new ones.
  More efficient than `from_list(log, to_list(log) ++ entries)`.
  """
  @spec append_entries(t, [{op_number, entry}]) :: t
  def append_entries(log, entries)

  @doc """
  Get the current view number from persistent storage.
  """
  @spec current_view_number(t) :: non_neg_integer()
  def current_view_number(log)

  @doc """
  Save the current view number to persistent storage.
  Required for crash recovery.
  """
  @spec save_current_view_number(t, non_neg_integer()) :: {:ok, t}
  def save_current_view_number(log, view_number)
end
