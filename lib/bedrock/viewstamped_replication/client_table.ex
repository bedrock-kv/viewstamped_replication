defmodule Bedrock.ViewstampedReplication.ClientTable do
  @moduledoc """
  Client request tracking for Viewstamped Replication.

  The client table tracks the most recent request from each client along
  with its cached response. This enables:

  1. Duplicate detection: If a client resends a request we're still processing,
     we can recognize it and avoid double-execution.

  2. At-most-once semantics: If a client resends a request we've already
     completed, we can return the cached result without re-executing.

  Per the VR paper, clients include a monotonically increasing request number
  with each request. The client table stores the highest request number seen
  from each client and, if completed, the result.
  """

  @type client_id :: term()
  @type request_number :: non_neg_integer()
  @type result :: term()
  @type entry :: {request_number, :pending | {:completed, result}}

  @type t :: %__MODULE__{
          table: %{client_id => entry}
        }

  defstruct table: %{}

  @doc """
  Create a new empty client table.
  """
  @spec new() :: t()
  def new, do: %__MODULE__{table: %{}}

  @doc """
  Look up the current state for a client.
  Returns {request_number, :pending | {:completed, result}} or nil.
  """
  @spec lookup(t(), client_id) :: entry | nil
  def lookup(%__MODULE__{table: table}, client_id), do: Map.get(table, client_id)

  @doc """
  Check if a request is new, a duplicate of a pending request, or has
  a cached result.

  Returns:
  - :new - This is a new request that should be processed
  - :duplicate - This request is already being processed (drop it)
  - {:cached, result} - This request was already completed, return cached result
  """
  @spec check_request(t(), client_id, request_number) :: :new | :duplicate | {:cached, result}
  def check_request(%__MODULE__{table: table}, client_id, request_number) do
    case Map.get(table, client_id) do
      nil ->
        :new

      {stored_request_num, _state} when request_number > stored_request_num ->
        # New request with higher number
        :new

      {stored_request_num, :pending} when request_number == stored_request_num ->
        # Same request, still pending
        :duplicate

      {stored_request_num, {:completed, result}} when request_number == stored_request_num ->
        # Same request, already completed
        {:cached, result}

      {stored_request_num, _state} when request_number < stored_request_num ->
        # Old request number, client must have moved on
        # Treat as duplicate to drop it
        :duplicate
    end
  end

  @doc """
  Record that we've started processing a request.
  """
  @spec record_pending(t(), client_id, request_number) :: t()
  def record_pending(%__MODULE__{table: table} = ct, client_id, request_number),
    do: %{ct | table: Map.put(table, client_id, {request_number, :pending})}

  @doc """
  Record the result of a completed request.
  """
  @spec record_result(t(), client_id, request_number, result) :: t()
  def record_result(%__MODULE__{table: table} = ct, client_id, request_number, result),
    do: %{ct | table: Map.put(table, client_id, {request_number, {:completed, result}})}

  @doc """
  Get all entries for log/state transfer during view change.
  """
  @spec entries(t()) :: [{client_id, entry}]
  def entries(%__MODULE__{table: table}), do: Map.to_list(table)

  @doc """
  Create a client table from a list of entries.
  """
  @spec from_entries([{client_id, entry}]) :: t()
  def from_entries(entries), do: %__MODULE__{table: Map.new(entries)}
end
