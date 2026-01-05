defmodule Bedrock.ViewstampedReplication.Log.InMemoryLog do
  @moduledoc """
  An in-memory implementation of the VR Log protocol.

  Uses a map for O(1) access to entries by op number. This implementation
  is suitable for testing and development; production use would require
  a persistent backing store.
  """

  alias Bedrock.ViewstampedReplication.Log

  @type t :: %__MODULE__{
          entries: %{Log.op_number() => Log.entry()},
          newest_op_number: Log.op_number(),
          current_view_number: non_neg_integer()
        }

  defstruct entries: %{},
            newest_op_number: 0,
            current_view_number: 0

  @doc """
  Create a new empty in-memory log.
  """
  @spec new() :: t()
  def new, do: %__MODULE__{}

  @doc """
  Create a new in-memory log with a specified initial view number.
  """
  @spec new(view_number :: non_neg_integer()) :: t()
  def new(view_number), do: %__MODULE__{current_view_number: view_number}

  defimpl Log do
    def append(log, op_number, entry) do
      if op_number == log.newest_op_number + 1 do
        new_entries = Map.put(log.entries, op_number, entry)
        {:ok, %{log | entries: new_entries, newest_op_number: op_number}}
      else
        {:error, :gap}
      end
    end

    def get(log, op_number), do: Map.get(log.entries, op_number)

    def entries_from(log, op_number) do
      log.entries
      |> Enum.filter(fn {n, _entry} -> n >= op_number end)
      |> Enum.sort_by(fn {n, _entry} -> n end)
    end

    def entries_to(log, op_number) do
      log.entries
      |> Enum.filter(fn {n, _entry} -> n <= op_number end)
      |> Enum.sort_by(fn {n, _entry} -> n end)
    end

    def newest_op_number(log), do: log.newest_op_number

    def truncate_after(log, op_number) do
      new_entries =
        log.entries
        |> Enum.filter(fn {n, _entry} -> n <= op_number end)
        |> Map.new()

      new_newest = if map_size(new_entries) == 0, do: 0, else: op_number

      %{log | entries: new_entries, newest_op_number: new_newest}
    end

    def to_list(log) do
      log.entries
      |> Enum.sort_by(fn {n, _entry} -> n end)
    end

    def from_list(log, entries) do
      new_entries = Map.new(entries)

      new_newest =
        if map_size(new_entries) == 0 do
          0
        else
          Enum.reduce(entries, 0, fn {n, _}, acc -> max(n, acc) end)
        end

      %{log | entries: new_entries, newest_op_number: new_newest}
    end

    # O(m) where m = length(entries), instead of O(n + m) for to_list ++ from_list
    def append_entries(log, []), do: log

    def append_entries(log, entries) do
      new_entries =
        Enum.reduce(entries, log.entries, fn {n, entry}, acc ->
          Map.put(acc, n, entry)
        end)

      new_newest =
        Enum.reduce(entries, log.newest_op_number, fn {n, _}, acc ->
          max(n, acc)
        end)

      %{log | entries: new_entries, newest_op_number: new_newest}
    end

    def current_view_number(log), do: log.current_view_number

    def save_current_view_number(log, view_number),
      do: {:ok, %{log | current_view_number: view_number}}
  end
end
