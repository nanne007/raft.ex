defmodule Raft.Log.Memory do
  use GenServer

  def start_link(_server_id) do
    GenServer.start_link(__MODULE__, {})
  end

  def get_last_log_index(pid) do
    pid |> GenServer.call(:get_last_log_index)
  end

  def get_last_log_term(pid) do
    pid |> GenServer.call(:get_last_log_term)
  end

  def sub_log(pid, start_index, len) do
    pid |> GenServer.call({:sub_log, start_index, len})
  end

  def get(pid, index) do
    pid |> GenServer.call({:get, index - 1})
  end
  def append(pid, entries) do
    pid |> GenServer.call({:append, entries})
  end

  def truncate(pid, size) do
    pid |> GenServer.call({:truncate, size})
  end


  @type t :: %__MODULE__{
    entries: list(Raft.Server.entry)
  }
  defstruct [
    entries: []
  ]


  def init({}) do
    {:ok, bootstrap()}
  end

  def handle_call(
    :get_last_log_index, _from,
    %__MODULE__{
      entries: entries
    } = data
  ) do
    {:reply, entries.len, data}
  end


  def handle_call(
    :get_last_log_term, _from,
    %__MODULE__{
      entries: entries
    } = data
  ) do
    last_entry = entries |> Enum.at(length(entries) - 1,
      %Raft.Server.Entry{
        term: 0
      })

    {:reply, last_entry.term, data}
  end

  def handle_call(
    {:sub_log, start_index, len}, _from,
    %__MODULE__{
      entries: entries
    } = data
  ) do
    sub_entries = entries |> Enum.slice(start_index..(start_index + len - 1))
    {:reply, sub_entries, data}
  end


  def handle_call(
    {:append, new_entries}, _from,
    %__MODULE__{
      entries: entries
    } = data
  ) do
    data = %{data |
             entries: entries |> Enum.concat(new_entries)
            }
    {:reply, :ok, data}
  end

  def handle_call(
    {:truncate, size}, _from,
    %__MODULE__{
      entries: entries
    } = data
  ) do
    entries = entries |> Enum.drop(size)
    {:reply, :ok, %{data | entries: entries}}
  end

  def handle_call(
    {:get, index}, _from,
    %__MODULE__{
      entries: entries
    } = data
  ) do
    res = entries |> Enum.fetch(index)
    {:reply, res, data}
  end


  defp bootstrap() do
    %__MODULE__{
      entries: []
    }
  end
end
