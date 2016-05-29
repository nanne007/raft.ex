defmodule Raft.Server.Configuration do

  @typep peer :: term

  defstruct [
    id: nil,
    term: 0,
    index: 0,
    members: %{}
  ]
  @type t :: %__MODULE__{
    id: term,
    term: non_neg_integer,
    index: non_neg_integer,
    members: list(term)
  }

  use GenServer

  def start_link(me) do
    # TODO: should read configuration from file
    # For now, just in memory.
    GenServer.start_link(__MODULE__, {me})
  end

  def get_peers(pid) do
    pid |> GenServer.call(:get_peers)
  end

  def set_peers(pid, peers) do
    pid |> GenServer.call({:set_peers, peers})
  end

  ### GenServer callbacks

  def init({me}) do
    state = %__MODULE__{
      id: me,
      term: 0,
      index: 0,
      members: [me]
    }
    {:ok, state}
  end

  def handle_call(:get_peers, _from,
                  %__MODULE__{
                    id: id,
                    members: members
                  }=state) do
    peers = members |> List.delete(id)
    {:reply, peers, state}
  end

  def handle_call({:set_peers, new_peers}, _from,
                  %__MODULE__{
                    id: id,
                    members: members
                  } = state) do
    # TODO: save to disk
    state = %{
      state | members: [id | new_peers]
    }
    {:reply, :ok, state}
  end
end
