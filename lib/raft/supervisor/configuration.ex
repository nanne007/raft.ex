defmodule Raft.Supervisor.Configuration do


  @type t :: %__MODULE__{
    me: Raft.Supervisor.id,
    members: list(term)
  }
  defstruct [
    me: nil,
    members: []
  ]

  use GenServer

  def start_link(me, peers) do
    # TODO: should read configuration from file
    # For now, just in memory.
    GenServer.start_link(__MODULE__, {me, peers})
  end

  def get_peers(pid) do
    pid |> GenServer.call(:get_peers)
  end

  def set_peers(pid, peers) do
    pid |> GenServer.call({:set_peers, peers})
  end

  ### GenServer callbacks

  def init({me, peers}) do
    state = %__MODULE__{
      me: me,
      members: peers
    }
    {:ok, state}
  end

  def handle_call(:get_peers, _from,
                  %__MODULE__{
                    members: members
                  }=state) do
    {:reply, members, state}
  end

  def handle_call({:set_peers, new_peers}, _from,
                  %__MODULE__{
                  } = state) do
    # TODO: save to disk
    state = %{
      state | members: new_peers
    }
    {:reply, :ok, state}
  end
end
