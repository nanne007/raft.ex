defmodule Raft.Server.Meta do

  @type t :: %__MODULE__{
    current_term: Raft.Server.rterm,
    voted_for: Raft.Server.id
  }
  defstruct [
    current_term: nil,
    voted_for: nil
  ]

  use GenServer

  def start_link(me) do
    GenServer.start_link(__MODULE__, {me})
  end

  def get_current_term(pid) do
    pid |> GenServer.call(:get_current_term)
  end

  def get_voted_for(pid) do
    pid |> GenServer.call(:get_voted_for)
  end
  ### GenServer callbacks

  def init({me}) do
    state = %__MODULE__{
      current_term: 0,
      voted_for: nil
    }
    {:ok, state}
  end

  def handle_call(:get_current_term, _from,
                  %__MODULE__{
                    current_term: current_term
                  } = state) do
    {:reply, current_term, state}
  end

  def handle_call(:get_voted_for, _from,
                  %__MODULE__{
                    voted_for: voted_for
                  } = state) do
    {:reply, voted_for, state}
  end

end
