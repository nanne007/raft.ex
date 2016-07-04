defmodule Raft.RPC do
  defmacro __using__(_opts) do
    quote do
      alias unquote(__MODULE__).AppendEntries
      alias unquote(__MODULE__).AppendEntriesReply
      alias unquote(__MODULE__).RequestVote
      alias unquote(__MODULE__).RequestVoteReply
    end
  end

  defmodule RequestVote do
    defstruct [
      :source,
      :dest,
      :term,
      :last_log_index,
      :last_log_term
    ]

    @type t :: %__MODULE__{
      source: Raft.Supervisor.id,
      dest: Raft.Supervisor.id,
      term: non_neg_integer,
      last_log_index: non_neg_integer,
      last_log_term: non_neg_integer
    }
  end

  defmodule RequestVoteReply do
    defstruct [
      :source,
      :dest,
      :term,
      :vote_granted
    ]
    @type t :: %__MODULE__{
      source: Raft.Supervisor.id,
      dest: Raft.Supervisor.id,
      term: Raft.Supervisor.rterm,
      vote_granted: boolean
    }
  end

  defmodule AppendEntries do
    defstruct [
      :term,
      :source,
      :dest,

      :prev_log_index,
      :prev_log_term,
      :entries,
      :leader_commit
    ]

    @type t :: %__MODULE__{
      term: Raft.Supervisor.rterm,
      source: Raft.Supervisor.id,
      dest: Raft.Supervisor.id,

      prev_log_index: Raft.Supervisor.index,
      prev_log_term: Raft.Supervisor.rterm,

      entries: list(term),
      leader_commit: Raft.Supervisor.index
    }
  end

  defmodule AppendEntriesReply do
    defstruct [
      :term,
      :source,
      :dest,
      :success,
      :match_index
    ]

    @type t :: %__MODULE__{
      term: Raft.Supervisor.rterm,
      source: Raft.Supervisor.id,
      dest: Raft.Supervisor.id,

      success: boolean,
      match_index: Raft.Supervisor.index
    }
  end


  @doc """
  send message to peer, four message types:
  1. RequestVote
  2. RequestVoteReply
  3. AppendEntries
  4. AppendEntriesReply
  """
  def send_msg(peer, message) do
    {name, node} = peer
    args = [name, message]
    :rpc.cast(node, Raft.RPC, :handle_msg, args)
  end

  def handle_msg(name, message) do
    name |> Raft.Supervisor.get_consensus() |> :gen_statem.cast(message)
  end
end
