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
      source: Raft.Server.id,
      dest: Raft.Server.id,
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
      source: Raft.Server.id,
      dest: Raft.Server.id,
      term: Raft.Server.rterm,
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
      term: Raft.Server.rterm,
      source: Raft.Server.id,
      dest: Raft.Server.id,

      prev_log_index: Raft.Server.index,
      prev_log_term: Raft.Server.rterm,

      entries: list(term),
      leader_commit: Raft.Server.index
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
      term: Raft.Server.rterm,
      source: Raft.Server.id,
      dest: Raft.Server.id,

      success: boolean,
      match_index: Raft.Server.index
    }
  end

  def send_msg(peer, %RequestVote{} = message) do
    {name, node} = peer
    args = [name, message]
    :rpc.cast(node, Raft.RPC, :handle_msg, args)
  end

  def send_msg(peer, %RequestVoteReply{} = message) do
    {name, node} = peer
    args = [name, message]
    :rpc.cast(node, Raft.RPC, :handle_msg, args)
  end

  def handle_msg(name, %RequestVote{} = message) do
    name |> Raft.Server.get_consensus() |> :gem_fsm.send_all_state_event(message)
  end
  def handle_msg(name, %RequestVoteReply{} = message) do
    name |> Raft.Server.get_consensus() |> :gem_fsm.send_all_state_event(message)
  end

end
