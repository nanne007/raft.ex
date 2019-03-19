defmodule Raft.Messages do
  @moduledoc false

  defmacro __using__(opts) do
    quote do
      alias Raft.Messages.RaftMessage

      alias Raft.Messages.RequestVote
      alias Raft.Messages.RequestVoteResp

      alias Raft.Messages.RequestPreVote
      alias Raft.Messages.RequestPreVoteResp

      alias Raft.Messages.AppendEntries
      alias Raft.Messages.AppendEntriesResp

      alias Raft.Messages.Entry
    end
  end

  defmodule RaftMessage do
    defstruct from: nil, to: nil, term: 0, msg: nil
  end

  defmodule RequestPreVote do
    defstruct last_log_index: 0, last_log_term: 0
  end

  defmodule RequestPreVoteResp do
    defstruct vote_granted: true
  end

  defmodule RequestVote do
    defstruct last_log_index: 0, last_log_term: 0
  end

  defmodule RequestVoteResp do
    defstruct vote_granted: true
  end

  defmodule AppendEntries do
    defstruct leader_id: 0,
              leader_commit: 0,
              prev_log_index: 0,
              prev_log_term: 0,
              entries: []
  end

  defmodule AppendEntriesResp do
    defstruct success: false
  end

  defmodule Entry do
    defstruct term: 0,
      index: 0,
      type: :normal
      data: nil
  end
end
