defmodule Raft.Messages do
  @moduledoc false

  defmodule RaftMessage do
    defstruct [from: nil, to: nil, term: 0, msg: nil]
  end

  defmodule RequestVote do
    defstruct [from: nil, to: nil, term: 0, last_log_index: 0, last_log_term: 0]
  end

  defmodule RequestVoteResp do
    defstruct [from: nil, to: nil, term: 0, vote_granted: true]
  end
end
