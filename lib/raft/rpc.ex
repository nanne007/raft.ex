defmodule Raft.RPC do
  defmodule RequestVote do
    defstruct [
      :term,
      :candidate_id,
      :last_log_index,
      :last_log_term
    ]

    @type t :: %__MODULE__{
      term: non_neg_integer,
      candidate_id: Raft.Server.id,
      last_log_index: non_neg_integer,
      last_log_term: non_neg_integer
    }
  end

  defmodule RequestVoteResult do
    defstruct [
      :term,
      :vote_granted
    ]
    @type t :: %__MODULE__{
      term: non_neg_integer,
      vote_granted: boolean
    }
  end

  defmodule AppendEntry do
    defstruct [
      :term,
      :leader_id,
      :prev_log_index,
      :prev_log_term,
      :entries,
      :leader_commit_index
    ]

    @type t :: %__MODULE__{
      term: non_neg_integer,
      leader_id: Raft.Server.id,
      prev_log_index: non_neg_integer,
      prev_log_term: non_neg_integer,
      entries: list(term),
      leader_commit_index: non_neg_integer
    }
  end

  defmodule AppendEntryResult do
    defstruct [:term, :success]

    @type t :: %__MODULE__{
      term: non_neg_integer,
      success: boolean
    }
  end
end
