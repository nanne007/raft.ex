defmodule Raft.Server do
  @typedoc """
  server identity.
  """
  @type id :: term

  @typedoc """
  number of leader's term of office
  """
  @type rterm :: non_neg_integer

  @typedoc """
  index of log
  """
  @type index :: non_neg_integer

  @typedoc """
  entry of log
  """
  @type entry :: {rterm, term}

  @typedoc """
  state of server
  """
  @type t :: %__MODULE__{
    current_term: rterm,
    voted_for: id,
    logs: list(entry),

    commit_index: index,
    last_applied_index: index,

    next_indexes: list(index),
    match_indexes: list(index)
  }

  defstruct [
    current_term: 0,
    voted_for: nil,
    logs: [],

    commit_index: 0,
    last_applied_index: 0,

    next_indexes: [],
    match_indexes: []
  ]

  @behaviour :gen_fsm

  def start_link do
    :gen_fsm.start_link()
  end

  def init([]) do
    { :ok, :follower, bootstrap() }

  end


  defp bootstrap do
    state = %__MODULE__{

    }
  end

  def follower(event, state) do
  end


end
