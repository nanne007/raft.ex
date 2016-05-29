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
    me: id,
    current_term: rterm,
    voted_for: id,
    logs: list(entry),

    commit_index: index,
    last_applied_index: index,

    next_indexes: list(index),
    match_indexes: list(index),

    timer: reference,
    meta: pid,
    config: pid
  }

  defstruct [
    me: nil,
    current_term: 0,
    voted_for: nil,
    logs: [],

    commit_index: 0,
    last_applied_index: 0,

    next_indexes: [],
    match_indexes: [],

    timer: nil,
    meta: nil,
    config: nil
  ]

  @behaviour :gen_fsm

  def start_link(me) do
    :gen_fsm.start_link(__MODULE__, {me})
  end

  def init({me}) do
    { :ok, :follower, bootstrap(me) }
  end


  def follower(:timeout,
               %__MODULE__{
                 me: me,
                 current_term: current_term
               } = state) do
    state = %{
      state |
      current_term: current_term + 1,
      voted_for: me # TODO: change to server id
    } |> reset_timer()

    # TODO: send request_vote to all other servers

    {:next_state, :candidate, state}
  end

  def candidate() do
  end

  defp bootstrap(me) do
    {:ok, meta} = Raft.Server.Meta.start_link(me)
    {:ok, config} = Raft.Server.Configuration.start_link(me)
    state = %__MODULE__{
      me: me,
      meta: meta,
      config: config,

      current_term: meta |> Raft.Server.Meta.get_current_term(),
      voted_for: meta |> Raft.Server.Meta.get_voted_for()
    } |> reset_timer()

    state
  end

  defp reset_timer(%__MODULE__{timer: timer} = state) do
    unless is_nil(timer) do
      timer |> :gen_fsm.cancel_timer()
    end

    new_timer = :gen_fsm.send_event_after(election_timeout(), :timeout)

    %{state | timer: new_timer}
  end

  @election_timeout_min 500
  @election_timeout_max 1000
  defp election_timeout() do
    min_timeout = Application.get_env(:raft, :election_timeout_min, @election_timeout_min)
    max_timeout = Application.get_env(:raft, :election_timeout_max, @election_timeout_max)
    :crypto.rand_uniform min_timeout, max_timeout + 1
  end
end
