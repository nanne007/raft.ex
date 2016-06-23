defmodule Raft.Consensus do
  require Logger
  use Raft.RPC

  @typedoc """
  state of server
  """
  @type t :: %__MODULE__{
    me: Raft.Server.id,
    current_term: Raft.Server.rterm,

    voted_for: Raft.Server.id,
    votes_responded: Map.t,
    votes_granted: Map.t,
    voter_log: Mapt.t,

    log: pid,

    commit_index: Raft.Server.index,
    last_applied: Raft.Server.index,

    next_indexes: list(Raft.Server.index),
    match_indexes: list(Raft.Server.index),

    timer: reference,
    meta: pid,
    config: pid,

    election: integer
  }

  defstruct [
    me: nil,
    current_term: 0,

    voted_for: nil,
    votes_responded: %{},
    votes_granted: %{},
    voter_log: %{}
    log: nil,

    commit_index: 0,
    last_applied: 0,

    next_indexes: [],
    match_indexes: [],

    timer: nil,
    meta: nil,
    config: nil,
    election: 0
  ]

  defmodule CandidateState do
    defstruct [
      votes: %{}
    ]
  end

  @behaviour :gen_fsm

  def start_link(me) do
    :gen_fsm.start_link(__MODULE__, {me})
  end

  def request_vote(pid, params) do
    pid |> :gem_fsm.sync_send_event({:request_vote, params})
  end

  def init({me}) do
    { :ok, :follower, bootstrap(me) }
  end



  #### gen_fsm callbacks

  def handle_sync_event(%RequestVote{} = vote_req, _from, state_name, state) do
    # Any RPC with a newer term causes the recipient to advance its term,
    # and transite to follower.
    if current_term < vote_req.term do
      state = state |> update_term(vote_req.term)
      state_name = :follower
    end

    ## Handle RequestVote Request
    {reply, state} = state |> handle_request_vote_request(vote_req)

    {:reply, reply, state_name, state}
  end


  def follower(:timeout, %__MODULE__{ me: me, current_term: current_term, config: config } = state) do
    {:next_state, :candidate, state |> reset_timer(0) }
  end

  def candidate(:timeout, %__MODULE__{} = state) do
    state = %{ state |
               current_term: current_term + 1,
               voted_for: nil,
               votes_responded: %{},
               votes_granted: %{},
               voter_log: %{}
             }

    peers = config |> Raft.Server.Configuration.get_peers()

    # send request_vote to all peers
    for perr <- peers do
      request_vote_req = %RequestVote{
        term: current_term,
        candidate_id: me,
        last_log_index: last_applied,
        last_log_term: last_log_term # TODO: get the last_log_term
      }
      Raft.RPC.send_msg(peer, request_vote_req)
    end
    {:next_state, :candidate, state |> reset_timer(election_timeout())}
  end

  # TODO: implement vote reply handling.
  # def candidate(
  #   %RequestVoteReply{} = vote_reply,
  #   %__MODULE__{
  #     current_term: current_term
  #   } = state) do
  #   if vote_reply.term > current_term do
  #     state = state |> update_term(vote_reply.term)
  #     next_state_name = :follower
  #   end
  #   Logger.debug("receive rpc message #{vote_reply}")
  #   if vote_reply.term < current_term do
  #     # drop stale response
  #     {:next_state, :candidate, state}
  #   else

  #   end
  # end

  def candidate(
    %AppendEntries{
      term: term,
      leader_id: leader,
      prev_log_index: prev_log_index,
      prev_log_term: prev_log_term,
      entries: entries,
      leader_commit: leader_commit
    } = message, %__MODULE__{
      me: me,
      current_term: current_term,
      voted_for: voted_for,
      log: log,
      config: config
    } = state) do
    Logger.debug("receive message #{message}")

    reply = %AppendEntriesReply{
      term: current_term
    }
    # if term < current_term do
    #   reply = %{reply | success: false}
    # else if term < Raft.Log.get_last_log_term(log) do
    # else

    # end
  end


  def leader({:timeout}, %__MODULE__{
               me: me,
               current_term: current_term,
               commit_index: commit_index,
               last_applied: last_applied_index,
               log: log
             } = state) do
    heartbeat_msg = %AppendEntries{
      term: current_term,
      leader_id: me,
      prev_log_index: last_applied_index,
      prev_log_term: Raft.Log.last_log_term(log),
      entries: [],
      leader_commit: commit_index,
    }
    for peer <- peers do
      # TODO: implement me
      Raft.RPC.send(peer, heartbeat_msg)
    end
    state = state |> reset_timer(heartbeat_timeout())
    {:next_state, :leader, state}
  end
  def leader({:command, command}, %__MODULE__{

             } = state) do

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



  defp reset_timer(%__MODULE__{timer: timer} = state, timeout) do
    unless is_nil(timer) do
      timer |> :gen_fsm.cancel_timer()
    end

    new_timer = :gen_fsm.send_event_after(timeout, :timeout)

    %{state | timer: new_timer}
  end

  @heartbeat_timeout_min 50
  @heartbeat_timeout_max 200
  defp heartbeat_timeout() do
    min_timeout = Application.get_env(:raft, :heartbeat_timeout_min, @heartbeat_timeout_min)
    max_timeout = Application.get_env(:raft, :heartbeat_timeout_max, @heartbeat_timeout_max)
    :crypto.rand_uniform min_timeout, max_timeout + 1
  end

  @election_timeout_min 500
  @election_timeout_max 1000
  defp election_timeout() do
    min_timeout = Application.get_env(:raft, :election_timeout_min, @election_timeout_min)
    max_timeout = Application.get_env(:raft, :election_timeout_max, @election_timeout_max)
    :crypto.rand_uniform min_timeout, max_timeout + 1
  end

  def peers(%__MODULE__{ config: config } = _state) do
    config |> Raft.Server.Configuration.get_peers()
  end

  # Any RPC with a newer term causes the recipient to advance its term first.
  # and transit to follower.
  defp update_term(%__MODULE__{} = state, term) do
    if state.current_term < term do
      %{state |
        current_term: term,
        voted_for: nil
      }
    else
      state
    end
  end

  # After update term, handle request.
  defp handle_request_vote_request(%__MODULE__{ log: log } = state, vote_req) do
    last_term = log |> Raft.Log.last_log_term()
    last_index = log |> Raft.Log.last_log_index()

    log_ok = cond do
      vote_req.last_log_term > last_term ->
        true
      vote_req.last_log_term == last_term ->
        vote_req.last_log_index >= last_index
      _ ->
        false
    end

    grant = vote_req.term == current_term and log_ok and (is_nil(voted_for) or voted_for == vote_req.source)

    if grant do
      state = %{ state | voted_for: vote_req.source }
    end

    reply = %RequestVoteReply{
      term: current_term,
      vote_granted: grant,
      source: vote_req.dest,
      dest: vote_req.source
    }
    {reply, state}
  end

end
