defmodule Raft.Consensus do
  require Logger
  use Raft.RPC

  @typedoc """
  state of server
  """
  @type t :: %__MODULE__{
    me: Raft.Server.id,

    ## server vars
    current_term: Raft.Server.rterm,
    voted_for: Raft.Server.id,

    # log vars
    log: pid,
    commit_index: Raft.Server.index,
    last_applied: Raft.Server.index,

    ## candidate vars
    votes_responded: Map.t,
    votes_granted: Map.t,
    voter_log: Mapt.t,

    ## leader vars
    next_indexes: Map.t,
    match_indexes: Map.t,

    timer: reference,
    meta: pid,
    config: pid
  }

  defstruct [
    me: nil,
    current_term: 0,

    voted_for: nil,
    votes_responded: %{},
    votes_granted: %{},
    voter_log: %{},
    log: nil,

    commit_index: 0,
    last_applied: 0,

    next_indexes: %{},
    match_indexes: %{},

    timer: nil,
    meta: nil,
    config: nil
  ]

  @behaviour :gen_fsm

  def start_link(me) do
    :gen_fsm.start_link(__MODULE__, {me})
  end

  def request_vote(pid, params) do
    pid |> :gem_fsm.sync_send_event({:request_vote, params})
  end

  def get_state(pid) do
    pid |> :gem_fsm.sync_send_event(:get_state)
  end

  #### gen_fsm callbacks

  def init({me}) do
    { :ok, :follower, bootstrap(me) }
  end

  def handle_sync_event(:get_state, _from, state_name, state) do
    {:reply, state_name, state_name, state}
  end

  def handle_event(
    %RequestVote{term: term} = vote_req, state_name,
    %__MODULE__{current_term: current_term} = state
  ) when current_term < term do
    state = state |> update_term(term)
    # XXX: just dispatch it
    handle_event(vote_req, :follower, state)
  end
  def handle_event(
    %RequestVote{term: term} = vote_req,
    state_name,
    %__MODULE__{
      current_term: current_term,
      log: log,
      voted_for: voted_for
    } = state
  ) when current_term >= term do
    grant = grant_vote?(state, vote_req)

    if grant do
      state = %{ state | voted_for: vote_req.source }
    end

    reply = %RequestVoteReply{
      term: current_term,
      vote_granted: grant,
      source: vote_req.dest,
      dest: vote_req.source
    }

    Raft.RPC.send_msg(reply.dest, reply)

    {:next_state, state_name, state}
  end




  def follower(:timeout, %__MODULE__{ me: me, current_term: current_term, config: config } = state) do
    {:next_state, :candidate, state |> reset_timer(0) }
  end

  def candidate(:timeout, %__MODULE__{
        current_term: current_term,
        me: me,
        config: config,
        log: log
                } = state) do
    state = %{ state |
               current_term: current_term + 1,
               voted_for: nil,
               votes_responded: %{},
               votes_granted: %{},
               voter_log: %{}
             }

    peers = config |> Raft.Server.Configuration.get_peers()

    last_log_term = log |> Raft.Log.last_log_term()
    last_applied = log |> Raft.Log.last_log_index()
    # send request_vote to all peers
    for peer <- peers do
      request_vote_req = %RequestVote{
        source: me,
        dest: peer,
        term: current_term,
        last_log_index: last_applied,
        last_log_term: last_log_term # TODO: get the last_log_term
      }
      Raft.RPC.send_msg(peer, request_vote_req)
    end
    {:next_state, :candidate, state |> reset_timer(election_timeout())}
  end

  @doc """
  Candidate handles `RequestVoteReply` from voters.
  """
  def candidate(
    %RequestVoteReply{term: term} = vote_reply,
    %__MODULE__{
      current_term: current_term
    } = state) when current_term < term do
    Logger.debug("receive rpc message #{vote_reply}")
    state = state|> update_term(term)
    # Do nothing here
    {:next_state, :follower, state}
  end
  def candidate(
    %RequestVoteReply{term: term} = vote_reply,
    %__MODULE__{
      current_term: current_term
    } = state) when current_term > term do # drop stale response
    Logger.debug("receive stale rpc message #{vote_reply}")
    {:next_state, :candidate, state}
  end
  def candidate(
    %RequestVoteReply{
      term: term,
      vote_granted: granted,
      source: source,
      dest: dest
    } = vote_reply,
    %__MODULE__{
      current_term: current_term,
      votes_responded: votes_responded,
      votes_granted: votes_granted
    } = state) when current_term == term and not granted do
    Logger.debug("receive rpc message #{vote_reply}")
    state = %{state | votes_responded: votes_responded.put(source) }
    {:next_state, :candidate, state}
  end
  def candidate(
    %RequestVoteReply{
      term: term,
      vote_granted: granted,
      source: source,
      dest: dest
    } = vote_reply,
    %__MODULE__{
      current_term: current_term,
      votes_responded: votes_responded,
      votes_granted: votes_granted
    } = state) when current_term == term and granted do
    Logger.debug("receive rpc message #{vote_reply}")
    # deal the granted vote
    state = %{state |
              votes_responded: votes_responded.put(source),
              votes_granted: votes_granted.put(source)
             }

    # try to became leader
    if win_election?(state) do
      # become_leader()
      last_log_index = state.log |> Raft.Log.last_log_index()
      peers = state.config |> Raft.Server.Configuration.get_peers()
      state = %{state |
                next_indexes: peers |> Enum.map(fn peer ->
                  {peer, last_log_index + 1}
                end) |> Map.new(),
                match_indexes: peers |> Enum.map(fn peer ->
                  {peer, 0}
                end) |> Map.new
               }
      {:next_state, :leader, state}
    else
      {:next_state, :candidate, state}
    end
  end

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
               log: log,
               config: config
             } = state) do
    heartbeat_msg = %AppendEntries{
      term: current_term,
      leader_id: me,
      prev_log_index: last_applied_index,
      prev_log_term: Raft.Log.last_log_term(log),
      entries: [],
      leader_commit: commit_index,
    }
    peers = config |> Raft.Server.Configuration.get_peers()
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
    } |> reset_timer(election_timeout())

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

  defp grant_vote?(
    %__MODULE__{
      current_term: current_term
    } = state,
    %RequestVote{
      term: term
    } = vote_req
  ) when current_term > term do
    false
  end
  defp grant_vote?(
    %__MODULE__{
      current_term: current_term,
      voted_for: voted_for,
      log: log
    } = state,
    %RequestVote{
      term: term
    } = vote_req
  ) when current_term == term do
    last_term = log |> Raft.Log.last_log_term()
    last_index = log |> Raft.Log.last_log_index()

    log_ok = cond do
      vote_req.last_log_term > last_term ->
        true
      vote_req.last_log_term == last_term ->
        vote_req.last_log_index >= last_index
      true ->
        false
    end

    log_ok && [vote_req.source, nil] |> Enum.member?(voted_for)
  end


  def win_election?(%__MODULE__{
        votes_granted: votes_granted,
        config: config
                    } = state) do
    peers = config |> Raft.Server.Configuration.get_peers()
    (MapSet.size(votes_granted) + 1) * 2 > length(peers) + 1
  end
end
