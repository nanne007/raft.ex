defmodule Raft.Consensus do
  require Logger
  use Raft.RPC


  @typedoc """
  state of server
  """
  @type t :: %__MODULE__{
    me: Raft.Supervisor.id,

    ## server vars
    current_term: Raft.Supervisor.rterm,
    voted_for: Raft.Supervisor.id,

    # log vars
    log: pid,
    commit_index: Raft.Supervisor.index,
    last_applied: Raft.Supervisor.index,

    ## candidate vars
    votes_responded: MapSet.t,
    votes_granted: MapSet.t,
    voter_log: Mapt.t,

    ## leader vars
    next_indexes: Map.t,
    match_indexes: Map.t,

    meta: pid,
    config: pid,

    timer: reference
  }

  defstruct [
    me: nil,
    # server vars
    current_term: 0,
    voted_for: nil,

    # log vars
    log: nil,
    commit_index: 0,
    last_applied: 0,

    # candidate vars
    votes_responded: MapSet.new,
    votes_granted: MapSet.new,
    voter_log: %{},

    # leader vars
    next_indexes: %{},
    match_indexes: %{},

    meta: nil,
    config: nil,

    timer: nil
  ]

  @behaviour :gen_statem
  def callback_mode(), do: :handle_event_function

  def start_link(me, peers) do
    :gen_statem.start_link(__MODULE__, {me, peers}, [])
  end

  #### gen_statem callbacks

  def init({me, peers}) do
    {callback_mode, :follower, bootstrap(me, peers), {:next_event, :internal, :became_follower}}
  end

  def code_change(_old_vsn, old_state, old_data, _extra) do
    {callback_mode(), old_state, old_data}
  end

  def terminate(_reason, _state, _data) do
    :void
  end

  #### Follower State ####


  # Guard: Update Term
  # if message's term is greater than current term,
  # update the term, and transite to :follower. and re-play the request.
  def handle_event(
    :cast, %{
      term: term
    } = rpc,
    :follower, %__MODULE__{
      current_term: current_term
    } = data
  ) when current_term < term do
    data = data |> update_term(term)
    {:keep_state, data, {:next_event, :internal, rpc}}
  end

  # Event: becaome_follower.
  # every trasistion to :follower should go through this to set timer.
  def handle_event(
    :internal, :became_follower,
    :follower, %__MODULE__{} = data
  ) do
    data = data |> reset_timer(:election)
    {:keep_state, data}
  end

  # Event: RequestVote.
  def handle_event(
    :cast, %RequestVote{
      term: term
    } = request_vote_req,
    :follower, %__MODULE__{
      current_term: current_term
    } = data
  ) when current_term > term do
    # reject the request_vote
    data |> reject_request_vote(request_vote_req)
    :keep_state_and_data
  end
  def handle_event(
    event_type,
    %RequestVote{
      term: term
    } = request_vote_req,
    :follower,
    %__MODULE__{
      current_term: current_term,
      log: log,
      voted_for: voted_for
    } = data
  ) when current_term == term and event_type in [:cast, :internal] do
    grant = log_ok?(log, request_vote_req) && (voted_for in [nil, request_vote_req.source])


    data = if grant do
      # the election timer should be reset after every valid rpc call.
      data
      |> accept_request_vote(request_vote_req)
      |> reset_timer(:election)
    else
      _ = data |> reject_request_vote(request_vote_req)
      data
    end
    {:keep_state, data}
  end

  # Event: AppendEntries
  def handle_event(
    :cast = _event_type,
    %AppendEntries{
      term: term,
    } = append_entries_req,
    :follower,
    %__MODULE__{
      current_term: current_term
    } = data
  ) when current_term > term do
    data |> refuse_append_entries(append_entries_req)
    :keep_state_and_data
  end
  def handle_event(
    event_type,
    %AppendEntries{
      term: term
    } = append_entries_req,
    :follower,
    %__MODULE__{
      current_term: current_term,
      log: log
    } = data
  ) when current_term == term and (event_type in [:cast, :internal]) do
    # handle the append_entries request based on the log_ok.
    log_ok = log |> log_ok?(append_entries_req)

    data = if log_ok do
      # accept_request
      data
      |> accept_append_entries(append_entries_req)
      |> reset_timer(:election)
    else
      # refuse request
      _ = data |> refuse_append_entries(append_entries_req)
      data
    end
    {:keep_state, data}
  end

  # Event: :election_timeout.
  def handle_event(
    :info = _event_type,
    :election_timeout = _event_content,
    :follower,
    %__MODULE__{
      current_term: current_term
    } = data
  ) do
    {:next_state, :candidate,
     %{data |
       current_term: current_term + 1
     },
     {:next_event, :internal, :became_candidate}
    }
  end

  def handle_event(
    _event_type, _event_content,
    :follower, _data
  ) do
    :keep_state_and_data
  end



  #### Candidate State ####


  # Guard: Update Term
  # if message's term is greater than current term,
  # update the term, and transite to :follower, and re-apply the request.
  def handle_event(
    :cast, %{
      term: term
    } = rpc,
    :candidate, %__MODULE__{
      current_term: current_term
    } = data
  ) when current_term < term do
    data = data |> update_term(term)
    replay_request = case rpc do
                       %RequestVote{}   -> true
                       %AppendEntries{} -> true
                       _                -> false
                     end
    actions = if replay_request do
      [
        {:next_event, :internal, rpc}, # replay the request
        {:next_event, :internal, :became_follower}  # and then became follower
      ]
    else
      [
        {:next_event, :internal, :became_follower}  # and then became follower
      ]
    end
    {:next_state, :follower, data, actions}
  end

  # Event: election_timeout
  # prev leader_election failed, start a new one.
  def handle_event(
    :info = _event_type,
    :election_timeout = _event_content,
    :candidate,
    %__MODULE__{
      current_term: current_term
    } = data
  ) do
    {:keep_state,
     %{data |
       current_term: current_term + 1
     },
     {:next_event, :internal, :became_candidate}
    }
  end

  def handle_event(
    :internal, :became_candidate,
    :candidate, %__MODULE__{
      me: me,
      current_term: current_term,
      config: config,
      log: log
    } = data
  ) do
    data = %{data |
             voted_for: me, # vote for myself
             votes_responded: MapSet.new,
             votes_granted: MapSet.new,
             voter_log: %{}
            } |> reset_timer(:election)
    # send request_vote rpc to all peers
    peers = config |> Raft.Supervisor.Configuration.get_peers()

    last_log_term = log |> Raft.Log.Memory.get_last_log_term()
    last_log_index = log |> Raft.Log.Memory.get_last_log_index()
    for peer <- peers do
      request_vote_req = %RequestVote{
        source: me,
        dest: peer,
        term: current_term,
        last_log_index: last_log_index,
        last_log_term: last_log_term
      }
      Raft.RPC.send_msg(peer, request_vote_req)
    end
    {:keep_state, data}
  end


  # Event: RequestVote
  def handle_event(
    :cast, %RequestVote{
      term: term
    } = request_vote_req,
    :candidate, %__MODULE__{
      current_term: current_term
    } = data
  ) when current_term >= term do
    # just reject the request_vote
    data |> reject_request_vote(request_vote_req)
    :keep_state_and_data
  end

  # Event: AppendEntries
  def handle_event(
    :cast = _event_type, %AppendEntries{
      term: term
    } = append_entries_req,
    :candidate, %__MODULE__{
      current_term: current_term
    } = data
  ) when current_term == term do
    # Another peer has already win a election, so I just fallback to follower
    actions = [
      {:next_event, :internal, append_entries_req},
      {:next_event, :internal, :became_follower}
    ]
    {:next_state, :follower, data, actions}
  end
  def handle_event(
    :cast = _event_type, %AppendEntries{
      term: term
    } = append_entries_req,
    :candidate = _state, %__MODULE__{
      current_term: current_term
    } = data
  ) when current_term > term do
    data |> refuse_append_entries(append_entries_req)
    :keep_state_and_data
  end

  # Event: RequestVoteReply
  def handle_event(
    :cast = _event_type, %RequestVoteReply{
      term: term
    } = _vote_reply,
    :candidate = _state, %__MODULE__{
      current_term: current_term
    } = _data
  ) when current_term > term do
    :keep_state_and_data
  end
  def handle_event(
    :cast = _event_type,
    %RequestVoteReply{
      term: term,
      vote_granted: granted,
      source: source,
    } = _vote_reply,
    :candidate = _state,
    %__MODULE__{
      current_term: current_term,
      votes_responded: votes_responded
    } = data) when current_term == term and not granted do
    data = %{data |
             votes_responded: votes_responded |> MapSet.put(source)
            }
    {:keep_state, data}
  end
  def handle_event(
    :cast = _event_type,
    %RequestVoteReply{
      term: term,
      vote_granted: granted,
      source: source
    } = _vote_reply,
    :candidate,
    %__MODULE__{
      current_term: current_term,
      votes_responded: votes_responded,
      votes_granted: votes_granted
    } = data) when current_term == term and granted do
    # deal the granted vote
    data = %{data |
             votes_responded: votes_responded |> MapSet.put(source),
             votes_granted: votes_granted |> MapSet.put(source)
            }

    if win_election?(data) do
      {:next_state, :leader, data, {:next_event, :internal, :became_leader}}
    else
      {:keep_state, data}
    end
  end

  def handle_event(
    :cast, %AppendEntriesReply{},
    :candidate, _data
  ) do
    :keep_state_and_data
  end

  def handle_event(
    _event_type, _event_content,
    :candidate, _data
  ) do
    :keep_state_and_data
  end


  #### Leader State ####


  # Guard: Update Term
  # if message's term is greater than current term,
  # update the term, and transite to :follower, and re-apply the request.
  def handle_event(
    :cast, %{
      term: term
    } = rpc,
    :candidate, %__MODULE__{
      current_term: current_term
    } = data
  ) when current_term < term do
    data = data |> update_term(term)
    replay_request =
      case rpc do
        %RequestVote{}   -> true
        %AppendEntries{} -> true
        _                -> false
        # in fact, no reply whose term is greater than it own,
        # this situation should not happen.
      end
    actions = if replay_request do
      [
        {:next_event, :internal, rpc}, # replay the request
        {:next_event, :internal, :became_follower}
      ]
    else
      [
        {:next_event, :internal, :became_follower}  # and then became follower
      ]
    end
    {:next_state, :follower, data, actions}
  end

  # Event: became_leader
  def handle_event(
    :internal,
    :became_leader,
    :leader,
    %__MODULE__{
      log: log,
      config: config,
    } = data
  ) do
    last_log_index = log |> Raft.Log.Memory.get_last_log_index()
    peers = config |> Raft.Supervisor.Configuration.get_peers()

    init_next_indexes = peers |> Enum.map(fn peer ->
      {peer, last_log_index + 1}
    end) |> Map.new()
    init_match_indexes = peers |> Enum.map(fn peer ->
      {peer, 0}
    end) |> Map.new()

    data = %{data |
             next_indexes: init_next_indexes,
             match_indexes: init_match_indexes
            }

    data |> append_entries_to_peers()
    data = data |> reset_timer(:heartbeat)
    {:keep_state, data}
  end

  # Event: heartbeat_timeout
  def handle_event(
    :info, :heartbeat_timeout,
    :leader, %__MODULE__{
    } = data
  ) do
    data |> append_entries_to_peers()
    data = data |> reset_timer(:heartbeat)
    {:keep_state, data}
  end

  # Event: AppendEntriesReply
  def handle_event(
    :cast = _event_type, %AppendEntriesReply{
      term: term
    } = _event_content,
    :leader, %__MODULE__{
      current_term: current_term
    } = _data
  ) when current_term > term do
    # Drop stale reply
    :keep_state_and_data
  end
  def handle_event(
    :cast = _event_type, %AppendEntriesReply{
      term: term,
      source: source,
      success: success,
      match_index: match_index
    } = _event_content,
    :leader = _state, %__MODULE__{
      current_term: current_term,
      next_indexes: next_indexes,
      match_indexes: match_indexes
    } = data
  ) when current_term == term and success do
    # Handle successful reply
    next_indexes = next_indexes |> Map.update!(source, fn _cur ->
      match_index + 1
    end)
    match_indexes = match_indexes |> Map.update!(source, fn _cur ->
      match_index
    end)

    data = %{data |
             next_indexes: next_indexes,
             match_indexes: match_indexes
            }
    {:keep_state, data,
     {:next_event, :internal, {:advance_commit_index, match_index}}
    }
  end
  def handle_event(
    :cast = _event_type, %AppendEntriesReply{
      term: term,
      success: success,
      source: source
    },
    :leader = _state, %__MODULE__{
      current_term: current_term,
      next_indexes: next_indexes
    } = data
  ) when current_term == term and not success do
    # Handle failed reply, just backoff the next_index
    next_indexes = next_indexes |> Map.update!(source, fn cur ->
      max(1, cur - 1)
    end)

    data = %{data |
             next_indexes: next_indexes
            }
    {:keep_state, data,
     {:next_event, :internal, {:advance_commit_index, next_indexes |> Map.fetch!(source)} }
    }
  end

  # Event: advance commit index
  def handle_event(
    :internal = _event_type, {:advance_commit_index, hint},
    :leader = _state, %__MODULE__{
      log: log,
      commit_index: commit_index,
      match_indexes: match_indexes
    } = data
  ) do
    last_log_index = log |> Raft.Log.Memory.get_last_log_index()

    peer_size = match_indexes |> Enum.count()
    # find the biggest log index that most peers agree on.
    new_commit_index =
      Enum.find(Range.new(commit_index, last_log_index), last_log_index + 1, fn index ->
        agree_on_index = match_indexes
        |> Enum.filter(fn {_k, v} -> v >= index end)
        |> Enum.count()
        (agree_on_index + 1) * 2 <= (peer_size + 1)
      end) - 1

    {:keep_state, %{data | commit_index: new_commit_index}}
  end

  @doc """
   1. append the command to log as a new entry
   2. issue append_entries rpc.
   3. if safely replicated, apply to state machine and return result to client.
  """
  def handle_event(
    :cast,
    {:command, _command},
    :leader,
    %__MODULE__{
    } = _data) do
    # TODO: implement me
    :keep_state_and_data
  end


  # Event: Any others
  def handle_event(
    _event_type, _event_content,
    :leader, _data
  ) do
    :keep_state_and_data
  end

  # bootstrap from durable device
  defp bootstrap(me, peers) do
    {:ok, config} = Raft.Supervisor.Configuration.start_link(me, peers)
    {:ok, log} = Raft.Log.Memory.start_link(me)
    data = %__MODULE__{
      me: me,
      config: config,
      log: log,
      current_term: 0,
      voted_for: nil
    }
    data
  end

  defp reset_timer(%__MODULE__{timer: timer} = data, kind)
  when kind in [:election, :heartbeat] and is_reference(timer) do
    Process.cancel_timer(timer)
    %{data | timer: nil} |> reset_timer(kind)
  end
  defp reset_timer(%__MODULE__{timer: nil} = data, :election) do
    timer = Process.send_after(self(), :election_timeout, election_timeout())
    %{data | timer: timer}
  end
  defp reset_timer(%__MODULE__{timer: nil} = data, :heartbeat) do
    timer = Process.send_after(self(), :heartbeat_timeout, heartbeat_timeout())
    %{data | timer: timer}
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

  # Any RPC with a newer term causes the recipient to advance its term first.
  # and transit to follower.
  defp update_term(%__MODULE__{current_term: current_term} = data, term) do
    if current_term < term do
      %{data |
        current_term: term,
        voted_for: nil
      }
    else
      data
    end
  end


  defp win_election?(
    %__MODULE__{
      votes_granted: votes_granted,
      config: config
    }
  ) do
    peers = config |> Raft.Supervisor.Configuration.get_peers()
    (MapSet.size(votes_granted) + 1) * 2 > length(peers) + 1
  end


  defp log_ok?(log,
    %RequestVote{
    } = vote_req
  ) do
    last_log_term = log |> Raft.Log.Memory.get_last_log_term()

    log_ok = cond do
      vote_req.last_log_term > last_log_term ->
        true
      vote_req.last_log_term == last_log_term ->
        last_log_index = log |> Raft.Log.Memory.get_last_log_index()
        vote_req.last_log_index >= last_log_index
      :else ->
        false
    end
    log_ok
  end


  defp log_ok?(_log,
    %AppendEntries{
      prev_log_index: prev_log_index
    }
  ) when prev_log_index == 0 do
    true
  end
  defp log_ok?(log,
    %AppendEntries{
      prev_log_index: prev_log_index,
      prev_log_term: prev_log_term
    }
  ) when prev_log_index > 0 do
    last_log_index = log |> Raft.Log.Memory.get_last_log_index()
    if prev_log_index > last_log_index do
      false
    else
      entry = log |> Raft.Log.Memory.get(prev_log_index)
      prev_log_term == entry.term
    end
  end
  defp reject_request_vote(
    %__MODULE__{
      current_term: current_term
    },
    %RequestVote{
      source: source,
      dest: dest
    }
  ) do
    reply = %RequestVoteReply{
      term: current_term,
      vote_granted: false,
      source: dest,
      dest: source
    }
    reply.dest |> Raft.RPC.send_msg(reply)
  end
  defp accept_request_vote(
    %__MODULE__{
      current_term: current_term
    } = data,
    %RequestVote{
      source: source,
      dest: dest
    }
  ) do
    reply = %RequestVoteReply{
      term: current_term,
      vote_granted: true,
      source: dest,
      dest: source
    }
    reply.dest |> Raft.RPC.send_msg(reply)
    %{data |
      voted_for: source
    }
  end

  defp refuse_append_entries(
    %__MODULE__{
      current_term: current_term
    },
    %AppendEntries{
      source: source,
      dest: dest
    }
  ) do
    reply = %AppendEntriesReply{
      term: current_term,
      source: dest,
      dest: source,
      success: false,
      match_index: 0
    }
    source |> Raft.RPC.send_msg(reply)
  end

  defp accept_append_entries(
    %__MODULE__{
      current_term: current_term,
      commit_index: commit_index
    } = data,
    %AppendEntries{
      source: source,
      dest: dest,
      prev_log_index: prev_log_index,
      entries: entries,
      leader_commit: leader_commit
    }
  ) when length(entries) == 0 do
    reply = %AppendEntriesReply{
      term: current_term,
      source: dest,
      dest: source,

      success: true,
      match_index: prev_log_index + length(entries)
    }
    reply.dest |> Raft.RPC.send_msg(reply)

    # XXX: only advance commit index when I got equal log with leader.
    commit_index = cond do
      leader_commit > commit_index ->
        min(leader_commit, prev_log_index + length(entries))
      :else ->
        commit_index
    end
    %{data | commit_index: commit_index}
  end
  defp accept_append_entries(
    %__MODULE__{
      current_term: current_term,
      log: log
    } = data,
    %AppendEntries{
      source: source,
      dest: dest,
      prev_log_index: prev_log_index,
      entries: entries
    }
  ) when length(entries) > 0 do
    last_log_index = log |> Raft.Log.Memory.get_last_log_index()
    cond do
      prev_log_index == last_log_index -> # no conflict: append entries
        log |> Raft.Log.Memory.append(entries)
        data
      prev_log_index < last_log_index ->
        entry = log |> Raft.Log.Memory.get(prev_log_index + 1)
        if entry.term != List.first(entries).term do
          # conflict: remove 1 entry to do backoff
          log |> Raft.Log.Memory.truncate(1)
        else
          # already done with the request
          reply = %AppendEntriesReply{
            term: current_term,
            source: dest,
            dest: source,

            success: true,
            match_index: prev_log_index + length(entries)
          }
          reply.dest |> Raft.RPC.send_msg(reply)
        end
        data
      true ->
        Logger.error("this should not happen")
        data
    end
  end


  defp append_entries_to_peers(
    %__MODULE__{
      me: me,
      current_term: current_term,
      commit_index: commit_index,
      next_indexes: next_indexes,
      log: log,
      config: config
    }
  ) do
    peers = config |> Raft.Supervisor.Configuration.get_peers()
    for peer <- peers do
      next_index = Map.fetch!(next_indexes, peer)
      relative_prev_log_index = next_index - 1
      relative_prev_log_term = cond do
        relative_prev_log_index > 0 -> Raft.Log.Memory.get(log, relative_prev_log_index).term
        :else                       -> 0
      end
      entries_size = cond do
        relative_prev_log_index == Raft.Log.Memory.get_last_log_index(log) -> 0
        :else                                                       -> 1
      end

      # XXX: the sub_log should be inclusive.
      entries = Raft.Log.Memory.sub_log(log, next_index, entries_size)

      append_entries_req = %AppendEntries{
        term: current_term,
        source: me,
        dest: peer,

        prev_log_index: relative_prev_log_index,
        prev_log_term: relative_prev_log_term,
        entries: entries,
        leader_commit: commit_index
      }
      append_entries_req.dest |> Raft.RPC.send_msg(append_entries_req)
    end
  end

end
