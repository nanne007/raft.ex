defmodule Raft.Node do
  defmodule Config do
    defstruct [:min_election_timeout, :max_election_timeout]
  end
  defmodule ConfState do
    defstruct [:peers]
  end

  defmodule HardState do
    defstruct current_term: 0, vote_for: nil
  end

  defmodule NodeState do
    defstruct [:hard_state, :conf_state]
  end

  @invalid_id 0
  defstruct config: nil,
            id: nil,
            peers: [],
            # persistent state
            current_term: 0,
            vote_for: nil,
            log: nil,

            # volatile state
            commit_index: 0,
            last_applied: 0,

            # follower state
            leader_id: @invalid_id,
            election_elapsed: 0,
            randomized_election_timeout: 0

  defguard is_leader_request_type(raft_msg_type) when raft_msg_type in [AppendEntries]
  alias Raft.HardState

  def start_link(args) do
    :gen_statem.start_link(__MODULE__, args, [])
  end

  def tick() do
  end

  def step(msg) do
  end

  def propose(data) do
    :gen_statem.call(__MODULE__, {:propose, data})
  end

  @behaviour :gen_statem
  @impl true
  def callback_mode(), do: :state_functions

  def init([config, id, %NodeState{ hard_state: hard_state, conf_state: conf_state}]) do
    peers = conf_state.peers
    if peers |> Enum.empty?() do
      peers = [id]
    end

    initial_data = %__MODULE__{
      config: config,
      id: id,
      peers: peers
    }
    |> load_state(hard_state)

    data = initial_data |> become_follower(initial_data.term, @invalid_id)

    {:ok, :follower, data}
  end

  ### Follower state

  # tick
  def follower(:cast, :tick, data) do
    data = %{data | election_elapsed: data.election_elapsed + 1}

    if data |> pass_election_timeout() do
      {:keep_state, data}
    else
      data = data |> campaign(:pre_election)
      {:next_state, :pre_candidate, data, [{:next_event, :internal, :check_votes_status}]}
    end
  end


  # handle raft message in follower state
  def follower(:cast, {:raft, %RaftMessage{to: to, msg: msg}}, data)
  when msg === nil or to != data.id do
    :keep_state_and_data
  end

  # handle an outdated message
  def follower(:cast, {:raft, %RaftMessage{from: from, term: term, msg: msg}}, data)
  when term < data.current_term do
    case msg do
      %RequestPreVote{} ->
        data |> reject_vote(from)
      %RequestVote{} ->
        data |> reject_vote(from, :pre_vote)
    end
    :keep_state_and_data
  end
  def follower(:cast, {:raft, %RaftMessage{from: from, term: term, msg: raft_msg}}, data)
  when term > data.current_term do
    case raft_msg do
      # For a pre-vote request:
      # Never change our term in response to a pre-vote request.
      #
      # For a pre-vote response with pre-vote granted:
      # We send pre-vote requests with a term in our future. If the
      # pre-vote is granted, we will increment our term when we get a
      # quorum. If it is not, the term comes from the node that
      # rejected our vote so we should become a follower at the new
      # term.
      %RequestPreVote{} | %RequestPreVoteResp{vote_granted: true} -> nil
      %raft_msg_type{} when is_leader_request_type(raft_msg_type) ->
        data = data |> become_follower(term, from)
        {}
      _ ->
        data = data |> become_follower(term, @invalid_id)
    end
  end

  # handle request vote
  def follower(:cast,
    {:raft, %RaftMessage{from: from, term: term, msg: %RequestVote{}}},
    data
  ) when term > data.current_term do
    # handle leader lease
    data |> reject_vote(from, term)
    :keep_state_and_data
  end
  def follower(:cast,
    {:raft, %RaftMessage{
        from: from,
        term: term,
        msg: %RequestVote{
          last_log_term: last_log_term,
          last_log_index: last_log_index
        }}},
    data
  ) when term == data.current_term do
    should_grant_vote = data.vote_for === nil and log_up_to_date?(
      last_log_term, last_log_index,
      data.log.get_last_term(), data.log.get_last_index()
    )

    if should_grant_vote do
      data = data |> grant_vote(from, term)
      {:keep_state, data}
    else
      data |> reject_vote(from, term)
      :keep_state_and_data
    end
  end

  def follower(_event_type, _event_content, _data) do
    :keep_state_and_data
  end


  ### PreCandidate state
  def pre_candidate(:internal, {:raft, msg}, data) do
  end

  ### Candidate state

  # tick
  def candidate(:cast, :tick, data) do
    data = %{data | election_elapsed: data.election_elapsed + 1}

    if data.election_elapsed > data.randomized_election_timeout do
      data = data |> become_candidate()

      data |> bcast_request_vote(data.term, :vote)
      {:next_state, :candidate, data}
    else
      {:keep_state, data}
    end
  end

  # receive raft messages
  def candidate(:cast, {:step, raft_message}, data) do
    case raft_message do
      %RequestVoteResp{} ->
        nil
    end
  end

  def candidate(_event_type, _event_content, _data) do
    :keep_state_and_data
  end

  defp become_follower(%__MODULE__{} = data, term, leader_id) do
    data = data |> reset()
    %{data | leader_id: @leader_id}
  end

  defp campaign(%__MODULE__{} = data, campaign_type) when campaign_type === :pre_election do
    data = data |> become_pre_candidate() |> register_vote(data.id, true)
    if data |> votes_status() === :elected do
      # We won the election after voting for ourselves
      # (which must mean that this is a single-node cluster),
      # Advance to the next state directly.
      data = data |> campaign(:election)
    else
      data |> bcast_request_vote(data.term + 1, :pre_vote)
      {:pre_candidate, data}
    end
  end
  defp campaign(%__MODULE__{} = data, campaign_type) when campaign_type === :election do
    data = data |> become_candidate() |> register_vote(data.id, true)
    if data|> votes_status() === :elected do
      # We won the election after voting for ourselves
      # (which must mean that this is a single-node cluster),
      # Advance to the next state directly.
      data = data |> become_leader()
      {:leader, data}
    else
      data |> bcast_request_vote(data.term, :vote)
      {:candidate, data}
    end
  end

  defp become_pre_candidate(%__MODULE__{} = data) do
    # Becoming a pre-candidate changes our state.
    # but doesn't change anything else. In particular it does not increase
    #  self.term or change self.vote.
    %{
      data|
      votes: %{},
      leader_id: @invalid_id
    }
  end
  def become_candidate(%__MODULE__{} = data) do
    data = data |> reset(data.term + 1)

    # vote for self
    data = %{
      data |
      vote_for: data.id,
      votes: %{}
    }
    data
  end

  def reset(%__MODULE__{} = data, term) do
    if term != data.term do
      data = %{
        data |
        term: term,
        vote_for: @invalid_id,
      }
    end
    %{
      data |
      leader_id: @leader_id,
      election_elapsed: 0,
      heartbeat_elapsed: 0,
      votes: %{}
    } |> reset_randomized_election_timeout()
  end

  defp reject_vote(%__MODULE__{
        id: id,
        current_term: current_term
                   },
    from,
    vote_type \\ :vote
  ) do
    raft_msg = %RaftMessage {
      from: id,
      to: from,
      term: current_term,
      msg: %RequestVoteResp{
        vote_granted: false
      }
    }
    send_messages([raft_messages])
  end
  def grant_vote(%__MODULE__{} = data, from, term) do
    data = %{
      data|
      vote_for: from,
      current_term: term,
      election_elapsed: 0
    } |> reset_randomized_election_timeout()
    resp = %RaftMessage{
      from: data.id,
      to: from,
      term: data.current_term,
      msg: %RequestVoteResp{
        vote_granted: grant_vote
      }
    }
    send_messages([resp])
    data
  end

  # sent messages out
  def send_messages(messages) do
  end

  defp incr_current_term(%__MODULE__{current_term: current_term} = data) do
    %{data | current_term: current_term + 1}
  end

  defp reset_randomized_election_timeout(%__MODULE__{config: config} = data) do
    randomized_election_timeout =
      :crypto.rand_uniform(config.min_election_timeout, config.max_election_timeout)

    %{data | randomized_election_timeout: randomized_election_timeout}
  end
  def log_up_to_date?(log_term, _, term, _) when log_term !== term do: log_term
  def log_up_to_date?(log_term, log_index, term, index) when log_term === term do: log_index >= index

  defp load_state(%__MODULE__{} = data, %HardState{current_term: current_term, vote_for: vote_for}) do
    %{
      data |
      current_term: current_term,
      vote_for: vote_for
    }
  end

  defp pass_election_timeout(
    %__MODULE__{
      election_elapsed: election_elapsed,
      randomized_election_timeout: randomized_election_timeout
    }) do
    election_elapsed >= randomized_election_timeout
  end

  defp register_vote(%__MODULE__{votes: votes} = data, from, vote) do
    %{
      data|
      votes: votes |> Map.put_new(from, vote)
    }
  end

  # Check votes response from peers to see if I can be leader
  def votes_status(%__MODULE__{peers: peers, votes: votes} = data) do
    majority = div(length(peers), 2) + 1
    {accepts, rejects} = votes |> Enum.split_with(fn {_k, v} -> v === true end)
    case peers > MapSet.intersection(MapSet.new(accepts)) |> MapSet.size() >= majority do
      true ->
        :elected
      false ->
        case peers > MapSet.intersection(MapSet.new(rejects)) |> MapSet.size() >= majority do
          true ->
            :ineligible
          false ->
            :eligible
        end
    end
  end

  # send request-vote request to other peers
  def bcast_request_vote(
    %__MODULE__{
      peers: peers,
      id: self_id,
      log: raft_log
    }, term, request_vote_type) do
    last_log_index = raft_log.get_last_log_index()
    last_log_term = raft_log.get_last_log_term()

    vote_req = if request_vote_type === :pre_vote do
      %RequestPreVote{
        last_log_index: last_log_index,
        last_log_term: last_log_term
      }
    else
      %RequestVote{
        last_log_index: last_log_index,
        last_log_term: last_log_term
      }
    end

    for peer <- peers, peer != self_id do
        msg = %RaftMessage{
          to: peer,
          term: term,
          msg: vote_req
        }
        send_msg(msg)
      end
  end
end
