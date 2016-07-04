# Raft

Raft Protocol Implementation in Elixir.


**WIP**

Currently, the first step(leader-election) is done.
One can use the following to see the result.

``` elixir
# in one iex shell,
{:ok, server} = Raft.start_consensus_server({:server1, :"server1@localhost"}, [server1: :"server1@localhost", server2: :"server2@localhost"])

# and another.
{:ok, server} = Raft.start_consensus_server({:server2, :"server2@localhost"}, [server1: :"server1@localhost", server2: :"server2@localhost"])
```

and use `server |> Raft.Supervisor.get_consensus() |> :sys.get_state()`
to get the instant state of each.

Here is the result on my mac:

``` elixir
{:leader,
 %Raft.Consensus{commit_index: 0, config: #PID<0.2035.0>, current_term: 11,
  last_applied: 0, log: #PID<0.2036.0>,
  match_indexes: %{{:server2, :server2@localhost} => 0},
  me: {:server1, :server1@localhost}, meta: nil,
  next_indexes: %{{:server2, :server2@localhost} => 1},
  timer: #Reference<0.0.3.7630>, voted_for: {:server1, :server1@localhost},
  voter_log: %{}, votes_granted: #MapSet<[server2: :server2@localhost]>,
  votes_responded: #MapSet<[server2: :server2@localhost]>}}
# and another
{:follower,
 %Raft.Consensus{commit_index: 0, config: #PID<0.1110.0>, current_term: 11,
  last_applied: 0, log: #PID<0.1111.0>, match_indexes: %{},
  me: {:server2, :server2@localhost}, meta: nil, next_indexes: %{},
  timer: #Reference<0.0.4.4728>, voted_for: {:server1, :server1@localhost},
  voter_log: %{}, votes_granted: #MapSet<[]>, votes_responded: #MapSet<[]>}}
```

### Contribute

Ping me by Issue and PR!
