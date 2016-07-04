defmodule Raft do
  def start_consensus_server(me, servers) do
    Raft.Supervisor.start_link(me, servers)
  end
end
