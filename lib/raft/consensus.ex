defmodule Raft.Consensus do
  @election_timeout_key :election_timeout
  @election_timeout_default_value 500

	def get_election_timeout() do
    Application.get_env(:raft_ex, @election_timeout_key, @election_timeout_default_value)
  end
end
