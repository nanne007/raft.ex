defmodule Raft.Log do
  @callback get_last_log_index() :: Raft.Supervisor.index
  @callback get_last_log_term() :: Raft.Supervisor.rterm
end
