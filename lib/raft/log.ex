defmodule Raft.Log do
  @callback get_last_log_index() :: Raft.Server.index
  @callback get_last_log_term() :: Raft.Server.rterm
end
