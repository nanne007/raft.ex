defmodule Raft.Storage do
  @moduledoc """
  storage api that provide basic functions to raft.
  """


  @callback initial_state() :: {:ok, term} | {:error, Map.t}
end
