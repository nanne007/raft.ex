defmodule Raft.Server do

  @typedoc """
  server identity.
  """
  @type id :: term

  @typedoc """
  number of leader's term of office
  """
  @type rterm :: non_neg_integer

  @typedoc """
  index of log
  """
  @type index :: non_neg_integer

  @typedoc """
  entry of log
  """
  @type entry :: {rterm, term}

  use Supervisor
  def start_link(me) do
    Supervisor.start_link(__MODULE__, me)
  end


  def request_vote(me, params) do
    server = me
    |> Supervisor.which_children()
    |> List.keyfind(Raft.Server, 0)
    case server do
      nil ->
        {:error, :not_found}
      {Raft.Server, child, :worker, _modules} ->
        case child do
          :undefined ->
            {:error, :not_found}
          :restarting ->
            request_vote(me, params)
          pid ->
            pid |> Raft.Server.request_vote(params)
        end
    end
  end


  def init(me) do

    children = [
      worker(Raft.Server, [me])
    ]
    options =[
      strategy: :one_for_one,
      max_restarts: 2,
      max_seconds: 5
    ]

    supervise(children, options)
  end
end
