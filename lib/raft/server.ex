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
  @type entry :: Entry.t
  defmodule Entry do
    @type t :: %__MODULE__{
      index: Raft.Sever.index,
      term: Raft.Server.rterm,
      content: term
    }
    defstruct [
      index: nil,
      term: nil,
      content: nil
    ]
  end


  use Supervisor
  def start_link(me) do
    {name, _} = me
    Supervisor.start_link(__MODULE__, name: name)
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


  def get_consensus(server) do
    consensus = server
    |> Supervisor.which_children()
    |> List.keyfind(Raft.Server, 0)

    case consensus do
      nil ->
        {:error, :not_found}
      {Raft.Consensus, child, :worker, _modules} ->
        case child do
          :undefined ->
            {:error, :not_found}
          :restarting ->
            get_consensus(server)
          pid ->
            pid
        end
    end

  end

  def init(me) do

    children = [
      worker(Raft.Consensus, [me])
    ]
    options =[
      strategy: :one_for_one,
      max_restarts: 2,
      max_seconds: 5
    ]

    supervise(children, options)
  end
end
