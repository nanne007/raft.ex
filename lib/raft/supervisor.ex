defmodule Raft.Supervisor do
  use Supervisor
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
      index: Raft.Supervisor.index,
      term: Raft.Supervisor.rterm,
      content: term
    }
    defstruct [
      index: nil,
      term: nil,
      content: nil
    ]
  end

  def start_link(me, servers) do
    if not me in servers do
      raise "#{me} should be in #{servers}"
    end

    {name, _} = me
    peers = servers |> Keyword.delete(name)
    Supervisor.start_link(__MODULE__, [me, peers], name: name)
  end

  def get_consensus(sup) do
    consensus = sup
    |> Supervisor.which_children()
    |> List.keyfind(Raft.Consensus, 0)

    case consensus do
      nil ->
        {:error, :not_found}
      {Raft.Consensus, child, :worker, _modules} ->
        case child do
          :undefined ->
            {:error, :not_found}
          :restarting ->
            get_consensus(sup)
          pid ->
            pid
        end
    end
  end

  ### Supervisor Callbacks

  def init([me, peers]) do
    children = [
      worker(Raft.Consensus, [me, peers])
    ]
    options = [
      strategy: :one_for_one,
      max_restarts: 2,
      max_seconds: 5
    ]

    supervise(children, options)
  end
end
