defmodule Raft.Supervisor do
  use Supervisor

  @type opts :: [
    name: String.t,
    host: :inet.ip_address,
    port: :inet.port_number,
    backend: module
  ]
  def start_link do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @doc """
  Start a raft server with `name`, listening on `host`, 'port'.
  """
  @spec start_consensus(opts) :: pid
  def start_consensus(opts) do
    name = Keyword.get(opts, :name, UUID.uuid4)
    host = Keyword.get(opts, :host, {0, 0, 0, 0})
    port = Keyword.get(opts, :port, 9966)
    backend = Keyword.get(opts, :backend)
    child = consensus_worker_spec({name, host, port, backend})
    Supervisor.start_child(__MODULE__, child) |> start_result()
  end

  ### Supervisor Callbacks

  def init([]) do
    children = []
    options = [
      strategy: :one_for_one,
      max_restarts: 2,
      max_seconds: 5
    ]

    supervise(children, options)
  end

  defp consensus_worker_spec({name, _host, _port, _backend} = me) do
    supervisor(Raft.Consensus, [me], id: name)
  end


  defp start_result(result) do
    case result do
      {:ok, c} ->
        {:ok, c}
      {:ok, c, _info} ->
        {:ok, c}
      {:error, {:already_started, _c}} ->
        {:error, :already_created}
      {:error, {:already_present}} ->
        {:error, :already_created}
      {:error, _t} = err ->
        err
    end
  end

end
