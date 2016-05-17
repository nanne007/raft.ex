defmodule Raft.Supervisor do
  use Supervisor

  def start_link do
    Supervisor.start_link(__MODULE__, name: __MODULE__)
  end


  def start_consensus(peer_id) do
    child = consensus_worker_spec([peer_id])
    Supervisor.start_child(__MODULE__, child) |> start_result
  end

  def start_consensus(peer_id, backend) do
    child = consensus_worker_spec([peer_id, backend])
    Supervisor.start_child(__MODULE__, child) |> start_result
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



  ### Supervisor Callbacks

  def init([]) do
    timeout = round(Raft.Consensus.get_election_timeout() * 4 / 1000) |> max(1)

    children = read_peers() |> Enum.map(&consensus_worker_spec(&1))

    options = [
      strategy: :one_for_one,
      max_restarts: 2,
      max_seconds: timeout
    ]

    supervise(children, options)
  end

  defp consensus_worker_spec([{peer_name, _}, _backend] = args) do
    worker(Raft.Consensus, args, id: peer_name)
  end

  defp read_peers do
    data_dir = Application.get_env(:raft_ex, :log_dir, "data")

    case File.ls(data_dir) do
      {:ok, dirs} ->
        read_peers(data_dir, dirs, [])
      _ ->
        []
    end
  end

  defp read_peers(data_dir, [dir | rest], acc) do
    raft_dir = Path.join(data_dir, dir)

    case Raft.FS.Log.load_raft_meta(raft_dir) do
      {:ok, %Raft.FS.Log.RaftMeta{id: peer, backend:  backend}} ->
        read_peers(data_dir, rest, [[peer, backend] | acc])
      _ ->
        require Logger
        Logger.warn("#{raft_dir} does't contain peer meta")
        read_peers(data_dir, rest, acc)
    end
  end
  defp read_peers(_data_dir, [], acc), do: acc
end
