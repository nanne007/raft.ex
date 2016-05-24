defmodule Raft.FS.Log do
  require Logger

  defmodule RaftMeta do
    defstruct [:id, :voted_for, :current_term, :backend]
  end

  defmodule Meta do
    defstruct [:version, :first, :raft_meta]
  end


  def load_raft_meta(dir) do
    case read_meta_file(Path.join(dir, "meta2.info")) do
      %Meta{version: version1} = m1 ->
        case read_meta_file(Path.join(dir, "meta1.info")) do
          %Meta{version: version2} = m2 when version2 > version1 ->
            {:ok, m2.raft_meta}
          _ ->
            {:ok, m1.raft_meta}
        end
      error ->
        case read_meta_file(Path.join(dir, "meta1.info")) do
          %Meta{} = m2 ->
            {:ok, m2.raft_meta}
          _ ->
            error
        end
    end
  end

  defp read_meta_file(filename) do
    case File.read(filename) do
      {:ok, data} ->
        case :erlang.binary_to_term(data) do
          %Meta{} = m ->
            m
          other ->
            Logger.error("cannot parse erlang term from file #{inspect filename}:#{inspect other}")
            {:error, :badformat}
        end
      err ->
        Logger.error("cannot read metafile #{inspect filename}:#{inspect err}")
        err
    end
  end
end
