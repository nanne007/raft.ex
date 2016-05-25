defmodule Raft.Meta do
  defstruct [
    :version,
    :current_term,
    :vote_for
  ]

  def load(file_name) do
    case File.read(file_name) do
      {:ok, data} ->
        case :erlang.binary_to_term(data) do
          %__MODULE__{} = meta ->
            meta
          other ->
            # TODO: add log
            {:error, :badformat}
        end
      {:error, reason} = err ->
        # TODO: add log
        err
    end
  end
end
