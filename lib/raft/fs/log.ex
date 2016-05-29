defmodule Raft.FS.Log do
  require Logger

  defmodule Configuration do
    @type t :: %__MODULE__{
      term: non_neg_integer,
      index: non_neg_integer,
      members: list(term)
    }
    defstruct [
      term: nil,
      index: nil,
      members: []
    ]
  end

  defmodule Meta do
    @type t :: %__MODULE__{
      id: term,
      voted_for: term,
      current_term: non_neg_integer,
      backend: term,
      configuration: Configuration.t
    }
    defstruct [
      id: nil,
      voted_for: nil,
      current_term: 0,
      backend: nil,
      configuration: nil
    ]
  end


  @type t ::%__MODULE__{
    meta: Meta.t
  }
  defstruct [
    meta: nil
  ]

  use GenServer

  def start_link(name, dir) do
    GenServer.start_link(__MODULE__, {name, dir}, name: process_name(name))
  end


  def get_meta(log) do
    log |> GenServer.call(:get_meta)
  end


  ### GenServer callbacks

  def init({name, dir}) do
    meta = case read_meta_file(Path.join(dir, "#{name}.meta")) do
             {:ok, m} ->
               m
             {:error, _} ->
               %Meta{}
           end
    state = %__MODULE__{
      meta: meta
    }
    {:ok, state}
  end

  def handle_call(:get_meta, _from, %__MODULE__{
            meta: meta
                           }=state) do
    {:reply, meta, state}
  end



  ### Helper functions

  defp read_meta_file(filename) do
    case File.read(filename) do
      {:ok, data} ->
        case :erlang.binary_to_term(data) do
          %Meta{} = m ->
            {:ok, m}
          other ->
            Logger.error("cannot parse erlang term from file #{inspect filename}:#{inspect other}")
            {:error, :badformat}
        end
      {:error, _} = err ->
        Logger.error("cannot read metafile #{inspect filename}:#{inspect err}")
        err
    end
  end



  # get process_id of the process from name
  defp process_name(name) do
    "#{name}_log"
  end
end
