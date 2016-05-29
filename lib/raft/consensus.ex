defmodule Raft.Consensus do
  use Supervisor
  def start_link(me) do
    Supervisor.start_link(__MODULE__, me)
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
