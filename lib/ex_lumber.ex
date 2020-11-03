defmodule ExLumber do
  @moduledoc """
  Documentation for `ExLumber`.
  """
  require Logger

  use Application

  @options  [port: 5555]
  @protocol ExLumber.Protocol

  def start(_type, _args) do
    Logger.info("Lumber start")
    {:ok, _} = :ranch.start_listener(:tcp_lumber, :ranch_tcp, @options, @protocol, [])
  end
end
