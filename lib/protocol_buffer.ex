defmodule ExLumber.ProtocolBuffer do
  use GenServer
  require Logger

  @behaviour :ranch_protocol

  def start_link(ref, socket, transport, reader) do
    Logger.info("Starting server")
    pid = :proc_lib.spawn_link(__MODULE__, :init, [{ref, socket, transport, reader}])
    {:ok, pid}
  end

  @impl true
  def init({ref, _socket, transport, reader}) do
    Logger.info("Starting protocol")

    {:ok, socket} = :ranch.handshake(ref)
    :ok = transport.setopts(socket, [{:active, true}])

    state = %{
      socket: socket,
      transport: transport,
      buffer: nil,
      reader: reader,
      demand: 0
    }
    {:ok, state}
  end

  @impl true
  def handle_info({:tcp, _socket, data}, state) do
    state = %{state | buffer: state.buffer <> data}
    dispatch_bytes(state)
  end

  def handle_info({:tcp_closed, socket}, state) do
    Logger.info("Closing")
    state.transport.close(socket)
    {:stop, :normal, state}
  end

  def handle_call({:read_bytes, demand}, state) do
    state = Map.put(state, :demand, state.demand + demand)
    dispatch_bytes(state)
  end

  defp dispatch_bytes(state) do
    buffer_size = byte_size(state.buffer)
    if buffer_size >= state.demand do
      {data, rest} = read_bytes(state.buffer, state.demand)
      GenServer.reply(state.reader, data)
      {:noreply, %{state | buffer: rest, demand: 0}}
    else
      {:noreply, state}
    end
  end

  defp read_bytes(buffer, demand) do
    <<head::binary-size(demand), rest::binary>> = buffer
    {head, rest}
  end
end
