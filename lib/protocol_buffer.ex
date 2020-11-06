defmodule ExLumber.ProtocolBuffer do
  use GenServer
  require Logger

  @behaviour :ranch_protocol

  @impl true
  def start_link(ref, socket, transport) do
    Logger.info("Starting server")
    pid = :proc_lib.spawn_link(__MODULE__, :init, [ref, socket, transport])
    {:ok, pid}
  end

  def init(ref, transport, _opts) do
    Logger.info("Starting protocol")

    {:ok, socket} = :ranch.handshake(ref)
    :ok = transport.setopts(socket, [{:active, true}])

    state = %{
      socket: socket,
      transport: transport,
      buffer: <<>>,
      reader: nil,
      demand: 0
    }

    :gen_server.enter_loop(__MODULE__, [], state)
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

  @impl true
  def handle_call({:read_bytes, demand}, from, state) do
    state = Map.put(state, :demand, state.demand + demand)
    state = Map.put(state, :reader, from)
    dispatch_bytes(state)
  end

  defp dispatch_bytes(state = %{demand: demand}) do
    buffer_size = byte_size(state.buffer)
    if demand > 0 and buffer_size >= demand do
      {data, rest} = read_bytes(state.buffer, demand)
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
