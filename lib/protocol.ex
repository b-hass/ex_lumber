defmodule ExLumber.Protocol do
  use GenServer
  require Logger

  @behaviour :ranch_protocol

  @code_version "2"
  @code_window_size "W"
  @code_json_data_frame "J"
  @code_compressed "C"
  @code_ack "A"

  def start_link(ref, socket, transport) do
    Logger.info("Starting server")
    pid = :proc_lib.spawn_link(__MODULE__, :init, [ref, socket, transport])
    {:ok, pid}
  end

  def init(ref, transport, _opts) do
    Logger.info("Starting protocol")

    {:ok, socket} = :ranch.handshake(ref)
    :ok = transport.setopts(socket, [{:active, true}])
    :gen_server.enter_loop(__MODULE__, [], %{socket: socket, transport: transport, buffer: nil})
  end

  def handle_info({:tcp, socket, data}, state = %{socket: socket, transport: transport, buffer: buffer}) do
    state = Map.put(state, :batch, buffer <> data)

    {:noreply, state}
  end

  def handle_info({:tcp_closed, socket}, state = %{socket: socket, transport: transport}) do
    Logger.info("Closing")
    transport.close(socket)
    {:stop, :normal, state}
  end

  @impl true
  def handle_call({:read_bytes, n}, _from, state = %{buffer: buffer}) do
    number_of_bytes = min(byte_size(buffer), n)
    <<head::binary-size(number_of_bytes), rest::binary>> = buffer

    new_state = Map.put(state, :buffer, rest)
    {:reply, head, new_state}
  end
end
