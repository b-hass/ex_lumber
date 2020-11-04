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
    :gen_server.enter_loop(__MODULE__, [], %{socket: socket, transport: transport})
  end


  def handle_info({:tcp, socket, data}, state = %{socket: socket, transport: transport}) do
    cursor = 0

    version_binary = :binary.part(data, {cursor, 1})
    version = List.to_string([version_binary])
    cursor = advance(cursor, 1)

    type_binary = :binary.part(data, {cursor, 1})
    type = List.to_string([type_binary])
    cursor = advance(cursor, 1)

    count = :binary.part(data, {cursor, 4}) |> :binary.decode_unsigned(:big)
    cursor = advance(cursor, 4)

    events = read_events(data, cursor, count)

    transport.send(socket, data)
    {:noreply, state}
  end

  defp read_events(data, cursor, count) do
    Enum.flat_map(0..count, fn _ ->
      version_binary = :binary.part(data, {cursor, 1})
      version = List.to_string([version_binary])
      cursor = advance(cursor, 1)
      type_binary = :binary.part(data, {cursor, 1})
      type = List.to_string([type_binary])
      cursor = advance(cursor, 1)

      case type do
        @code_json_data_frame ->
          {event, cursor} = read_json(data, cursor)
          [event]
        @code_compressed ->
          {uncompressed, cursor} = read_compressed(data, cursor)
          read_events(uncompressed, 0, 0)
        _ ->
          Logger.error("Unknown frame type #{type}")
          []
      end
    end)
  end

  defp read_json(data, cursor) do
    seq = :binary.part(data, {cursor, 4})
    |> :binary.decode_unsigned(:big)
    cursor = advance(cursor, 4)

    payload_size = :binary.part(data, {cursor, 4})
    |> :binary.decode_unsigned(:big)
    cursor = advance(cursor, 4)

    payload = :binary.part(data, {cursor, payload_size})

    {List.to_string([payload]), advance(cursor, payload_size)}
  end

  defp read_compressed(data, cursor) do
    payload_size = min(
      byte_size(data) - cursor - 4,
      :binary.part(data, {cursor, 4})
      |> :binary.decode_unsigned(:big)
    )
    cursor = advance(cursor, 4)

    payload = :binary.part(data, {cursor, payload_size})
    z = :zlib.open()
    :zlib.inflateInit(z)
    [uncompressed] = :zlib.inflate(z, payload)
    :zlib.close(z)

    {uncompressed, advance(cursor, payload_size)}
  end

  defp advance(cursor, byte_count) do
    cursor + byte_count
  end

  def handle_info({:tcp_closed, socket}, state = %{socket: socket, transport: transport}) do
    Logger.info("Closing")
    transport.close(socket)
    {:stop, :normal, state}
  end
end
