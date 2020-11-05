defmodule ExLumber.Parser do
  use GenServer
  require Logger

  @behaviour :ranch_protocol

  @code_version "2"
  @code_window_size "W"
  @code_json_data_frame "J"
  @code_compressed "C"
  @code_ack "A"


  def read_batch(server) do
    version_binary = read_bytes(server, 1)
    type_binary = read_bytes(server, 1)

    if @code_version != "#{version_binary}" or
    @code_window_size != "#{type_binary}" do
      Logger.error("Expected Window frame version 2. Received #{type_binary} version #{version_binary}")
    else
      count = read_bytes(server, 4) |> :binary.decode_unsigned(:big)

      events = read_events(server, count)

      (Enum.count(events) - 1)
      |> ack()

      events
    end
  end

  defp read_events(server, count) do
    Enum.flat_map(0..count, fn _ ->
      version_binary = read_bytes(server, 1)
      _version = "#{version_binary}"
      type_binary = read_bytes(server, 1)
      type = "#{type_binary}"

      case type do
        @code_json_data_frame ->
          event = read_json(server)
          [event]
        @code_compressed ->
          read_compressed(server)
        _ ->
          Logger.error("Unknown frame type #{type}")
          []
      end
    end)
  end


  defp read_json(server) when is_bitstring(server) do
    _seq = read_bytes(server, 4)
    |> :binary.decode_unsigned(:big)

    payload_size = read_bytes(server, 4)
    |> :binary.decode_unsigned(:big)

    payload = read_bytes(server, payload_size)

    List.to_string([payload])
  end

  defp read_json(server) do
    _seq = read_bytes(server, 4)
    |> :binary.decode_unsigned(:big)

    payload_size = read_bytes(server, 4)
    |> :binary.decode_unsigned(:big)

    payload = read_bytes(server, payload_size)

    List.to_string([payload])
  end

  defp read_compressed(server) do
    payload_size = read_bytes(server, 4)
    |> :binary.decode_unsigned(:big)

    payload = read_bytes(server, payload_size)
    z = :zlib.open()
    :zlib.inflateInit(z)
    [uncompressed] = :zlib.inflate(z, payload)
    :zlib.close(z)

    uncompressed
  end

  defp ack(n) do
    [
      :unicode.characters_to_binary(@code_version),
      :unicode.characters_to_binary(@code_ack),
      <<n::32>>
    ]
  end

  defp read_bytes(server, n) do
    GenServer.call(server, {:read_bytes, n})
  end
end
