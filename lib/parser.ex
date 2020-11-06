defmodule ExLumber.Parser do
  use GenServer
  require Logger

  @behaviour :ranch_protocol

  @code_version "2"
  @code_window_size "W"
  @code_json_data_frame "J"
  @code_compressed "C"
  @code_ack "A"
  def read_batch(buffer) do
    version_binary = read_bytes(buffer, 1)
    type_binary = read_bytes(buffer, 1)

    if @code_version != "#{version_binary}" or
    @code_window_size != "#{type_binary}" do
      Logger.error("Expected Window frame version 2. Received #{type_binary} version #{version_binary}")
    else
      count = read_bytes(buffer, 4) |> :binary.decode_unsigned(:big)

      events = read_events(buffer, count)

      (Enum.count(events) - 1)
      |> ack()

      events
      |> IO.inspect()
    end

    read_batch(buffer)
  end

  defp read_events(buffer, count) do
    Enum.flat_map(0..count, fn _ ->
      version_binary = read_bytes(buffer, 1)
      _version = "#{version_binary}"
      type_binary = read_bytes(buffer, 1)
      type = "#{type_binary}"

      case type do
        @code_json_data_frame ->
          event = read_json(buffer)
          [event]
        @code_compressed ->
          [read_compressed(buffer)]
        _ ->
          Logger.error("Unknown frame type #{type}")
          []
      end
    end)
  end

  defp read_json(buffer) when is_pid(buffer) do
    _seq = read_bytes(buffer, 4)
    |> :binary.decode_unsigned(:big)

    payload_size = read_bytes(buffer, 4)
    |> :binary.decode_unsigned(:big)

    payload = read_bytes(buffer, payload_size)

    List.to_string([payload])
  end

  defp read_json(buffer) do
    <<_seq::binary-size(4), buffer::binary>> = buffer

    <<payload_binary::binary-size(4), buffer::binary>> = buffer
    payload_size = :binary.decode_unsigned(payload_binary, :big)

    <<payload::binary-size(payload_size)>> = buffer

    List.to_string([payload])
  end

  defp read_compressed(buffer) do
    payload_size = read_bytes(buffer, 4)
    |> :binary.decode_unsigned(:big)

    payload = read_bytes(buffer, payload_size)
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

  defp read_bytes(buffer, n) do
    GenServer.call(buffer, {:read_bytes, n})
  end
end
