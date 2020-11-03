defmodule ExLumberTest do
  use ExUnit.Case
  doctest ExLumber

  test "greets the world" do
    assert ExLumber.hello() == :world
  end
end
