defmodule PlockTest do
  use ExUnit.Case
  doctest Plock

  test "greets the world" do
    assert Plock.hello() == :world
  end
end
