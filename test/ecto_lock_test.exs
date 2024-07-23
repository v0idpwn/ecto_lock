defmodule EctoLockTest do
  use ExUnit.Case

  defmodule ReturnVoidRepo do
    def query!(string, params, _opts) do
      send(self(), {:query, string, params})
    end
  end

  defmodule ReturnTrueRepo do
    def query!(string, params, _opts) do
      send(self(), {:query, string, params})
      %Postgrex.Result{rows: [[true]]}
    end
  end

  defmodule ReturnFalseRepo do
    def query!(string, params, _opts) do
      send(self(), {:query, string, params})
      %Postgrex.Result{rows: [[false]]}
    end
  end

  test "advisory_lock/2 calls proper function" do
    assert :ok = EctoLock.advisory_lock(ReturnVoidRepo, 1)
    assert_received {:query, "SELECT pg_advisory_lock($1)", [1]}
  end

  test "advisory_lock_shared/2 calls proper function" do
    assert :ok = EctoLock.advisory_lock_shared(ReturnVoidRepo, 1)
    assert_received {:query, "SELECT pg_advisory_lock_shared($1)", [1]}
  end

  test "advisory_unlock/2 calls proper function" do
    assert :ok = EctoLock.advisory_unlock(ReturnTrueRepo, 1)
    assert_received {:query, "SELECT pg_advisory_unlock($1)", [1]}
    assert :error = EctoLock.advisory_unlock_shared(ReturnFalseRepo, 1)
  end

  test "advisory_unlock_all/1 calls proper function" do
    assert :ok = EctoLock.advisory_unlock_all(ReturnTrueRepo)
    assert_received {:query, "SELECT pg_advisory_unlock_all()", []}
  end

  test "advisory_unlock_shared/2 calls proper function" do
    assert :ok = EctoLock.advisory_unlock_shared(ReturnTrueRepo, 1)
    assert_received {:query, "SELECT pg_advisory_unlock_shared($1)", [1]}
    assert :error = EctoLock.advisory_unlock_shared(ReturnFalseRepo, 1)
  end

  test "advisory_xact_lock/2 calls proper function" do
    assert :ok = EctoLock.advisory_xact_lock(ReturnVoidRepo, 1)
    assert_received {:query, "SELECT pg_advisory_xact_lock($1)", [1]}
  end

  test "advisory_xact_lock_shared/2 calls proper function" do
    assert :ok = EctoLock.advisory_xact_lock_shared(ReturnVoidRepo, 1)
    assert_received {:query, "SELECT pg_advisory_xact_lock_shared($1)", [1]}
  end

  test "try_advisory_lock/2 calls proper function" do
    assert :ok = EctoLock.try_advisory_lock(ReturnTrueRepo, 1)
    assert_received {:query, "SELECT pg_try_advisory_lock($1)", [1]}
    assert :error = EctoLock.try_advisory_lock(ReturnFalseRepo, 1)
  end

  test "try_advisory_lock_shared/2 calls proper function" do
    assert :ok = EctoLock.try_advisory_lock_shared(ReturnTrueRepo, 1)
    assert_received {:query, "SELECT pg_try_advisory_lock_shared($1)", [1]}
    assert :error = EctoLock.try_advisory_lock_shared(ReturnFalseRepo, 1)
  end

  test "try_advisory_xact_lock/2 calls proper function" do
    assert :ok = EctoLock.try_advisory_xact_lock(ReturnTrueRepo, 1)
    assert_received {:query, "SELECT pg_try_advisory_xact_lock($1)", [1]}
    assert :error = EctoLock.try_advisory_xact_lock(ReturnFalseRepo, 1)
  end

  test "try_advisory_xact_lock_shared/2 calls proper function" do
    assert :ok = EctoLock.try_advisory_xact_lock_shared(ReturnTrueRepo, 1)
    assert_received {:query, "SELECT pg_try_advisory_xact_lock_shared($1)", [1]}
    assert :error = EctoLock.try_advisory_xact_lock_shared(ReturnFalseRepo, 1)
  end

  test "tuple_to_key/1 turns tuple into namespaced keys" do
    assert key = EctoLock.tuple_to_key({:business, 0})
    assert ^key = EctoLock.tuple_to_key({"business", 0})

    key2 = EctoLock.tuple_to_key({:business, 1})
    assert key2 == key + 1
    assert in_key_range?(key)
  end

  test "tuple_to_key/1 can handle uint32 coming from :erlang.crc32" do
    key =
      {<<255, 15, 14, 153, 135, 79, 33, 91, 4, 127>>, 1000}
      |> EctoLock.tuple_to_key()

    assert key < 0
    assert in_key_range?(key)
  end

  test "regression: keep in bounds without losing precision" do
    # Key is any binary where the MAX_UINT32 > crc32 > MAX_I32
    # "wins" is an example of such case
    namespace = "wins"

    Enum.reduce(1..10, [], fn v, keys ->
      key = EctoLock.tuple_to_key({namespace, v})
      refute key in keys
      assert in_key_range?(key)
      [key | keys]
    end)
  end

  test "regression: no overflow" do
    # Sample key that caused overflow
    key = EctoLock.tuple_to_key({"uc", 1})
    assert in_key_range?(key)
  end

  defp in_key_range?(key) do
    key in -9_223_372_036_854_775_808..9_223_372_036_854_775_807
  end
end
