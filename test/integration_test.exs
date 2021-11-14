defmodule EctoLock.IntegrationTest do
  use ExUnit.Case

  defmodule MyRepo do
    use Ecto.Repo, otp_app: :ecto_lock, adapter: Ecto.Adapters.Postgres
  end

  setup_all do
    Application.put_env(:ecto_lock, MyRepo, database: "ecto_lock_test2")
    MyRepo.__adapter__().storage_up(MyRepo.config())

    start_supervised(MyRepo)

    :ok
  end

  test "raises on timeout when trying to acquire unavailable lock" do
    MyRepo.transaction(fn ->
      assert :ok = EctoLock.advisory_lock(MyRepo, 10)
      assert :ok = EctoLock.advisory_lock(MyRepo, 10)
      assert :ok = EctoLock.advisory_lock(MyRepo, 10)
    end)

    assert_raise(DBConnection.ConnectionError, fn ->
      MyRepo.transaction(
        fn ->
          EctoLock.advisory_lock(MyRepo, 10)
        end,
        timeout: 100
      )
    end)
  end

  test "try locks" do
    test_pid = self()

    spawn(fn ->
      MyRepo.transaction(fn ->
        assert :ok = EctoLock.advisory_lock(MyRepo, 20)

        send(test_pid, :acquired)

        :timer.sleep(500)

        assert :ok = EctoLock.advisory_unlock(MyRepo, 20)

        send(test_pid, :released)
      end)
    end)

    MyRepo.transaction(fn ->
      assert_receive :acquired, 1000

      :error = EctoLock.try_advisory_lock(MyRepo, 20)

      assert_receive :released, 1000

      :ok = EctoLock.try_advisory_lock(MyRepo, 20)
    end)
  end

  test "drop all locks" do
    MyRepo.transaction(fn ->
      idxs = [30, 40, 50, 60, 70, 80]

      for idx <- idxs do
        assert :ok = EctoLock.advisory_lock(MyRepo, idx)
      end

      assert :ok = EctoLock.advisory_unlock_all(MyRepo)

      active_locks = pg_locks()

      for idx <- idxs do
        refute idx in active_locks
      end
    end)
  end

  test "xact lock is freed upon transaction being finished" do
    test_pid = self()

    spawn(fn ->
      MyRepo.transaction(fn ->
        assert :ok = EctoLock.advisory_xact_lock(MyRepo, 90)
        send(test_pid, :locked)
        :timer.sleep(500)
      end)
    end)

    assert_receive :locked, 1000

    # Lock will be acquired when process dies
    MyRepo.transaction(fn ->
      assert :ok = EctoLock.advisory_xact_lock(MyRepo, 90)
    end)
  end

  def pg_locks do
    import Ecto.Query

    from("pg_locks")
    |> where([l], l.locktype == "advisory")
    |> select([l], l.objid)
    |> MyRepo.all()
  end
end
