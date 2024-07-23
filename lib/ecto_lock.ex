defmodule EctoLock do
  @moduledoc """
  Provides helpers for advisory locks with postgresql
  """

  @max_i32 0b1111111111111111111111111111111
  @u32_last_bit 0b10000000000000000000000000000000

  @type repo :: module()
  @type key :: integer()
  @type result :: :ok | :error

  defguardp is_key(key) when is_integer(key)

  @doc """
  Turns a namespace + key tuple into a key

  Expects namespace to be either an atom or binary, and key to be an integer

  If a key with more than 32 bits is given, **collisions between different
  namespaces may happen**.
  """
  def tuple_to_key({namespace, int}) when is_atom(namespace),
    do: tuple_to_key({Atom.to_string(namespace), int})

  def tuple_to_key({namespace, int}) when is_binary(namespace) do
    import Bitwise

    upper32 = :erlang.crc32(namespace)
    lower32 = int

    # To mimic uints, we use the signal to represent the last bit
    if upper32 > @max_i32 do
      -((upper32 - @u32_last_bit) <<< 32 ||| lower32)
    else
      upper32 <<< 32 ||| lower32
    end
  end

  @doc """
  Obtains an exclusive session-level advisory lock, waiting if necessary.
  """
  @spec advisory_lock(repo, key, Keyword.t()) :: :ok
  def advisory_lock(repo, key, opts \\ []) when is_key(key) do
    repo.query!("SELECT pg_advisory_lock($1)", [key], opts)
    :ok
  end

  @doc """
  Obtains a shared session-level advisory lock, waiting if necessary.
  """
  @spec advisory_lock_shared(repo, key, Keyword.t()) :: result
  def advisory_lock_shared(repo, key, opts \\ []) when is_key(key) do
    repo.query!("SELECT pg_advisory_lock_shared($1)", [key], opts)
    :ok
  end

  @doc """
  Frees an exclusive session-level advisory lock held by current session

  Returns `:ok` if it was successfully released or :error if it wasn't
  """
  @spec advisory_unlock(repo, key, Keyword.t()) :: result
  def advisory_unlock(repo, key, opts \\ []) when is_key(key) do
    repo.query!("SELECT pg_advisory_unlock($1)", [key], opts)
    |> handle_result()
  end

  @doc """
  Frees all session-level advisory locks held by current session
  """
  @spec advisory_unlock_all(repo, Keyword.t()) :: :ok
  def advisory_unlock_all(repo, opts \\ []) do
    repo.query!("SELECT pg_advisory_unlock_all()", [], opts)
    :ok
  end

  @doc """
  Frees a shared transaction-level advisory lock held by current session

  Returns `:ok` if it was successfully released or :error if it wasn't
  """
  @spec advisory_unlock_shared(repo, key, Keyword.t()) :: result
  def advisory_unlock_shared(repo, key, opts \\ []) when is_key(key) do
    repo.query!("SELECT pg_advisory_unlock_shared($1)", [key], opts)
    |> handle_result()
  end

  @doc """
  Obtains an exclusive transaction-level advisory lock, waiting if necessary.
  """
  @spec advisory_xact_lock(repo, key, Keyword.t()) :: :ok
  def advisory_xact_lock(repo, key, opts \\ []) when is_key(key) do
    repo.query!("SELECT pg_advisory_xact_lock($1)", [key], opts)
    :ok
  end

  @doc """
  Obtains a shared transaction-level advisory lock, waiting if necessary.
  """
  @spec advisory_xact_lock_shared(repo, key, Keyword.t()) :: :ok
  def advisory_xact_lock_shared(repo, key, opts \\ []) when is_key(key) do
    repo.query!("SELECT pg_advisory_xact_lock_shared($1)", [key], opts)
    :ok
  end

  @doc """
  Obtains a shared session-level advisory lock, returning :ok if it was aquired, or :error if it wasn't possible
  """
  @spec try_advisory_lock(repo, key, Keyword.t()) :: result
  def try_advisory_lock(repo, key, opts \\ []) do
    repo.query!("SELECT pg_try_advisory_lock($1)", [key], opts)
    |> handle_result()
  end

  @doc """
  Obtains a shared session-level advisory lock, returning :ok if it was aquired, or :error if it wasn't possible
  """
  @spec try_advisory_lock_shared(repo, key, Keyword.t()) :: result
  def try_advisory_lock_shared(repo, key, opts \\ []) do
    repo.query!("SELECT pg_try_advisory_lock_shared($1)", [key], opts)
    |> handle_result()
  end

  @doc """
  Obtains a shared transaction-level advisory xact_lock, returning :ok if it was aquired, or :error if it wasn't possible
  """
  @spec try_advisory_xact_lock(repo, key, Keyword.t()) :: result
  def try_advisory_xact_lock(repo, key, opts \\ []) do
    repo.query!("SELECT pg_try_advisory_xact_lock($1)", [key], opts)
    |> handle_result()
  end

  @doc """
  Obtains a shared transaction-level advisory xact_lock, returning :ok if it was aquired, or :error if it wasn't possible
  """
  @spec try_advisory_xact_lock_shared(repo, key, Keyword.t()) :: result
  def try_advisory_xact_lock_shared(repo, key, opts \\ []) do
    repo.query!("SELECT pg_try_advisory_xact_lock_shared($1)", [key], opts)
    |> handle_result()
  end

  defp handle_result(%Postgrex.Result{rows: [[true]]}), do: :ok
  defp handle_result(%Postgrex.Result{rows: [[false]]}), do: :error
end
