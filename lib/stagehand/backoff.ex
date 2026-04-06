defmodule Stagehand.Backoff do
  @moduledoc """
  Exponential backoff calculation for job retries.
  """

  @doc """
  Compute backoff delay in seconds for the given attempt number.

  Uses exponential backoff: `2^attempt + 15 + jitter`

  The jitter is a random value between 0 and `2^(attempt - 1)` to prevent
  thundering herd when many jobs fail simultaneously.
  """
  @spec compute(pos_integer()) :: pos_integer()
  def compute(attempt) when attempt > 0 do
    base = Integer.pow(2, attempt)
    jitter = :rand.uniform(max(1, Integer.pow(2, attempt - 1)))

    base + 15 + jitter
  end
end
