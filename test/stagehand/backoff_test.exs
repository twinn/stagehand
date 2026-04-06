defmodule Stagehand.BackoffTest do
  use ExUnit.Case, async: true

  alias Stagehand.Backoff

  describe "compute/1" do
    test "returns increasing values for higher attempts" do
      # Run multiple times to account for jitter, check the trend
      values = for attempt <- 1..5, do: Backoff.compute(attempt)

      # Each value should be at least 2^attempt + 15
      for {value, attempt} <- Enum.zip(values, 1..5) do
        min_expected = Integer.pow(2, attempt) + 15
        assert value >= min_expected, "attempt #{attempt}: #{value} < #{min_expected}"
      end
    end

    test "always returns positive integers" do
      for attempt <- 1..20 do
        assert Backoff.compute(attempt) > 0
      end
    end
  end
end
