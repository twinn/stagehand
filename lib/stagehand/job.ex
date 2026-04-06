defmodule Stagehand.Job do
  @moduledoc """
  A struct representing a background job.
  """

  @type state ::
          :available
          | :scheduled
          | :executing
          | :completed
          | :retryable
          | :cancelled
          | :discarded

  @type t :: %__MODULE__{
          ref: reference() | nil,
          producer_pid: pid() | nil,
          state: state(),
          queue: binary(),
          worker: module(),
          args: map(),
          attempt: non_neg_integer(),
          max_attempts: pos_integer(),
          priority: 0..9,
          tags: [binary()],
          meta: map(),
          errors: [map()],
          unique: keyword() | nil,
          conflict?: boolean(),
          scheduled_at: DateTime.t() | nil,
          inserted_at: DateTime.t() | nil,
          attempted_at: DateTime.t() | nil
        }

  defstruct [
    :ref,
    :producer_pid,
    :worker,
    :scheduled_at,
    :inserted_at,
    :attempted_at,
    :unique,
    state: :available,
    queue: "default",
    args: %{},
    attempt: 0,
    max_attempts: 20,
    priority: 0,
    tags: [],
    meta: %{},
    errors: [],
    conflict?: false
  ]
end
