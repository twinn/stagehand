Logger.configure(level: :warning)
{:ok, _} = :net_kernel.start([:"stagehand_test@127.0.0.1"])
ExUnit.start()
