Logger.configure(level: :warning)
:net_kernel.start([:"stagehand_test@127.0.0.1"])
ExUnit.start()
