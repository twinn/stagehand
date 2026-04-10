# Demonstrates Stagehand running across two nodes in a cluster.
#
# Open two terminals and run:
#
#   Terminal 1:
#     iex --sname node1 -S mix run examples/cluster_node.exs
#
#   Terminal 2:
#     iex --sname node2 -S mix run examples/cluster_node.exs
#
# Then in either IEx session:
#
#     Node.connect(:node1@hostname)  # or :node2@hostname
#
#     # Check that both nodes see each other's producers
#     Stagehand.Queue.Pipeline.producers_for_queue(Demo.Stagehand, "default")
#
#     # Insert jobs — they'll be routed across both nodes
#     for i <- 1..10 do
#       %{"id" => i} |> Demo.PrintWorker.new() |> then(&Stagehand.insert(Demo.Stagehand, &1))
#     end
#
#     # Test unique dedup across nodes
#     %{"key" => "once"} |> Demo.UniqueWorker.new() |> then(&Stagehand.insert(Demo.Stagehand, &1))
#
#     # Check queue status
#     Stagehand.check_queue(Demo.Stagehand, queue: :default)
#
#     # Stop one node (Ctrl+C twice) and watch the other pick up the work

IO.puts("This file contains instructions. Run examples/cluster_node.exs instead.")
IO.puts("See the comments at the top of this file for usage.")
