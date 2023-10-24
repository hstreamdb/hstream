module Main where

import           HStream.Base             (setupFatalSignalHandler)
import           HStream.Kafka.Client.Cli

main :: IO ()
main = do
  setupFatalSignalHandler
  (opts, cmd) <- runCliParser
  case cmd of
    TopicCommand c   -> handleTopicCommand opts c
    NodeCommand c    -> handleNodeCommand opts c
    ProduceCommand c -> handleProduceCommand opts c
    ConsumeCommand c -> handleConsumeCommand opts c
