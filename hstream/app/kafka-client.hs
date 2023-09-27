module Main where

import           HStream.Kafka.Client.Cli

main :: IO ()
main = do
  parsed@(cmd, opts) <- runCliParser
  case cmd of
    Nodes                 -> handleCmdNodes opts
    Topics                -> handleCmdTopics opts
    DescribeTopic topic   -> handleCmdDescribeTopic opts topic
    Groups                -> handleCmdGroups opts
    DescribeGroup group   -> handleCmdDescribeGroup opts group
    DeleteTopic topicName -> handleDeleteTopic opts topicName
    CreateTopic topicName numPartitions replicationFactor ->
      handleCreateTopic opts topicName numPartitions replicationFactor
    Produce request       -> handleProduce opts request
