module Kafka.Protocol.Message.Common where

-------------------------------------------------------------------------------
-- TODO: Generate by kafka message json schema

import           Data.Int

import           Kafka.Protocol.Encoding (Serializable)

newtype ApiKey = ApiKey Int16
  deriving newtype (Num, Integral, Real, Enum, Ord, Eq, Bounded, Serializable)
instance Show ApiKey where
  show (ApiKey 0)  = "Produce(0)"
  show (ApiKey 1)  = "Fetch(1)"
  show (ApiKey 2)  = "ListOffsets(2)"
  show (ApiKey 3)  = "Metadata(3)"
  show (ApiKey 8)  = "OffsetCommit(8)"
  show (ApiKey 9)  = "OffsetFetch(9)"
  show (ApiKey 10) = "FindCoordinator(10)"
  show (ApiKey 11) = "JoinGroup(11)"
  show (ApiKey 12) = "Heartbeat(12)"
  show (ApiKey 13) = "LeaveGroup(13)"
  show (ApiKey 14) = "SyncGroup(14)"
  show (ApiKey 15) = "DescribeGroups(15)"
  show (ApiKey 16) = "ListGroups(16)"
  show (ApiKey 18) = "ApiVersions(18)"
  show (ApiKey 19) = "CreateTopics(19)"
  show (ApiKey 20) = "DeleteTopics(20)"
  show (ApiKey n)  = "Unknown " <> show n
