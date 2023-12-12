{-# LANGUAGE DataKinds #-}

module HStream.Kafka.Client.Api
  ( clientId
    -- * Oneshot RPC
  , withSendAndRecv
  , metadata
  , createTopics
  , deleteTopics
  , listGroups
  , describeGroups
  , hadminCommand
  ) where

import           Data.Int

import           HStream.Kafka.Network
import qualified Kafka.Protocol.Encoding as K
import qualified Kafka.Protocol.Message  as K
import qualified Kafka.Protocol.Service  as K

clientId :: K.NullableString
clientId  = Just "hstream-kafka-cli"

withSendAndRecv :: String -> Int -> (ClientHandler -> IO a) -> IO a
withSendAndRecv host port =
  withClient defaultClientOptions{host = host, port = port}

metadata
  :: Int32
  -> K.MetadataRequestV1
  -> ClientHandler -> IO K.MetadataResponseV1
metadata correlationId req h = snd <$> do
  sendAndRecv (K.RPC :: K.RPC K.HStreamKafkaV1 "metadata")
              correlationId (Just clientId) Nothing req h

createTopics
  :: Int32
  -> K.CreateTopicsRequestV0
  -> ClientHandler -> IO K.CreateTopicsResponseV0
createTopics correlationId req h = snd <$> do
  sendAndRecv (K.RPC :: K.RPC K.HStreamKafkaV0 "createTopics")
              correlationId (Just clientId) Nothing req h

deleteTopics
  :: Int32
  -> K.DeleteTopicsRequestV0
  -> ClientHandler -> IO K.DeleteTopicsResponseV0
deleteTopics correlationId req h = snd <$> do
  sendAndRecv (K.RPC :: K.RPC K.HStreamKafkaV0 "deleteTopics")
              correlationId (Just clientId) Nothing req h

listGroups
  :: Int32
  -> K.ListGroupsRequestV0
  -> ClientHandler
  -> IO K.ListGroupsResponseV0
listGroups correlationId req h = snd <$> do
  sendAndRecv (K.RPC :: K.RPC K.HStreamKafkaV0 "listGroups")
              correlationId (Just clientId) Nothing req h

describeGroups
  :: Int32
  -> K.DescribeGroupsRequestV0
  -> ClientHandler -> IO K.DescribeGroupsResponseV0
describeGroups correlationId req h = snd <$> do
  sendAndRecv (K.RPC :: K.RPC K.HStreamKafkaV0 "describeGroups")
              correlationId (Just clientId) Nothing req h

hadminCommand
  :: Int32
  -> K.HadminCommandRequestV0
  -> ClientHandler -> IO K.HadminCommandResponseV0
hadminCommand correlationId req h = snd <$> do
  sendAndRecv (K.RPC :: K.RPC K.HStreamKafkaV0 "hadminCommand")
              correlationId (Just clientId) (Just K.EmptyTaggedFields) req h
