{-# LANGUAGE DataKinds #-}

module HStream.Kafka.Client.Api
  ( clientId
    -- * Oneshot RPC
  , withSendAndRecv
  , metadata
  , createTopics
  , deleteTopics
  , produce
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

produce
  :: Int32
  -> K.ProduceRequestV2
  -> ClientHandler -> IO K.ProduceResponseV2
produce correlationId req h = snd <$> do
  sendAndRecv (K.RPC :: K.RPC K.HStreamKafkaV2 "produce")
              correlationId (Just clientId) Nothing req h
