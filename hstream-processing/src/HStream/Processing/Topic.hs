{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE StrictData        #-}

module HStream.Processing.Topic
  ( module HStream.Processing.Topic.MockStore,
    module HStream.Processing.Topic.Type,
    module HStream.Processing.Topic.LogDevice,
    MessageStoreType (..),
  )
where

import           HStream.Processing.Topic.LogDevice
import           HStream.Processing.Topic.MockStore
import           HStream.Processing.Topic.Type

data MessageStoreType
  = Mock MockTopicStore
  | LogDevice ProducerConfig ConsumerConfig
  | Kafka
