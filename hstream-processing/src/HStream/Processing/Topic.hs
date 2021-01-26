{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE StrictData        #-}

module HStream.Processing.Topic
  ( TopicName,
    Offset,
    Timestamp,
    RawConsumerRecord (..),
    RawProducerRecord (..),
    TopicConsumer (..),
    TopicProducer (..),
  )
where

import           HStream.Processing.Type
import           RIO
import qualified RIO.ByteString.Lazy     as BL
import qualified RIO.Text                as T

type TopicName = T.Text

type Offset = Word64

class TopicConsumer a where
  subscribe :: a -> [TopicName] -> IO a
  pollRecords :: a -> Int -> IO [RawConsumerRecord]

-- closeConsumer :: a -> IO ()

class TopicProducer a where
  -- createProducer :: c -> IO a
  send :: a -> RawProducerRecord -> IO ()

-- closeProducer :: a -> IO ()

data RawConsumerRecord
  = RawConsumerRecord
      { rcrTopic :: TopicName,
        rcrOffset :: Offset,
        rcrTimestamp :: Timestamp,
        rcrKey :: Maybe BL.ByteString,
        rcrValue :: BL.ByteString
      }

data RawProducerRecord
  = RawProducerRecord
      { rprTopic :: TopicName,
        rprKey :: Maybe BL.ByteString,
        rprValue :: BL.ByteString,
        rprTimestamp :: Timestamp
      }
