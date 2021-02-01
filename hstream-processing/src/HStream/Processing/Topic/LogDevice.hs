{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE StrictData        #-}

module HStream.Processing.Topic.LogDevice
  ( HStore.ProducerConfig (..),
    HStore.ConsumerConfig (..),
    HStore.mkProducer,
    mkConsumer,
  )
where

import           HStream.Processing.Topic.Type
import qualified HStream.Store                 as HStore
import           HStream.Store.Stream          (SequenceNum (..))
import           RIO
import qualified RIO.ByteString.Lazy           as BL
import qualified RIO.HashMap                   as HM
import           RIO.HashMap.Partial           as HM'
import qualified RIO.HashSet                   as HS
import qualified RIO.Text                      as T
import qualified Z.Data.CBytes                 as ZCB
import qualified Z.Foreign                     as ZF

convertRawProducerRecord :: RawProducerRecord -> HStore.ProducerRecord
convertRawProducerRecord RawProducerRecord {..} =
  HStore.ProducerRecord
    { dataInTopic = ZCB.pack $ T.unpack rprTopic,
      dataInKey = fmap (ZCB.fromBytes . ZF.fromByteString . BL.toStrict) rprKey,
      dataInValue = ZF.fromByteString $ BL.toStrict rprValue,
      dataInTimestamp = rprTimestamp
    }

toRawConsumerRecord :: HStore.ConsumerRecord -> RawConsumerRecord
toRawConsumerRecord HStore.ConsumerRecord {..} =
  RawConsumerRecord
    { rcrTopic = T.pack $ ZCB.unpack dataOutTopic,
      rcrOffset = unSequenceNum dataOutOffset,
      rcrTimestamp = dataOutTimestamp,
      rcrKey = fmap (BL.fromStrict . ZF.toByteString . ZCB.toBytes) dataOutKey,
      rcrValue = BL.fromStrict $ ZF.toByteString dataOutValue
    }

instance TopicProducer HStore.Producer where
  send producer record = HStore.sendMessage producer (convertRawProducerRecord record)

instance TopicConsumer HStore.Consumer where
  pollRecords consumer nrecords timeout = do
    records <- HStore.pollMessages consumer nrecords (fromIntegral timeout)
    return $ fmap toRawConsumerRecord records

mkConsumer :: HStore.ConsumerConfig -> [TopicName] -> IO HStore.Consumer
mkConsumer config topics = do
  let topicNames = fmap (ZCB.pack . T.unpack) topics
  HStore.mkConsumer config topicNames
