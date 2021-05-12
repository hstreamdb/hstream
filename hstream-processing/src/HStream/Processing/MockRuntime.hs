{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE StrictData        #-}

module HStream.Processing.MockRuntime
  ( runTask,
  )
where

import           HStream.Processing.MockStreamStore
import           HStream.Processing.Processor
import           HStream.Processing.Processor.Internal
import           HStream.Processing.Util
import           RIO
import qualified RIO.ByteString.Lazy                   as BL
import qualified RIO.HashMap                           as HM
import           RIO.HashMap.Partial                   as HM'
import qualified RIO.Text                              as T

runTask ::
  MockStreamStore ->
  Task ->
  IO ()
runTask mockStore task@Task {..} = do
  let sourceTopicNames = HM.keys taskSourceConfig
  consumer <- mkMockConsumer mockStore sourceTopicNames
  producer <- mkMockProducer mockStore
  runTaskInternal producer consumer
  where
    runTaskInternal ::
      MockProducer ->
      MockConsumer ->
      IO ()
    runTaskInternal topicProducer topicConsumer = do
      -- add InternalSink Node
      let newTaskTopologyForward =
            HM.foldlWithKey'
              ( \a k v@InternalSinkConfig {..} ->
                  let internalSinkProcessor = buildInternalSinkProcessor topicProducer v
                      ep = mkEProcessor internalSinkProcessor
                      (sinkProcessor, children) = taskTopologyForward HM'.! k
                      name = T.append iSinkTopicName "-INTERNAL-SINK"
                      tp = HM.insert k (sinkProcessor, children ++ [name]) a
                   in HM.insert name (ep, []) tp
              )
              taskTopologyForward
              taskSinkConfig
      logOptions <- logOptionsHandle stderr True
      withLogFunc logOptions $ \lf -> do
        ctx <- buildTaskContext task {taskTopologyForward = newTaskTopologyForward} lf
        forever $
          runRIO ctx $
            do
              logDebug "start iteration..."
              rawRecords <- liftIO $ pollRecords topicConsumer 100 2000
              logDebug $ "polled " <> display (length rawRecords) <> " records"
              forM_
                rawRecords
                ( \RawConsumerRecord {..} -> do
                    let acSourceName = iSourceName (taskSourceConfig HM'.! rcrTopic)
                    let (sourceEProcessor, _) = newTaskTopologyForward HM'.! acSourceName
                    liftIO $ updateTimestampInTaskContext ctx rcrTimestamp
                    runEP sourceEProcessor (mkERecord Record {recordKey = rcrKey, recordValue = rcrValue, recordTimestamp = rcrTimestamp})
                )

buildInternalSinkProcessor ::
  MockProducer ->
  InternalSinkConfig ->
  Processor BL.ByteString BL.ByteString
buildInternalSinkProcessor producer InternalSinkConfig {..} = Processor $ \Record {..} -> do
  ts <- liftIO getCurrentTimestamp
  liftIO $
    send
      producer
      RawProducerRecord
        { rprTopic = iSinkTopicName,
          rprKey = recordKey,
          rprValue = recordValue,
          rprTimestamp = ts
        }
