{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler.Common where

import           Control.Concurrent
import           Control.Exception                (Handler (Handler),
                                                   SomeException (..), catches,
                                                   onException, throw)
import           Control.Exception.Base           (AsyncException (..))
import           Control.Monad
import qualified Data.Aeson                       as Aeson
import qualified Data.HashMap.Strict              as HM
import           Data.Int                         (Int64)
import           Data.Maybe                       (fromJust)
import           Data.Text                        (Text)
import qualified Data.Time                        as Time
import           Network.GRPC.HighLevel.Generated
import           Network.GRPC.LowLevel.Op         (Op (OpRecvCloseOnServer),
                                                   OpRecvResult (OpRecvCloseOnServerResult),
                                                   runOps)

import qualified HStream.Logger                   as Log
import           HStream.Server.ConnectorTypes    (SinkConnector (..),
                                                   SinkRecord (..),
                                                   SourceConnectorWithoutCkp (..),
                                                   SourceRecord (..),
                                                   TemporalFilter (..))
import qualified HStream.Server.ConnectorTypes    as HCT
import qualified HStream.Server.HStore            as HStore
import           HStream.Server.HStreamApi
import qualified HStream.Server.MetaData          as P
import           HStream.Server.Types
import           HStream.SQL.AST
import           HStream.SQL.Codegen
import           HStream.Utils                    (TaskStatus (..),
                                                    newRandomText)

import qualified DiffFlow.Graph                   as DiffFlow
import qualified DiffFlow.Shard                   as DiffFlow
import qualified DiffFlow.Types                   as DiffFlow
import qualified DiffFlow.Weird                   as DiffFlow
import qualified HStream.Exception                as HE

import Debug.Trace

runTaskWrapper :: ServerContext
               -> Text
--               -> [(DiffFlow.Node, Text)]
--               -> (DiffFlow.Node, Text)
--               -> Maybe RWindow
               -> Text
               -> [In]
               -> Out
               -> DiffFlow.GraphBuilder FlowObject
               -> Maybe (MVar (DiffFlow.DataChangeBatch FlowObject HCT.Timestamp))
               -> IO ()
runTaskWrapper ctx taskName sinkStream ins out graphBuilder accumulation = do
  let consumerName = taskName

  sourceConnectors <- forM ins
    (\_ -> do
      -- create a new sourceConnector
      return $ HStore.hstoreSourceConnectorWithoutCkp ctx consumerName
    )

  -- create a new sinkConnector
  let sinkConnector = HStore.hstoreSinkConnector ctx

  -- build graph then shard
  let graph = DiffFlow.buildGraph graphBuilder
  shard <- DiffFlow.buildShard graph
{-
  let temporalFilter = case window of
        Just (RTumblingWindow interval) ->
          let interval_ms = (Time.diffTimeToPicoseconds interval) `div` (1000 * 1000 * 1000)
           in Tumbling (fromIntegral interval_ms)
        Just (RHoppingWindow len hop) ->
          let len_ms = (Time.diffTimeToPicoseconds len) `div` (1000 * 1000 * 1000)
              hop_ms = (Time.diffTimeToPicoseconds hop) `div` (1000 * 1000 * 1000)
           in Hopping (fromIntegral len_ms) (fromIntegral hop_ms)
        Just (RSlidingWindow interval) ->
          let interval_ms = (Time.diffTimeToPicoseconds interval) `div` (1000 * 1000 * 1000)
           in Sliding (fromIntegral interval_ms)
        Nothing -> NoFilter
-}
  -- RUN TASK
  runTask sinkStream ins out sourceConnectors sinkConnector accumulation shard

--------------------------------------------------------------------------------
runTask :: Text
        -> [In]
        -> Out
        -> [SourceConnectorWithoutCkp]
        -> SinkConnector
        -> Maybe (MVar (DiffFlow.DataChangeBatch FlowObject HCT.Timestamp))
        -> DiffFlow.Shard FlowObject HCT.Timestamp
        -> IO ()
runTask sinkStream ins out sourceConnectors sinkConnector accumulation shard = do

  -- the task itself
  tid1 <- forkIO $ DiffFlow.run shard

  -- subscribe to all source streams
  forM_ (sourceConnectors `zip` ins)
    (\(SourceConnectorWithoutCkp{..}, In{..}) ->
        subscribeToStreamWithoutCkp inStream SpecialOffsetLATEST
    )

  -- main loop: push input to INPUT nodes
  tids2 <- forM (sourceConnectors `zip` ins)
    (\(SourceConnectorWithoutCkp{..}, In{..}) -> do
        forkIO $ withReadRecordsWithoutCkp inStream $ \sourceRecords -> do
          forM_ sourceRecords $ \SourceRecord{..} -> do
            ts <- HCT.getCurrentTimestamp
            let dataChange
                  = DiffFlow.DataChange
                  { dcRow = jsonObjectToFlowObject . fromJust . Aeson.decode $ srcValue
                  , dcTimestamp = DiffFlow.Timestamp ts [] -- Timestamp srcTimestamp []
                  , dcDiff = 1
                  }
            Log.debug . Log.buildString $ "Get input: " <> show dataChange
            DiffFlow.pushInput shard inNode dataChange -- original update
            -- insert new negated updates to limit the valid range of this update
            let insert_ms = DiffFlow.timestampTime (DiffFlow.dcTimestamp dataChange)
{-
            case temporalFilter of
              NoFilter -> return ()
              Tumbling interval_ms -> do
                let _start_ms = interval_ms * (insert_ms `div` interval_ms)
                    end_ms    = interval_ms * (1 + insert_ms `div` interval_ms)
                let negatedDataChange = dataChange
                                        { DiffFlow.dcTimestamp = DiffFlow.Timestamp end_ms []
                                        , DiffFlow.dcDiff = -1
                                        }
                DiffFlow.pushInput shard inNode negatedDataChange -- negated update
              Hopping interval_ms hop_ms -> do
                -- FIXME: determine the accurate semantic of HOPPING window
                let _start_ms = hop_ms * (insert_ms `div` hop_ms)
                    end_ms = interval_ms + hop_ms * (insert_ms `div` hop_ms)
                let negatedDataChange = dataChange
                                        { DiffFlow.dcTimestamp = DiffFlow.Timestamp end_ms []
                                        , DiffFlow.dcDiff = -1
                                        }
                DiffFlow.pushInput shard inNode negatedDataChange -- negated update
              Sliding interval_ms -> do
                let _start_ms = insert_ms
                    end_ms    = insert_ms + interval_ms
                let negatedDataChange = dataChange
                                        { DiffFlow.dcTimestamp = DiffFlow.Timestamp end_ms []
                                        , DiffFlow.dcDiff = -1
                                        }
                DiffFlow.pushInput shard inNode negatedDataChange -- negated update
              _ -> return ()
-}
            DiffFlow.flushInput shard inNode
    )

  -- second loop: advance input after an interval
  tid3 <- forkIO . forever $ do
    forM_ ins $ \In{..} -> do
      ts <- HCT.getCurrentTimestamp
      -- Log.debug . Log.buildString $ "### Advance time to " <> show ts
      DiffFlow.advanceInput shard inNode (DiffFlow.Timestamp ts [])
    threadDelay 100000

  -- third loop: push output from OUTPUT node to output stream
  accumulatedOutput <- case accumulation of
                         Nothing -> newMVar DiffFlow.emptyDataChangeBatch
                         Just m  -> return m

  forever (do
    DiffFlow.popOutput shard (outNode out) $ \dcb@DiffFlow.DataChangeBatch{..} -> do
      Log.debug . Log.buildString $ "~~~ POPOUT: " <> show dcb
      forM_ dcbChanges $ \change -> do
        Log.debug . Log.buildString $ "<<< this change: " <> show change
        modifyMVar_ accumulatedOutput
          (\dcb -> return $ DiffFlow.updateDataChangeBatch' dcb (\xs -> xs ++ [change]))
        when (DiffFlow.dcDiff change > 0) $ do
          let sinkRecord = SinkRecord
                { snkStream = sinkStream
                , snkKey = Nothing
                , snkValue = (Aeson.encode . flowObjectToJsonObject) (DiffFlow.dcRow change)
                , snkTimestamp = DiffFlow.timestampTime (DiffFlow.dcTimestamp change)
                }
          replicateM_ (DiffFlow.dcDiff change) $ writeRecord sinkConnector sinkRecord
          ) `onException` (do
    let childrenThreads = tid1 : tids2 ++ [tid3]
    mapM_ killThread childrenThreads
                          )

--------------------------------------------------------------------------------

handlePushQueryCanceled :: ServerCall () -> IO () -> IO ()
handlePushQueryCanceled ServerCall{..} handle = do
  x <- runOps unsafeSC callCQ [OpRecvCloseOnServer]
  case x of
    Left err   -> print err
    Right []   -> putStrLn "GRPCIOInternalUnexpectedRecv"
    Right [OpRecvCloseOnServerResult b]
      -> when b handle
    _ -> putStrLn "impossible happened"

--------------------------------------------------------------------------------
-- GRPC Handler Helper

-- TODO: return info in a more maintainable way
handleCreateAsSelect :: ServerContext
                     -> HStreamPlan
                     -> Text
                     -> Text
                     -> P.QueryType
                     -> IO (Text, Int64)
handleCreateAsSelect ctx@ServerContext{..} plan sinkStream commandQueryStmtText queryType = do
  taskName <- newRandomText 10
  (qid, timestamp) <- P.createInsertPersistentQuery
    taskName commandQueryStmtText queryType serverID metaHandle
  P.setQueryStatus qid Running zkHandle
  tid <- forkIO $ catches (action qid taskName) (cleanup qid)
  modifyMVar_ runningQueries (return . HM.insert qid tid)
  return (qid, timestamp)
  where
    (ins, out, builder, accumulation) = case plan of
        SelectPlan ins out builder -> (ins, out, builder, Nothing)
{-
        CreateBySelectPlan tName inNodesWithStreams outNodeWithStream win builder _ ->
          (tName,inNodesWithStreams,outNodeWithStream,win,builder, Nothing)
        CreateViewPlan tName _ inNodesWithStreams outNodeWithStream win builder accumulation ->
          (tName,inNodesWithStreams,outNodeWithStream,win,builder, Just accumulation)
        _ -> throw $ HE.WrongExecutionPlan "Only support select sql in the method called"
-}
    action qid taskName = do
      Log.debug . Log.buildString
        $ "CREATE AS SELECT: query " <> show qid
       <> " has stared working on " <> show commandQueryStmtText
      runTaskWrapper ctx taskName sinkStream ins out builder accumulation
    cleanup qid =
      [ Handler (\(e :: AsyncException) -> do
                    Log.debug . Log.buildString
                       $ "CREATE AS SELECT: query " <> show qid
                      <> " is killed because of " <> show e
                    P.setQueryStatus qid Terminated metaHandle
                    void $ releasePid qid)
      , Handler (\(e :: SomeException) -> do
                    Log.warning . Log.buildString
                       $ "CREATE AS SELECT: query " <> show qid
                      <> " died because of " <> show e
                    P.setQueryStatus qid ConnectionAbort metaHandle
                    void $ releasePid qid)
      ]
    releasePid qid = modifyMVar_ runningQueries (return . HM.delete qid)
