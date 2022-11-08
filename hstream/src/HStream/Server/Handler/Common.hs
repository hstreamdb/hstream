{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler.Common where

import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Exception                (Handler (Handler),
                                                   SomeException (..), catches,
                                                   onException, throw, throwIO)
import           Control.Exception.Base           (AsyncException (..))
import           Control.Monad
import qualified Data.Aeson                       as Aeson
import qualified Data.HashMap.Strict              as HM
import           Data.Int                         (Int64)
import           Data.IORef
import           Data.Maybe                       (catMaybes, fromJust)
import           Data.Text                        (Text)
import qualified Data.Time                        as Time
import           Network.GRPC.HighLevel.Generated
import           Network.GRPC.LowLevel.Op         (Op (OpRecvCloseOnServer),
                                                   OpRecvResult (OpRecvCloseOnServerResult),
                                                   runOps)

import qualified HStream.Exception                as HE
import qualified HStream.Logger                   as Log
import           HStream.Server.ConnectorTypes    (SinkConnector (..),
                                                   SinkRecord (..),
                                                   SourceConnectorWithoutCkp (..),
                                                   SourceRecord (..))
import qualified HStream.Server.ConnectorTypes    as HCT
import qualified HStream.Server.HStore            as HStore
import           HStream.Server.HStreamApi
import qualified HStream.Server.MetaData          as P
import           HStream.Server.Types
import           HStream.SQL.AST
import           HStream.SQL.Codegen
import qualified HStream.Store                    as S
import           HStream.Utils                    (TaskStatus (..),
                                                   newRandomText)

import qualified DiffFlow.Graph                   as DiffFlow
import qualified DiffFlow.Shard                   as DiffFlow
import qualified DiffFlow.Types                   as DiffFlow
import qualified DiffFlow.Weird                   as DiffFlow
import qualified HStream.Exception                as HE

-------------------------------------------------------------------------------
data IdentifierRole = RoleStream | RoleView deriving (Eq, Show)

findIdentifierRole :: ServerContext -> Text -> IO (Maybe IdentifierRole)
findIdentifierRole ServerContext{..} name = do
  isStream <- S.doesStreamExist scLDClient (transToStreamName name)
  case isStream of
    True  -> return (Just RoleStream)
    False -> do
      hm <- readIORef P.groupbyStores
      case HM.lookup name hm of
        Nothing -> return Nothing
        Just _  -> return (Just RoleView)

--------------------------------------------------------------------------------
applyTempFilter :: DiffFlow.Shard Row Int64 -> In -> DiffFlow.DataChange Row Int64 -> IO ()
applyTempFilter shard In{..} dataChange = do
  let insert_ms = DiffFlow.timestampTime (DiffFlow.dcTimestamp dataChange)
  case inWindow of
    Nothing -> return ()
    Just (Tumbling interval) -> do
      let interval_ms = calendarDiffTimeToMs interval
      let _start_ms = interval_ms * (insert_ms `div` interval_ms)
          end_ms    = interval_ms * (1 + insert_ms `div` interval_ms)
      let negatedDataChange = dataChange
                              { DiffFlow.dcTimestamp = DiffFlow.Timestamp end_ms []
                              , DiffFlow.dcDiff = -1
                              }
      DiffFlow.pushInput shard inNode negatedDataChange -- negated update
    Just (Hopping interval hop) -> do
      let interval_ms = calendarDiffTimeToMs interval
          hop_ms      = calendarDiffTimeToMs hop
      -- FIXME: determine the accurate semantic of HOPPING window
      let _start_ms = hop_ms * (insert_ms `div` hop_ms)
          end_ms = interval_ms + hop_ms * (insert_ms `div` hop_ms)
      let negatedDataChange = dataChange
                              { DiffFlow.dcTimestamp = DiffFlow.Timestamp end_ms []
                              , DiffFlow.dcDiff = -1
                              }
      DiffFlow.pushInput shard inNode negatedDataChange -- negated update
    Just (Sliding interval) -> do
      let interval_ms = calendarDiffTimeToMs interval
      let _start_ms = insert_ms
          end_ms    = insert_ms + interval_ms
      let negatedDataChange = dataChange
                              { DiffFlow.dcTimestamp = DiffFlow.Timestamp end_ms []
                              , DiffFlow.dcDiff = -1
                              }
      DiffFlow.pushInput shard inNode negatedDataChange -- negated update
    _ -> return ()



--------------------------------------------------------------------------------
runTask :: ServerContext
        -> Text
        -> Text
        -> [(In, IdentifierRole)]
        -> (Out, IdentifierRole)
        -> DiffFlow.GraphBuilder FlowObject
        -> IO ()
runTask ctx@ServerContext{..} taskName sink insWithRole outWithRole graphBuilder = do
  ------------------
  let consumerName = taskName
  let graph = DiffFlow.buildGraph graphBuilder
  shard <- DiffFlow.buildShard graph
  stop_m <- newEmptyMVar

  ------------------
  -- the task itself
  tid1 <- forkIO $ DiffFlow.run shard stop_m

  ------------------
  -- In
  srcConnectors_m <- forM insWithRole $ \(In{..}, role) -> do
    case role of
      RoleStream -> do
        let connector@SourceConnectorWithoutCkp{..} = HStore.hstoreSourceConnectorWithoutCkp ctx consumerName
        subscribeToStreamWithoutCkp inStream SpecialOffsetLATEST
        return (Just connector)
      RoleView -> return Nothing

  tids2_m <- forM (insWithRole `zip` srcConnectors_m) $ \((in_@In{..}, role), srcConnector_m) -> do
    case role of
      RoleStream -> do
        -- Note: the N_th input DataChange of each input node has the same `dcExtra`
        extra_m <- newMVar 0
        let (Just SourceConnectorWithoutCkp{..}) = srcConnector_m
        tid <- forkIO $ withReadRecordsWithoutCkp inStream $ \sourceRecords -> do
          forM_ sourceRecords $ \SourceRecord{..} -> do
            ts <- HCT.getCurrentTimestamp
            extra <- modifyMVar extra_m (\x -> return (x+1, x))
            let dataChange
                  = DiffFlow.DataChange
                    { dcRow = (jsonObjectToFlowObject srcStream) . fromJust . Aeson.decode $ srcValue
                    , dcTimestamp = DiffFlow.Timestamp ts [] -- Timestamp srcTimestamp []
                    , dcDiff = 1
                    , dcExtra = fromIntegral extra
                    }
            Log.debug . Log.buildString $ "Get input: " <> show dataChange
            DiffFlow.pushInput shard inNode dataChange -- original update
            -- insert new negated updates to limit the valid range of this update
            applyTempFilter shard in_ dataChange
            DiffFlow.flushInput shard inNode
        return (Just tid)
      RoleView -> do
        viewStore_m <- readIORef P.groupbyStores >>= \hm -> return (hm HM.! inStream)
        dcb <- readMVar viewStore_m
        forM (DiffFlow.dcbChanges dcb) $ \change -> do
          ts <- HCT.getCurrentTimestamp
          let thisChange = change { DiffFlow.dcTimestamp = DiffFlow.Timestamp ts [] }
          Log.debug . Log.buildString $ "Get input(from view): " <> show thisChange
          DiffFlow.pushInput shard inNode thisChange
        return Nothing

  ------------------
  -- second loop: advance input after an interval
  tid3 <- forkIO . forever $ do
    forM_ insWithRole $ \(In{..}, _) -> do
      ts <- HCT.getCurrentTimestamp
      -- Log.debug . Log.buildString $ "### Advance time to " <> show ts
      DiffFlow.advanceInput shard inNode (DiffFlow.Timestamp ts [])
    threadDelay 100000

  -- third loop: push output from OUTPUT node to output stream
  let (out, outRole) = outWithRole
  forever (do
    DiffFlow.popOutput shard (outNode out) $ \dcb@DiffFlow.DataChangeBatch{..} -> do
      Log.debug . Log.buildString $ "~~~ POPOUT: " <> show dcb
      case outRole of
        RoleStream -> do
          let SinkConnector{..} = HStore.hstoreSinkConnector ctx
          forM_ dcbChanges $ \change -> do
            Log.debug . Log.buildString $ "<<< this change: " <> show change
            when (DiffFlow.dcDiff change > 0) $ do
              let sinkRecord = SinkRecord
                    { snkStream = sink
                    , snkKey = Nothing
                    , snkValue = (Aeson.encode . flowObjectToJsonObject) (DiffFlow.dcRow change)
                    , snkTimestamp = DiffFlow.timestampTime (DiffFlow.dcTimestamp change)
                    }
              replicateM_ (DiffFlow.dcDiff change) $ writeRecord sinkRecord
        RoleView -> do
          viewStore_m <- readIORef P.groupbyStores >>= \hm -> return (hm HM.! sink)
          modifyMVar_ viewStore_m
            (\old -> return $ DiffFlow.updateDataChangeBatch' old (\xs -> xs ++ dcbChanges))
          ) `onException` (do
    let childrenThreads = tid1 : (catMaybes tids2_m) ++ [tid3]
    mapM_ killThread childrenThreads
                          )

runImmTask :: ServerContext
           -> [(In, IdentifierRole)]
           -> Out
           -> MVar (DiffFlow.DataChangeBatch Row Int64)
           -> DiffFlow.GraphBuilder FlowObject
           -> IO ()
runImmTask ctx@ServerContext{..} insWithRole out out_m graphBuilder = do
  let graph = DiffFlow.buildGraph graphBuilder
  shard <- DiffFlow.buildShard graph
  stop_m <- newEmptyMVar

  -- run DiffFlow shard
  task_async <- async $ DiffFlow.run shard stop_m

  -- In
  forM_ insWithRole $ \(in_@In{..}, role) -> do
    case role of
      RoleView -> do
        viewStore_m <- readIORef P.groupbyStores >>= \hm -> return (hm HM.! inStream)
        dcb <- readMVar viewStore_m
        forM (DiffFlow.dcbChanges dcb) $ \change -> do
          ts <- HCT.getCurrentTimestamp
          let thisChange = change { DiffFlow.dcTimestamp = DiffFlow.Timestamp ts [] }
          Log.debug . Log.buildString $ "Get input(from view): " <> show thisChange
          DiffFlow.pushInput shard inNode thisChange
          -- insert new negated updates to limit the valid range of this update
          applyTempFilter shard in_ thisChange
          DiffFlow.flushInput shard inNode
      RoleStream ->
        throwIO $ HE.InvalidSqlStatement "Can not perform non-pushing SELECT from streams. "

  -- advance input
  forM_ insWithRole $ \(In{..}, _) -> replicateM_ 10 $ do
    ts <- HCT.getCurrentTimestamp
    -- Log.debug . Log.buildString $ "### Advance time to " <> show ts
    DiffFlow.advanceInput shard inNode (DiffFlow.Timestamp ts [])
    threadDelay 1000

  -- push output from OUTPUT node
  replicateM_ 5 $ do
    DiffFlow.popOutput shard (outNode out) $ \dcb@DiffFlow.DataChangeBatch{..} -> do
      Log.debug . Log.buildString $ "~~~ POPOUT: " <> show dcb
      modifyMVar_ out_m
        (\old -> return $ DiffFlow.updateDataChangeBatch' old (\xs -> xs ++ dcbChanges))
      threadDelay 100000

  -- stop DiffFlow.run
  putMVar stop_m ()
  wait task_async

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
                     -> Text
                     -> [(In, IdentifierRole)]
                     -> (Out, IdentifierRole)
                     -> DiffFlow.GraphBuilder Row
                     -> Text
                     -> P.QueryType
                     -> IO (Text, Int64)
handleCreateAsSelect ctx@ServerContext{..} sink insWithRole outWithRole builder commandQueryStmtText queryType = do
  taskName <- newRandomText 10
  (qid, timestamp) <- P.createInsertPersistentQuery
                      taskName commandQueryStmtText queryType serverID metaHandle
  P.setQueryStatus qid Running metaHandle
  tid <- forkIO $ catches (action qid taskName) (cleanup qid)
  modifyMVar_ runningQueries (return . HM.insert qid tid)
  return (qid, timestamp)
  where
    action qid taskName = do
      Log.debug . Log.buildString
        $ "CREATE AS SELECT: query " <> show qid
       <> " has stared working on " <> show commandQueryStmtText
      runTask ctx taskName sink insWithRole outWithRole builder
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
