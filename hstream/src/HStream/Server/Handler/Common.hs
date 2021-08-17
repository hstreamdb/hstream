{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler.Common where

import           Control.Concurrent               (MVar, ThreadId, forkIO,
                                                   killThread, putMVar,
                                                   readMVar, swapMVar, takeMVar)
import           Control.Concurrent.STM           (STM, TVar, atomically,
                                                   modifyTVar', readTVar,
                                                   writeTVar)
import           Control.Exception                (Handler (Handler),
                                                   SomeException (..), catches,
                                                   displayException,
                                                   onException, throwIO, try)
import           Control.Exception.Base           (AsyncException (..))
import           Control.Monad                    (forever, void, when)
import qualified Data.ByteString.Char8            as C
import           Data.Foldable                    (foldrM)
import qualified Data.HashMap.Strict              as HM
import           Data.Int                         (Int64)
import           Data.List                        (find)
import qualified Data.Map.Strict                  as M
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import           Database.ClickHouseDriver.Client (createClient)
import           Database.MySQL.Base              (ERRException)
import qualified Database.MySQL.Base              as MySQL
import           GHC.Conc                         (readTVarIO)
import           Network.GRPC.HighLevel.Generated
import           Network.GRPC.LowLevel.Op         (Op (OpRecvCloseOnServer),
                                                   OpRecvResult (OpRecvCloseOnServerResult),
                                                   runOps)
import qualified Z.Data.CBytes                    as CB
import           ZooKeeper.Types

import qualified Data.Aeson                       as Aeson
import           Data.IORef                       (IORef, atomicModifyIORef',
                                                   newIORef)
import           HStream.Connector.ClickHouse
import qualified HStream.Connector.HStore         as HCS
import           HStream.Connector.MySQL
import qualified HStream.Logger                   as Log
import           HStream.Processing.Connector
import           HStream.Processing.Processor     (TaskBuilder, getTaskName,
                                                   runTask)
import           HStream.Processing.Stream        (Materialized (..))
import           HStream.Processing.Type          (Offset (..), SinkRecord (..),
                                                   SourceRecord (..))
import           HStream.SQL.Codegen
import           HStream.Server.Exception
import           HStream.Server.HStreamApi        (Subscription)
import qualified HStream.Server.Persistence       as P
import qualified HStream.Store                    as HS
import qualified HStream.Store.Admin.API          as AA
import           HStream.ThirdParty.Protobuf      (Empty (Empty))
import           HStream.Utils                    (returnErrResp, returnResp,
                                                   textToCBytes)
import           System.IO.Unsafe                 (unsafePerformIO)

--------------------------------------------------------------------------------

groupbyStores :: IORef (HM.HashMap T.Text (Materialized Aeson.Object Aeson.Object SerMat))
groupbyStores = unsafePerformIO $ newIORef HM.empty
{-# NOINLINE groupbyStores #-}


checkpointRootPath :: CB.CBytes
checkpointRootPath = "/tmp/checkpoint"

type Timestamp = Int64

data ServerContext = ServerContext {
    scLDClient               :: HS.LDClient
  , scDefaultStreamRepFactor :: Int
  , zkHandle                 :: Maybe ZHandle
  , runningQueries           :: MVar (HM.HashMap CB.CBytes ThreadId)
  , runningConnectors        :: MVar (HM.HashMap CB.CBytes ThreadId)
  , subscribedReaders        :: SubscribedReaders
  , subscribeHeap            :: TVar (M.Map TL.Text Timestamp)
  , cmpStrategy              :: HS.Compression
  , headerConfig             :: AA.HeaderConfig AA.AdminAPI
}

runTaskWrapper :: HS.StreamType -> HS.StreamType -> TaskBuilder -> HS.LDClient -> IO ()
runTaskWrapper sourceType sinkType taskBuilder ldclient = do
  -- create a new ckpReader from ldclient
  let readerName = textToCBytes (getTaskName taskBuilder)
  -- FIXME: We are not sure about the number of logs we are reading here, so currently the max number of log is set to 1000
  ldreader <- HS.newLDRsmCkpReader ldclient readerName HS.checkpointStoreLogID 5000 1000 Nothing 10
  -- create a new sourceConnector
  let sourceConnector = HCS.hstoreSourceConnector ldclient ldreader sourceType
  -- create a new sinkConnector
  let sinkConnector = HCS.hstoreSinkConnector ldclient sinkType
  -- RUN TASK
  runTask sourceConnector sinkConnector taskBuilder

runSinkConnector
  :: ServerContext
  -> CB.CBytes -- ^ Connector Id
  -> SourceConnectorWithoutCkp
  -> SinkConnector
  -> IO ThreadId
runSinkConnector ServerContext{..} cid src connector = do
    P.withMaybeZHandle zkHandle $ P.setConnectorStatus cid P.Running
    forkIO $ forever (catches action cleanup)
  where
    writeToConnector c SourceRecord{..} =
      writeRecord c $ SinkRecord srcStream srcKey srcValue srcTimestamp
    action = readRecordsWithoutCkp src >>= mapM_ (writeToConnector connector)
    cleanup =
      [ Handler (\(_ :: ERRException) ->
                   do Log.debug "Sink connector thread died due to SQL errors"
                      P.withMaybeZHandle zkHandle $ P.setConnectorStatus cid P.ConnectionAbort)
      , Handler (\(e :: AsyncException) ->
                   do Log.debug . Log.buildString $ "Sink connector thread killed because of " <> show e
                      P.withMaybeZHandle zkHandle $ P.setConnectorStatus cid P.Terminated)
      ]

handlePushQueryCanceled :: ServerCall () -> IO () -> IO ()
handlePushQueryCanceled ServerCall{..} handle = do
  x <- runOps unsafeSC callCQ [OpRecvCloseOnServer]
  case x of
    Left err   -> print err
    Right []   -> putStrLn "GRPCIOInternalUnexpectedRecv"
    Right [OpRecvCloseOnServerResult b]
      -> when b handle
    _ -> putStrLn "impossible happened"

eitherToResponse :: Either SomeException () -> a -> IO (ServerResponse 'Normal a)
eitherToResponse (Left err) _   =
  returnErrResp StatusInternal $ StatusDetails (C.pack . displayException $ err)
eitherToResponse (Right _) resp =
  returnResp resp

responseWithErrorMsgIfNothing :: Maybe a -> StatusCode -> StatusDetails -> IO (ServerResponse 'Normal a)
responseWithErrorMsgIfNothing (Just resp) _ _ = return $ ServerNormalResponse (Just resp) [] StatusOk ""
responseWithErrorMsgIfNothing Nothing errCode msg = return $ ServerNormalResponse Nothing [] errCode msg

--------------------------------------------------------------------------------
-- GRPC Handler Helper

handleCreateSinkConnector
  :: ServerContext
  -> CB.CBytes -- ^ Connector Name
  -> T.Text -- ^ Source Stream Name
  -> ConnectorConfig -> IO P.PersistentConnector
handleCreateSinkConnector serverCtx@ServerContext{..} cid sName cConfig = do
  onException action cleanup
  where
    cleanup = do
      Log.debug "Create sink connector failed"
      P.withMaybeZHandle zkHandle $ P.setConnectorStatus cid P.CreationAbort

    action = do
      P.withMaybeZHandle zkHandle $ P.setConnectorStatus cid P.Creating
      Log.debug "Start creating sink connector"
      ldreader <- HS.newLDReader scLDClient 1000 Nothing
      let src = HCS.hstoreSourceConnectorWithoutCkp scLDClient ldreader
      subscribeToStreamWithoutCkp src sName Latest

      connector <- case cConfig of
        ClickhouseConnector config -> do
          Log.debug $ "Connecting to clickhouse with " <> Log.buildString (show config)
          clickHouseSinkConnector <$> createClient config
        MySqlConnector      config -> do
          Log.debug $ "Connecting to mysql with " <> Log.buildString (show config)
          mysqlSinkConnector      <$> MySQL.connect config
      P.withMaybeZHandle zkHandle $ P.setConnectorStatus cid P.Created
      Log.debug . Log.buildString . CB.unpack $ cid <> "Connected"

      tid <- runSinkConnector serverCtx cid src connector
      Log.debug . Log.buildString $ "Sink connector started running on thread#" <> show tid

      takeMVar runningConnectors >>= putMVar runningConnectors . HM.insert cid tid
      P.withMaybeZHandle zkHandle $ P.getConnector cid

-- TODO: return info in a more maintainable way
handleCreateAsSelect :: ServerContext
                     -> TaskBuilder
                     -> TL.Text
                     -> P.QueryType
                     -> HS.StreamType
                     -> IO (CB.CBytes, Int64)
handleCreateAsSelect ServerContext{..} taskBuilder commandQueryStmtText queryType sinkType = do
  (qid, timestamp) <- P.createInsertPersistentQuery
    (getTaskName taskBuilder) (TL.toStrict commandQueryStmtText) queryType zkHandle
  tid <- forkIO $ P.withMaybeZHandle zkHandle (P.setQueryStatus qid P.Running)
        >> runTaskWrapper HS.StreamTypeStream sinkType taskBuilder scLDClient
  takeMVar runningQueries >>= putMVar runningQueries . HM.insert qid tid
  return (qid, timestamp)

handleTerminateConnector :: ServerContext -> CB.CBytes -> IO ()
handleTerminateConnector ServerContext{..} cid = do
  hmapC <- readMVar runningConnectors
  case HM.lookup cid hmapC of
    Just tid -> do
      void $ killThread tid >> swapMVar runningConnectors (HM.delete cid hmapC)
      Log.debug . Log.buildString $ "terminated connector: " <> show cid
    _        -> throwIO ConnectorNotExist

dropHelper :: ServerContext -> T.Text -> Bool -> Bool
  -> IO (ServerResponse 'Normal Empty)
dropHelper sc@ServerContext{..} name checkIfExist isView = do
  when isView $ atomicModifyIORef' groupbyStores (\hm -> (HM.delete name hm, ()))
  let sName = if isView then HCS.transToViewStreamName name else HCS.transToStreamName name
  streamExists <- HS.doesStreamExists scLDClient sName
  if streamExists
    then terminateQueryAndRemove sc (textToCBytes name)
      >> terminateRelatedQueries sc (textToCBytes name)
      >> HS.removeStream scLDClient sName
      >> returnResp Empty
    else if checkIfExist
           then returnResp Empty
           else returnErrResp StatusInternal "Object does not exist"

--------------------------------------------------------------------------------
-- Query

terminateQueryAndRemove :: ServerContext -> CB.CBytes -> IO ()
terminateQueryAndRemove sc@ServerContext{..} objectId = do
  queries <- P.withMaybeZHandle zkHandle P.getQueries
  let queryExists = find (\query -> P.getQuerySink query == objectId) queries
  case queryExists of
    Just query ->
      handleQueryTerminate sc (OneQuery $ P.queryId query)
      >> P.withMaybeZHandle zkHandle (P.removeQuery' $ P.queryId query)
    Nothing    -> pure ()

terminateRelatedQueries :: ServerContext -> CB.CBytes -> IO ()
terminateRelatedQueries sc@ServerContext{..} name = do
  queries <- P.withMaybeZHandle zkHandle P.getQueries
  let getRelatedQueries= [P.queryId query | query <- queries, name `elem` P.getRelatedStreams query]
  mapM_ (handleQueryTerminate sc . OneQuery) getRelatedQueries

handleQueryTerminate :: ServerContext -> TerminationSelection -> IO [CB.CBytes]
handleQueryTerminate ServerContext{..} (OneQuery qid) = do
  hmapQ <- readMVar runningQueries
  case HM.lookup qid hmapQ of Just tid -> killThread tid; _ -> pure ()
  P.withMaybeZHandle zkHandle $ P.setQueryStatus qid P.Terminated
  void $ swapMVar runningQueries (HM.delete qid hmapQ)
  return [qid]
handleQueryTerminate sc@ServerContext{..} AllQueries = do
  hmapQ <- readMVar runningQueries
  handleQueryTerminate sc (ManyQueries $ HM.keys hmapQ)
handleQueryTerminate ServerContext{..} (ManyQueries qids) = do
  hmapQ <- readMVar runningQueries
  (qids', hmapQ') <- foldrM action ([], hmapQ) qids
  void $ swapMVar runningQueries hmapQ'
  return qids'
  where
    action x (terminatedQids, hm) = do
      result <- try $ do
        case HM.lookup x hm of Just tid -> killThread tid; _ -> pure ()
        P.withMaybeZHandle zkHandle (P.setQueryStatus x P.Terminated)
      case result of
        Left (_ ::SomeException) -> return (terminatedQids, hm)
        Right _                  -> return (x:terminatedQids, HM.delete x hm)

--------------------------------------------------------------------------------
-- Subscription

data ReaderStatus = Released | Occupied deriving (Show)
-- | SubscribedReaders is an map, Map: { subscriptionId : (LDSyncCkpReader, Subscription) }
type SubscribedReaders = TVar (HM.HashMap TL.Text ReaderMap)
-- When the value of ReaderMap is None, it is used to indicate that the current value is a placeholder
data ReaderMap = None | ReaderMap HS.LDSyncCkpReader Subscription ReaderStatus deriving (Show)

getReaderStatus :: SubscribedReaders -> TL.Text -> STM (Maybe ReaderStatus)
getReaderStatus readers subscriptionId = do
  mp <- readTVar readers
  case HM.lookup subscriptionId mp of
    Just (ReaderMap _ _ s) -> return $ Just s
    _                      -> return Nothing

updateReaderStatus :: SubscribedReaders -> ReaderStatus -> TL.Text -> STM ()
updateReaderStatus readers status subscriptionId  = do
  hm <- readTVar readers
  case HM.lookup subscriptionId hm of
    Just (ReaderMap rd sId _) -> do
      let newMap = HM.insert subscriptionId (ReaderMap rd sId status) hm
      writeTVar readers newMap
    _ -> return ()

lookupSubscribedReaders :: SubscribedReaders -> TL.Text -> IO (HS.LDSyncCkpReader, Subscription)
lookupSubscribedReaders readers subscriptionId = do
  hm <- readTVarIO readers
  case HM.lookup subscriptionId hm of
    Just (ReaderMap reader subscription _) -> return (reader, subscription)
    _                                      -> throwIO SubscriptionIdNotFound

-- | Modify the SubscribedReaders strictly. If key exist in map, return false, otherwise insert the key value pair
insertSubscribedReaders :: SubscribedReaders -> TL.Text -> ReaderMap -> IO Bool
insertSubscribedReaders readers subscriptionId readerMap = atomically $ do
  hm <- readTVar readers
  case HM.lookup subscriptionId hm of
    Just _ -> return False
    Nothing -> do
      let newMap = HM.insert subscriptionId readerMap hm
      writeTVar readers newMap
      return True

-- | Update the subscribedReaders. If key is existed in map, the old value will be replaced by new value
updateSubscribedReaders :: SubscribedReaders -> TL.Text -> ReaderMap -> STM ()
updateSubscribedReaders readers subscriptionId readerMap = do
  modifyTVar' readers $ \hm -> HM.insert subscriptionId readerMap hm

deleteSubscribedReaders :: SubscribedReaders -> TL.Text -> STM ()
deleteSubscribedReaders readers subscriptionId = do
  modifyTVar' readers $ \hm -> HM.delete subscriptionId hm

subscribedReadersToMap :: SubscribedReaders -> IO (HM.HashMap TL.Text (HS.LDSyncCkpReader, Subscription))
subscribedReadersToMap readers = atomically $ do
  hm <- readTVar readers
  if HM.null hm
  then return HM.empty
  else return $
    HM.mapMaybe (\case None                   -> Nothing
                       ReaderMap reader sId _ -> Just (reader, sId)
                ) hm
