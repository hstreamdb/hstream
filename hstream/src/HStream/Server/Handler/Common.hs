{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedLists   #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.Server.Handler.Common where

import           Control.Concurrent               (MVar, ThreadId, forkIO,
                                                   killThread, putMVar,
                                                   readMVar, swapMVar, takeMVar)
import           Control.Concurrent.STM           (STM, TVar, atomically,
                                                   modifyTVar', readTVar,
                                                   writeTVar)
import           Control.Exception                (Exception, SomeException,
                                                   displayException, handle,
                                                   throwIO)

import           Control.Monad                    (void, when)
import qualified Data.ByteString.Char8            as C
import qualified Data.HashMap.Strict              as HM
import           Data.Int                         (Int64)
import qualified Data.Map.Strict                  as MP
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import qualified Database.MySQL.Base              as MySQL
import           Network.GRPC.HighLevel.Generated
import           Network.GRPC.LowLevel.Op         (Op (OpRecvCloseOnServer),
                                                   OpRecvResult (OpRecvCloseOnServerResult),
                                                   runOps)
import           RIO                              (forever)
import qualified Z.Data.CBytes                    as CB
import qualified Z.Data.Text                      as ZT
import           Z.IO.Time                        (SystemTime (..),
                                                   getSystemTime')
import           ZooKeeper.Types

import           Database.ClickHouseDriver.Client (createClient)
import           HStream.Connector.ClickHouse
import           HStream.Connector.HStore
import qualified HStream.Connector.HStore         as HCS
import           HStream.Connector.MySQL
import           HStream.Processing.Connector
import           HStream.Processing.Processor     (TaskBuilder, getTaskName,
                                                   runTask)
import           HStream.Processing.Type          (Offset (..), SinkRecord (..),
                                                   SourceRecord (..))
import           HStream.SQL.Codegen
import           HStream.Server.HStreamApi        (Subscription)
import qualified HStream.Server.Persistence       as HSP
import qualified HStream.Store                    as HS
import           HStream.Utils                    (textToCBytes)

checkpointRootPath :: CB.CBytes
checkpointRootPath = "/tmp/checkpoint"

data ReaderStatus = Released | Occupyied deriving (Show)

-- | SubscribedReaders is an map, Map: { subscriptionId : (LDSyncCkpReader, Subscription) }
type SubscribedReaders = TVar (HM.HashMap TL.Text ReaderMap)
data ReaderMap = None | ReaderMap HS.LDSyncCkpReader Subscription ReaderStatus deriving (Show)

getReaderStatus :: SubscribedReaders -> TL.Text -> STM (Maybe ReaderStatus)
getReaderStatus readers subscriptionId = do
  mp <- readTVar readers
  case HM.lookup subscriptionId mp of
    Nothing                -> return Nothing
    Just None              -> return Nothing
    Just (ReaderMap _ _ s) -> return $ Just s

updateReaderStatus :: SubscribedReaders -> ReaderStatus -> TL.Text -> STM ()
updateReaderStatus readers status subscriptionId  = do
  hm <- readTVar readers
  case HM.lookup subscriptionId hm of
    Just (ReaderMap rd sId _) -> do
      let newMap = HM.insert subscriptionId (ReaderMap rd sId status) hm
      writeTVar readers newMap
    _ -> return ()

type Timestamp = Int64

data ServerContext = ServerContext {
    scLDClient               :: HS.LDClient
  , scDefaultStreamRepFactor :: Int
  , zkHandle                 :: Maybe ZHandle
  , runningQueries           :: MVar (HM.HashMap CB.CBytes ThreadId)
  , runningConnectors        :: MVar (HM.HashMap CB.CBytes ThreadId)
  , subscribedReaders        :: SubscribedReaders
  , subscribeHeap            :: TVar (MP.Map TL.Text Timestamp)
  , cmpStrategy              :: HS.Compression
}

runTaskWrapper :: Bool -> TaskBuilder -> HS.LDClient -> IO ()
runTaskWrapper isTemp taskBuilder ldclient = do
  -- create a new ckpReader from ldclient
  let readerName = textToCBytes (getTaskName taskBuilder)
  -- FIXME: We are not sure about the number of logs we are reading here, so currently the max number of log is set to 1000
  ldreader <- HS.newLDRsmCkpReader ldclient readerName HS.checkpointStoreLogID 5000 1000 Nothing 10
  -- create a new sourceConnector
  let sourceConnector = HCS.hstoreSourceConnector ldclient ldreader
  -- create a new sinkConnector
  let sinkConnector = if isTemp then HCS.hstoreTempSinkConnector ldclient else HCS.hstoreSinkConnector ldclient
  -- RUN TASK
  runTask sourceConnector sinkConnector taskBuilder

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
eitherToResponse (Left err) _ = return $
  ServerNormalResponse Nothing [] StatusInternal $ StatusDetails (C.pack . displayException $ err)
eitherToResponse (Right _) resp = return $ ServerNormalResponse (Just resp) [] StatusOk ""

handleCreateSinkConnector :: ServerContext -> TL.Text -> T.Text -> T.Text -> ConnectorConfig -> IO (CB.CBytes, Int64)
handleCreateSinkConnector ServerContext{..} sql cName sName cConfig = do
    MkSystemTime timestamp _ <- getSystemTime'
    let cid = CB.pack $ T.unpack cName
        cinfo = HSP.Info (ZT.pack $ T.unpack $ TL.toStrict sql) timestamp
    HSP.withMaybeZHandle zkHandle $ HSP.insertConnector cid cinfo

    ldreader <- HS.newLDReader scLDClient 1000 Nothing
    let sc = hstoreSourceConnectorWithoutCkp scLDClient ldreader
    subscribeToStreamWithoutCkp sc sName Latest

    connector <- case cConfig of
      ClickhouseConnector config -> clickHouseSinkConnector <$> createClient config
      MySqlConnector config -> mysqlSinkConnector <$> MySQL.connect config
    tid <- forkIO $ do
      HSP.withMaybeZHandle zkHandle (HSP.setConnectorStatus cid HSP.Running)
      forever (readRecordsWithoutCkp sc >>= mapM_ (writeToConnector connector))
    takeMVar runningConnectors >>= putMVar runningConnectors . HM.insert cid tid
    return (cid, timestamp)
  where
    writeToConnector c SourceRecord{..} = writeRecord c $ SinkRecord srcStream srcKey srcValue srcTimestamp

-- TODO: return info in a more maintainable way
handleCreateAsSelect :: ServerContext -> TaskBuilder -> TL.Text -> HSP.QueryType -> IO (CB.CBytes, Int64)
handleCreateAsSelect ServerContext{..} taskBuilder commandQueryStmtText extra = do
  (qid, timestamp) <- HSP.createInsertPersistentQuery (getTaskName taskBuilder) commandQueryStmtText extra zkHandle
  tid <- forkIO $ HSP.withMaybeZHandle zkHandle (HSP.setQueryStatus qid HSP.Running)
        >> runTaskWrapper False taskBuilder scLDClient
  takeMVar runningQueries >>= putMVar runningQueries . HM.insert qid tid
  return (qid, timestamp)

mark :: (Exception e, Exception f) => (e -> f) -> IO a -> IO a
mark mke = handle (throwIO . mke)

handleTerminateConnector :: ServerContext -> CB.CBytes
  -> IO ()
handleTerminateConnector ServerContext{..} cid = do
  hmapC <- readMVar runningConnectors
  case HM.lookup cid hmapC of
    Just tid -> void $ killThread tid >> swapMVar runningConnectors (HM.delete cid hmapC)
    -- TODO: shall we throwIO here
    _        -> return ()
  -- TODO: shall we move this op to Just tid -> killThread tid
  void $ HSP.withMaybeZHandle zkHandle (HSP.setConnectorStatus cid HSP.Terminated)

lookupSubscribedReaders :: SubscribedReaders -> TL.Text -> IO (Maybe (HS.LDSyncCkpReader, Subscription))
lookupSubscribedReaders readers subscriptionId = atomically $ do
  hm <- readTVar readers
  case HM.lookup subscriptionId hm of
    Just None                            -> return Nothing
    Just (ReaderMap reader subscription _) -> return $ Just (reader, subscription)
    Nothing                              -> return Nothing

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

-- | The strick version of deleteSubscribedReaders, if the key doesn't exist, return error
deleteSubscribedReaders' :: SubscribedReaders -> TL.Text -> IO (Either String ())
deleteSubscribedReaders' readers subscriptionId = atomically $ do
  hm <- readTVar readers
  case HM.lookup subscriptionId hm of
    Just _ -> do
      let newMap = HM.delete subscriptionId hm
      writeTVar readers newMap
      return $ Right ()
    Nothing -> return $ Left "cann't find subscriptionId."

subscribedReadersToMap :: SubscribedReaders -> IO (HM.HashMap TL.Text (HS.LDSyncCkpReader, Subscription))
subscribedReadersToMap readers = atomically $ do
  hm <- readTVar readers
  if HM.null hm
  then return HM.empty
  else return $
    HM.mapMaybe (\case None                   -> Nothing
                       ReaderMap reader sId _ -> Just (reader, sId)
                ) hm

