{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedLists   #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.Server.Handler.Common where

import           Control.Concurrent               (MVar, ThreadId, forkIO,
                                                   putMVar, takeMVar)
import           Control.Exception                (Exception, SomeException,
                                                   displayException, handle,
                                                   throwIO)
import           Control.Monad                    (void, when)
import qualified Data.ByteString.Char8            as C
import qualified Data.HashMap.Strict              as HM
import           Data.Int                         (Int64)
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import qualified Database.MySQL.Base              as MySQL
import           Network.GRPC.HighLevel.Generated
import           Network.GRPC.LowLevel.Op         (Op (OpRecvCloseOnServer),
                                                   OpRecvResult (OpRecvCloseOnServerResult),
                                                   runOps)
import           RIO                              (async, forever)
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
import qualified HStream.Server.Persistence       as HSP
import qualified HStream.Store                    as HS
import           HStream.Utils                    (textToCBytes)

checkpointRootPath :: CB.CBytes
checkpointRootPath = "/tmp/checkpoint"

data ServerContext = ServerContext {
    scLDClient               :: HS.LDClient
  , scDefaultStreamRepFactor :: Int
  , zkHandle                 :: Maybe ZHandle
  , runningQueries           :: MVar (HM.HashMap CB.CBytes ThreadId)
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


createInsertPersistentQuery :: TaskName -> TL.Text -> HSP.QueryType -> Maybe ZHandle -> IO (CB.CBytes, Int64)
createInsertPersistentQuery taskName queryText extraInfo zkHandle = do
  MkSystemTime timestamp _ <- getSystemTime'
  let qid = case extraInfo of
        HSP.PlainQuery             -> ""
        HSP.StreamQuery streamName -> "stream_" <> streamName <> "-"
        HSP.ViewQuery   viewName _ -> "view_" <> viewName <> "-"
        <> CB.pack (T.unpack taskName)
      qinfo = HSP.Info (ZT.pack $ T.unpack $ TL.toStrict queryText) timestamp
  HSP.withMaybeZHandle zkHandle $ HSP.insertQuery qid qinfo extraInfo
  return (qid, timestamp)

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
eitherToResponse (Left err) resp = return $
  ServerNormalResponse resp [] StatusInternal $ StatusDetails (C.pack . displayException $ err)
eitherToResponse (Right _) resp = return $ ServerNormalResponse resp [] StatusOk ""

handleCreateSinkConnector :: ServerContext -> TL.Text -> T.Text -> T.Text -> ConnectorConfig -> IO ()
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
    void . async $ do
      HSP.withMaybeZHandle zkHandle (HSP.setConnectorStatus cid HSP.Running)
      forever (readRecordsWithoutCkp sc >>= mapM_ (writeToConnector connector))

  where
    writeToConnector c SourceRecord{..} = writeRecord c $ SinkRecord srcStream srcKey srcValue srcTimestamp

-- TODO: return info in a more maintainable way
handleCreateAsSelect :: ServerContext -> TaskBuilder -> TL.Text -> HSP.QueryType -> IO (CB.CBytes, Int64)
handleCreateAsSelect ServerContext{..} taskBuilder commandQueryStmtText extra = do
  (qid, timestamp) <- createInsertPersistentQuery (getTaskName taskBuilder) commandQueryStmtText extra zkHandle
  tid <- forkIO $ HSP.withMaybeZHandle zkHandle (HSP.setQueryStatus qid HSP.Running)
        >> runTaskWrapper False taskBuilder scLDClient
  takeMVar runningQueries >>= putMVar runningQueries . HM.insert qid tid
  return (qid, timestamp)

mark :: (Exception e, Exception f) => (e -> f) -> IO a -> IO a
mark mke = handle (throwIO . mke)
