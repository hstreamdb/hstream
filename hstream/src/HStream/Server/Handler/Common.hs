{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedLists   #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.Server.Handler.Common where

import           Control.Concurrent               (MVar, ThreadId)
import           Control.Exception                (SomeException,
                                                   displayException)
import           Control.Monad                    (when)
import qualified Data.ByteString.Char8            as C
import qualified Data.HashMap.Strict              as HM
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import           Network.GRPC.HighLevel.Generated
import           Network.GRPC.LowLevel.Op         (Op (OpRecvCloseOnServer),
                                                   OpRecvResult (OpRecvCloseOnServerResult),
                                                   runOps)
import qualified Z.Data.CBytes                    as CB
import qualified Z.Data.Text                      as ZT
import           Z.IO.Time                        (SystemTime (..),
                                                   getSystemTime')
import           ZooKeeper.Types

import qualified HStream.Connector.HStore         as HCS
import           HStream.Processing.Processor     (TaskBuilder, getTaskName,
                                                   runTask)
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


createInsertPersistentQuery :: TaskName -> TL.Text -> HSP.QueryType -> Maybe ZHandle -> IO CB.CBytes
createInsertPersistentQuery taskName queryText extraInfo zkHandle = do
  MkSystemTime timestamp _ <- getSystemTime'
  let qid = case extraInfo of
        HSP.PlainQuery             -> ""
        HSP.StreamQuery streamName -> "stream_" <> streamName <> "-"
        HSP.ViewQuery   viewName _ -> "view_" <> viewName <> "-"
        <> CB.pack (T.unpack taskName)
      qinfo = HSP.Info (ZT.pack $ T.unpack $ TL.toStrict queryText) timestamp
  HSP.withMaybeZHandle zkHandle $ HSP.insertQuery qid qinfo extraInfo
  return qid

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
