{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE DeriveAnyClass    #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.Server.Persistence.Tasks where

import           Control.Exception                    (throwIO)
import           Data.Int                             (Int64)
import qualified Data.Text                            as T
import           Z.Data.CBytes                        (CBytes, pack)
import           Z.IO.Time                            (SystemTime (MkSystemTime),
                                                       getSystemTime')
import           ZooKeeper                            (zooDeleteAll, zooGet,
                                                       zooGetChildren)
import           ZooKeeper.Types

import           HStream.Server.Persistence.Common
import           HStream.Server.Persistence.Exception
import           HStream.Server.Persistence.Utils
import           HStream.Utils                        (TaskStatus (..))
--------------------------------------------------------------------------------

instance TaskPersistence ZHandle where
  insertQuery qid qSql qTime qType qHServer zk = ifThrow FailedToRecordInfo $ do
    MkSystemTime timestamp _ <- getSystemTime'
    createPath   zk (mkQueryPath qid)
    createInsert zk (mkQueryPath qid <> "/sql") (encodeValueToBytes qSql)
    createInsert zk (mkQueryPath qid <> "/createdTime") (encodeValueToBytes qTime)
    createInsert zk (mkQueryPath qid <> "/type") (encodeValueToBytes qType)
    createInsert zk (mkQueryPath qid <> "/status")  (encodeValueToBytes Created)
    createInsert zk (mkQueryPath qid <> "/timeCkp") (encodeValueToBytes timestamp)
    createInsert zk (mkQueryPath qid <> "/hServer") (encodeValueToBytes qHServer)

  setQueryStatus qid newStatus zk = ifThrow FailedToSetStatus $ do
    MkSystemTime timestamp _ <- getSystemTime'
    setZkData zk (mkQueryPath qid <> "/status") (encodeValueToBytes newStatus)
    setZkData zk (mkQueryPath qid <> "/timeCkp") (encodeValueToBytes timestamp)

  setQueryHServer qid hServer zk = ifThrow FailedToSetHServer $ do
    setZkData zk (mkQueryPath qid <> "/hServer") (encodeValueToBytes hServer)

  getQueryIds = ifThrow FailedToGet . (unStrVec . strsCompletionValues <$>)  . flip zooGetChildren queriesPath

  getQuery qid zk = ifThrow FailedToGet $ do
    sql         <- getThenDecode "/sql" qid
    createdTime <- getThenDecode "/createdTime" qid
    typ         <- getThenDecode "/type" qid
    status      <- getThenDecode "/status" qid
    timeCkp     <- getThenDecode "/timeCkp" qid
    hServer     <- getThenDecode "/hServer" qid
    return $ PersistentQuery qid sql createdTime typ status timeCkp hServer
    where
      getThenDecode field queryId = decodeZNodeValue' zk (mkQueryPath queryId <> field)

  insertConnector cid cSql cTime cHServer zk = ifThrow FailedToRecordInfo $ do
    MkSystemTime timestamp _ <- getSystemTime'
    createPath   zk (mkConnectorPath cid)
    createInsert zk (mkConnectorPath cid <> "/sql") (encodeValueToBytes cSql)
    createInsert zk (mkConnectorPath cid <> "/createdTime") (encodeValueToBytes cTime)
    createInsert zk (mkConnectorPath cid <> "/status") (encodeValueToBytes Created)
    createInsert zk (mkConnectorPath cid <> "/timeCkp") (encodeValueToBytes timestamp)
    createInsert zk (mkConnectorPath cid <> "/hServer") (encodeValueToBytes cHServer)

  setConnectorStatus cid newStatus zk = ifThrow FailedToSetStatus $ do
    MkSystemTime timestamp _ <- getSystemTime'
    setZkData zk (mkConnectorPath cid <> "/status") (encodeValueToBytes newStatus)
    setZkData zk (mkConnectorPath cid <> "/timeCkp") (encodeValueToBytes timestamp)

  setConnectorHServer qid hServer zk = ifThrow FailedToSetHServer $ do
    setZkData zk (mkConnectorPath qid <> "/hServer") (encodeValueToBytes hServer)

  getConnectorIds = ifThrow FailedToGet . (unStrVec . strsCompletionValues <$>) . flip zooGetChildren connectorsPath

  getConnector cid zk = ifThrow FailedToGet $ do
    sql         <- ((decodeDataCompletion' <$>) . zooGet zk . (<> "/sql") . mkConnectorPath) cid
    createdTime <- ((decodeDataCompletion' <$>) . zooGet zk . (<> "/createdTime") . mkConnectorPath) cid
    status      <- ((decodeDataCompletion' <$>) . zooGet zk . (<> "/status") . mkConnectorPath) cid
    timeCkp     <- ((decodeDataCompletion' <$>) . zooGet zk . (<> "/timeCkp") . mkConnectorPath) cid
    hServer     <- ((decodeDataCompletion' <$>) . zooGet zk . (<> "/hServer") . mkConnectorPath) cid
    return $ PersistentConnector cid sql createdTime status timeCkp hServer

  removeQuery qid zk  = ifThrow FailedToRemove $
    getQueryStatus qid zk >>= \case
      Terminated -> zooDeleteAll zk (mkQueryPath qid)
      _          -> throwIO QueryStillRunning
  removeQuery' qid zk = ifThrow FailedToRemove $ zooDeleteAll zk (mkQueryPath qid)

  removeConnector cid zk  = ifThrow FailedToRemove $
    getConnectorStatus cid zk >>= \case
      Terminated -> zooDeleteAll zk (mkConnectorPath cid)
      _          -> throwIO ConnectorStillRunning
  removeConnector' cid zk = ifThrow FailedToRemove $ zooDeleteAll zk (mkConnectorPath cid)

--------------------------------------------------------------------------------

isViewQuery :: PersistentQuery -> Bool
isViewQuery PersistentQuery{..} =
  case queryType of
    ViewQuery{} -> True
    _           -> False

isStreamQuery :: PersistentQuery -> Bool
isStreamQuery PersistentQuery{..} =
  case queryType of
    StreamQuery{} -> True
    _             -> False

getRelatedStreams :: PersistentQuery -> RelatedStreams
getRelatedStreams PersistentQuery{..} =
  case queryType of
    (PlainQuery ss)    -> ss
    (StreamQuery ss _) -> ss
    (ViewQuery ss _ _) -> ss

getQuerySink :: PersistentQuery -> CBytes
getQuerySink PersistentQuery{..} =
  case queryType of
    PlainQuery{}      -> ""
    (StreamQuery _ s) -> s
    (ViewQuery _ s _) -> s

createInsertPersistentQuery :: T.Text -> T.Text -> QueryType -> CBytes ->  ZHandle -> IO (CBytes, Int64)
createInsertPersistentQuery taskName queryText queryType queryHServer zkHandle = do
  MkSystemTime timestamp _ <- getSystemTime'
  let qid   = Z.Data.CBytes.pack (T.unpack taskName)
  insertQuery qid queryText timestamp queryType queryHServer zkHandle
  return (qid, timestamp)
