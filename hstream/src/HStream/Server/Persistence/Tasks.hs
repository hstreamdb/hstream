{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.Server.Persistence.Tasks where

import           Control.Exception                    (throwIO)
import           Control.Monad                        (void)
import           Data.Int                             (Int64)
import qualified Data.Text                            as T
import           Z.Data.CBytes                        (CBytes, pack)
import           Z.IO.Time                            (SystemTime (MkSystemTime),
                                                       getSystemTime')
import           ZooKeeper                            (zooDeleteAll,
                                                       zooGetChildren, zooMulti)
import           ZooKeeper.Types

import           HStream.Server.Persistence.Common
import           HStream.Server.Persistence.Exception
import           HStream.Server.Persistence.Utils
import           HStream.Server.Types                 (ServerID)
import           HStream.Utils                        (TaskStatus (..))

--------------------------------------------------------------------------------

instance TaskPersistence ZHandle where
  insertQuery qid qSql qTime qType qHServer zk = ifThrow FailedToRecordInfo $ do
    MkSystemTime timestamp _ <- getSystemTime'
    void . zooMulti zk $ createPathOp (mkQueryPath qid) :
      map createHelper
        [ ("/sql", encodeValueToBytes qSql)
        , ("/createdTime", encodeValueToBytes qTime)
        , ("/type", encodeValueToBytes qType)
        , ("/status", encodeValueToBytes Created)
        , ("/timeCkp", encodeValueToBytes timestamp)
        , ("/hServer", encodeValueToBytes qHServer)
        ]
    where
      createHelper (field, contents) = createInsertOp (mkQueryPath qid <> field) contents

  setQueryStatus qid newStatus zk = ifThrow FailedToSetStatus $ do
    MkSystemTime timestamp _ <- getSystemTime'
    void . zooMulti zk $
      [ setZkDataOp (mkQueryPath qid <> "/status") (encodeValueToBytes newStatus)
      , setZkDataOp (mkQueryPath qid <> "/timeCkp") (encodeValueToBytes timestamp)
      ]

  setQueryHServer qid hServer zk = ifThrow FailedToSetHServer $ do
    setZkData zk (mkQueryPath qid <> "/hServer") (encodeValueToBytes hServer)

  getQueryIds = ifThrow FailedToGet . (unStrVec . strsCompletionValues <$>)  . flip zooGetChildren queriesPath

  getQuery queryId zk = ifThrow FailedToGet $ do
    queryBindedSql   <- getThenDecode "/sql"
    queryCreatedTime <- getThenDecode "/createdTime"
    queryType        <- getThenDecode "/type"
    queryStatus      <- getThenDecode "/status"
    queryTimeCkp     <- getThenDecode "/timeCkp"
    queryHServer     <- getThenDecode "/hServer"
    return $ PersistentQuery {..}
    where
      getThenDecode field = decodeZNodeValue' zk (mkQueryPath queryId <> field)

  insertConnector cid cSql cTime cHServer zk = ifThrow FailedToRecordInfo $ do
    MkSystemTime timestamp _ <- getSystemTime'
    void . zooMulti zk $ createPathOp (mkConnectorPath cid) :
      map createHelper
        [ ("/sql", encodeValueToBytes cSql)
        , ("/createdTime", encodeValueToBytes cTime)
        , ("/status",  encodeValueToBytes Created)
        , ("/timeCkp", encodeValueToBytes timestamp)
        , ("/hServer", encodeValueToBytes cHServer)
        ]
    where
      createHelper (field, contents) = createInsertOp (mkConnectorPath cid <> field) contents

  setConnectorStatus cid newStatus zk = ifThrow FailedToSetStatus $ do
    MkSystemTime timestamp _ <- getSystemTime'
    void . zooMulti zk $
      [ setZkDataOp (mkConnectorPath cid <> "/status") (encodeValueToBytes newStatus)
      , setZkDataOp (mkConnectorPath cid <> "/timeCkp") (encodeValueToBytes timestamp)
      ]

  setConnectorHServer qid hServer zk = ifThrow FailedToSetHServer $ do
    setZkData zk (mkConnectorPath qid <> "/hServer") (encodeValueToBytes hServer)

  getConnectorIds = ifThrow FailedToGet . (unStrVec . strsCompletionValues <$>) . flip zooGetChildren connectorsPath

  getConnector connectorId zk = ifThrow FailedToGet $ do
    connectorBindedSql   <- getThenDecode "/sql"
    connectorCreatedTime <- getThenDecode "/createdTime"
    connectorStatus      <- getThenDecode "/status"
    connectorTimeCkp     <- getThenDecode "/timeCkp"
    connectorHServer     <- getThenDecode "/hServer"
    return $ PersistentConnector {..}
    where
      getThenDecode field = decodeZNodeValue' zk (mkConnectorPath connectorId <> field)

  removeQuery qid zk  = ifThrow FailedToRemove $
    getQueryStatus qid zk >>= \case
      Terminated -> zooDeleteAll zk (mkQueryPath qid)
      _          -> throwIO QueryStillRunning
  removeQuery' qid zk = ifThrow FailedToRemove $ zooDeleteAll zk (mkQueryPath qid)

  removeConnector cid zk  = ifThrow FailedToRemove $
    getConnectorStatus cid zk >>= \st->
      if st `elem` [Terminated, CreationAbort, ConnectionAbort]
        then zooDeleteAll zk (mkConnectorPath cid)
        else throwIO ConnectorStillRunning
  removeConnector' cid zk = ifThrow FailedToRemove $
    zooDeleteAll zk (mkConnectorPath cid)

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

createInsertPersistentQuery :: T.Text -> T.Text -> QueryType -> ServerID ->  ZHandle -> IO (CBytes, Int64)
createInsertPersistentQuery taskName queryText queryType queryHServer zkHandle = do
  MkSystemTime timestamp _ <- getSystemTime'
  let qid   = Z.Data.CBytes.pack (T.unpack taskName)
  insertQuery qid queryText timestamp queryType queryHServer zkHandle
  return (qid, timestamp)
