{-# LANGUAGE DeriveAnyClass    #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
module HStream.Server.Persistence.Tasks where

import           Control.Exception                    (throwIO)
import           Data.Int                             (Int64)
import qualified Data.Text                            as T
import           GHC.Generics                         (Generic)
import           GHC.Stack                            (HasCallStack)
import           Z.Data.CBytes                        (CBytes, pack)
import           Z.Data.JSON                          (JSON, encode)
import qualified Z.Data.Text                          as ZT
import           Z.IO.Time                            (SystemTime (MkSystemTime),
                                                       getSystemTime')
import           ZooKeeper                            (zooDeleteAll, zooGet,
                                                       zooGetChildren)
import           ZooKeeper.Types

import           HStream.Server.Persistence.Exception
import           HStream.Server.Persistence.Utils
import           HStream.Utils                        (TaskStatus (..))

type ViewSchema     = [String]
type RelatedStreams = [CBytes]

data PersistentQuery = PersistentQuery
  { queryId          :: CBytes
  , queryBindedSql   :: ZT.Text
  , queryCreatedTime :: Int64
  , queryType        :: QueryType
  , queryStatus      :: TaskStatus
  , queryTimeCkp     :: Int64
  } deriving (Generic, Show, JSON)

data PersistentConnector = PersistentConnector
  { connectorId          :: CBytes
  , connectorBindedSql   :: ZT.Text
  , connectorCreatedTime :: Int64
  , connectorStatus      :: TaskStatus
  , connectorTimeCkp     :: Int64
  } deriving (Generic, Show, JSON)

data QueryType
  = PlainQuery  RelatedStreams
  | StreamQuery RelatedStreams CBytes            -- ^ related streams and the stream it creates
  | ViewQuery   RelatedStreams CBytes ViewSchema -- ^ related streams and the view it creates
  deriving (Show, Eq, Generic, JSON)

--------------------------------------------------------------------------------

class Persistence handle where
  insertQuery        :: HasCallStack => CBytes -> T.Text -> Int64 -> QueryType -> handle -> IO ()
  insertConnector    :: HasCallStack => CBytes -> T.Text -> Int64 -> handle -> IO ()

  setQueryStatus     :: HasCallStack => CBytes -> TaskStatus -> handle -> IO ()
  setConnectorStatus :: HasCallStack => CBytes -> TaskStatus -> handle -> IO ()

  getQueryIds        :: HasCallStack => handle -> IO [CBytes]
  getQuery           :: HasCallStack => CBytes -> handle -> IO PersistentQuery

  getQueries         :: HasCallStack => handle -> IO [PersistentQuery]
  getQueries h = getQueryIds h >>= mapM (`getQuery` h)
  getQueryStatus     :: HasCallStack => CBytes -> handle -> IO TaskStatus
  getQueryStatus qid h = queryStatus <$> getQuery qid h

  getConnectorIds    :: HasCallStack => handle -> IO [CBytes]
  getConnector       :: HasCallStack => CBytes -> handle -> IO PersistentConnector

  getConnectors      :: HasCallStack => handle -> IO [PersistentConnector]
  getConnectors h = getConnectorIds h >>= mapM (`getConnector` h)
  getConnectorStatus :: HasCallStack => CBytes -> handle -> IO TaskStatus
  getConnectorStatus cid h = connectorStatus <$> getConnector cid h

  removeQuery'       :: HasCallStack => CBytes -> handle ->  IO ()
  removeQuery        :: HasCallStack => CBytes -> handle -> IO ()

  removeConnector'   :: HasCallStack => CBytes -> handle ->  IO ()
  removeConnector    :: HasCallStack => CBytes -> handle -> IO ()

createInsertPersistentQuery :: T.Text -> T.Text -> QueryType -> ZHandle -> IO (CBytes, Int64)
createInsertPersistentQuery taskName queryText queryType zkHandle = do
  MkSystemTime timestamp _ <- getSystemTime'
  let qid   = Z.Data.CBytes.pack (T.unpack taskName)
  insertQuery qid queryText timestamp queryType zkHandle
  return (qid, timestamp)

instance Persistence ZHandle where
  insertQuery qid qSql qTime qType zk = ifThrow FailedToRecordInfo $ do
    MkSystemTime timestamp _ <- getSystemTime'
    createPath   zk (mkQueryPath qid)
    createInsert zk (mkQueryPath qid <> "/sql") (encode . ZT.pack . T.unpack $ qSql)
    createInsert zk (mkQueryPath qid <> "/createdTime") (encode qTime)
    createInsert zk (mkQueryPath qid <> "/type") (encode qType)
    createInsert zk (mkQueryPath qid <> "/status")  (encode Created)
    createInsert zk (mkQueryPath qid <> "/timeCkp") (encode timestamp)

  setQueryStatus qid newStatus zk = ifThrow FailedToSetStatus $ do
    MkSystemTime timestamp _ <- getSystemTime'
    setZkData zk (mkQueryPath qid <> "/status") (encode newStatus)
    setZkData zk (mkQueryPath qid <> "/timeCkp") (encode timestamp)

  getQueryIds = ifThrow FailedToGet . (unStrVec . strsCompletionValues <$>)  . flip zooGetChildren queriesPath

  getQuery qid zk = ifThrow FailedToGet $ do
    sql         <- getThenDecode "/sql" qid
    createdTime <- getThenDecode "/createdTime" qid
    typ         <- getThenDecode "/type" qid
    status      <- getThenDecode "/status" qid
    timeCkp     <- getThenDecode "/timeCkp" qid
    return $ PersistentQuery qid sql createdTime typ status timeCkp
    where
      getThenDecode s = (decodeQ <$>) . zooGet zk . (<> s) . mkQueryPath

  insertConnector cid cSql cTime zk = ifThrow FailedToRecordInfo $ do
    MkSystemTime timestamp _ <- getSystemTime'
    createPath   zk (mkConnectorPath cid)
    createInsert zk (mkConnectorPath cid <> "/sql") (encode . ZT.pack . T.unpack $ cSql)
    createInsert zk (mkConnectorPath cid <> "/createdTime") (encode cTime)
    createInsert zk (mkConnectorPath cid <> "/status") (encode Created)
    createInsert zk (mkConnectorPath cid <> "/timeCkp") (encode timestamp)

  setConnectorStatus cid newStatus zk = ifThrow FailedToSetStatus $ do
    MkSystemTime timestamp _ <- getSystemTime'
    setZkData zk (mkConnectorPath cid <> "/status") (encode newStatus)
    setZkData zk (mkConnectorPath cid <> "/timeCkp") (encode timestamp)

  getConnectorIds = ifThrow FailedToGet . (unStrVec . strsCompletionValues <$>) . flip zooGetChildren connectorsPath

  getConnector cid zk = ifThrow FailedToGet $ do
    sql         <- ((decodeQ <$>) . zooGet zk . (<> "/sql") . mkConnectorPath) cid
    createdTime <- ((decodeQ <$>) . zooGet zk . (<> "/createdTime")  . mkConnectorPath) cid
    status      <- ((decodeQ <$>) . zooGet zk . (<> "/status") . mkConnectorPath) cid
    timeCkp     <- ((decodeQ <$>) . zooGet zk . (<> "/timeCkp") . mkConnectorPath) cid
    return $ PersistentConnector cid sql createdTime status timeCkp

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
