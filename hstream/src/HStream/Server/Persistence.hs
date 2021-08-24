{-# LANGUAGE DeriveAnyClass      #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ParallelListComp    #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Persistence
  ( PersistentQuery(..)
  , PersistentConnector(..)
  , Status
  , pattern Creating
  , pattern Created
  , pattern Running
  , pattern CreationAbort
  , pattern ConnectionAbort
  , pattern Terminated
  , QueryType (..)
  , queriesPath
  , connectorsPath
  , defaultHandle
  , Persistence (..)
  , initializeAncestors
  , withMaybeZHandle
  , ZooException
  , isViewQuery
  , isStreamQuery
  , createInsertPersistentQuery
  , getRelatedStreams
  , getQuerySink) where

import           Control.Exception                    (Exception, handle, throw)
import           Control.Monad                        (void)
import qualified Data.HashMap.Strict                  as HM
import           Data.IORef                           (IORef, modifyIORef,
                                                       newIORef, readIORef)
import           Data.Int                             (Int64)
import qualified Data.List                            as L
import qualified Data.Text                            as T
import           GHC.Generics                         (Generic)
import qualified Proto3.Suite                         as PB
import           System.IO.Unsafe                     (unsafePerformIO)
import           Z.Data.CBytes                        (CBytes (..), pack)
import           Z.Data.JSON                          (JSON, decode, encode)
import qualified Z.Data.Text                          as ZT
import           Z.Data.Vector                        (Bytes)
import           Z.IO.Exception                       (HasCallStack, catch)
import           Z.IO.Time                            (SystemTime (..),
                                                       getSystemTime')
import           ZooKeeper
import           ZooKeeper.Exception
import           ZooKeeper.Types

import qualified HStream.Server.HStreamApi            as PB
import           HStream.Server.Persistence.Exception

--------------------------------------------------------------------------------
type ViewSchema     = [String]
type RelatedStreams = [CBytes]

data PersistentQuery = PersistentQuery
  { queryId          :: CBytes
  , queryBindedSql   :: ZT.Text
  , queryCreatedTime :: Int64
  , queryType        :: QueryType
  , queryStatus      :: Status
  , queryTimeCkp     :: Int64
  } deriving (Generic, Show, JSON)

data PersistentConnector = PersistentConnector
  { connectorId          :: CBytes
  , connectorBindedSql   :: ZT.Text
  , connectorCreatedTime :: Int64
  , connectorStatus      :: Status
  , connectorTimeCkp     :: Int64
  } deriving (Generic, Show, JSON)

type Status = PB.Enumerated PB.Status
instance JSON Status
instance JSON PB.Status

pattern Created :: Status
pattern Created = (PB.Enumerated (Right PB.StatusCREATED))

pattern Creating :: Status
pattern Creating = (PB.Enumerated (Right PB.StatusCREATING))

pattern Running :: Status
pattern Running = (PB.Enumerated (Right PB.StatusRUNNING))
-- Abort during creating the connector
pattern CreationAbort :: Status
pattern CreationAbort = (PB.Enumerated (Right PB.StatusCREATIONABORT))
-- Abort during execution of SQL statements
pattern ConnectionAbort :: Status
pattern ConnectionAbort = (PB.Enumerated (Right PB.StatusCONNECTIONABORT))

pattern Terminated :: Status
pattern Terminated = (PB.Enumerated (Right PB.StatusTERMINATED))

data QueryType
  = PlainQuery  RelatedStreams
  | StreamQuery RelatedStreams CBytes            -- ^ related streams and the stream it creates
  | ViewQuery   RelatedStreams CBytes ViewSchema -- ^ related streams and the view it creates
  deriving (Show, Eq, Generic, JSON)

--------------------------------------------------------------------------------
queriesPath :: CBytes
queriesPath = "/hstreamdb/hstream/queries"

connectorsPath :: CBytes
connectorsPath = "/hstreamdb/hstream/connectors"

class Persistence handle where
  insertQuery        :: HasCallStack => CBytes -> T.Text -> Int64 -> QueryType -> handle -> IO ()
  insertConnector    :: HasCallStack => CBytes -> T.Text -> Int64 -> handle -> IO ()

  setQueryStatus     :: HasCallStack => CBytes -> Status -> handle -> IO ()
  setConnectorStatus :: HasCallStack => CBytes -> Status -> handle -> IO ()

  getQueryIds        :: HasCallStack => handle -> IO [CBytes]
  getQuery           :: HasCallStack => CBytes -> handle -> IO PersistentQuery

  getQueries         :: HasCallStack => handle -> IO [PersistentQuery]
  getQueries h = getQueryIds h >>= mapM (`getQuery` h)
  getQueryStatus     :: HasCallStack => CBytes -> handle -> IO Status
  getQueryStatus qid h = queryStatus <$> getQuery qid h

  getConnectorIds    :: HasCallStack => handle -> IO [CBytes]
  getConnector       :: HasCallStack => CBytes -> handle -> IO PersistentConnector

  getConnectors      :: HasCallStack => handle -> IO [PersistentConnector]
  getConnectors h = getConnectorIds h >>= mapM (`getConnector` h)
  getConnectorStatus :: HasCallStack => CBytes -> handle -> IO Status
  getConnectorStatus cid h = connectorStatus <$> getConnector cid h

  removeQuery'       :: HasCallStack => CBytes -> handle ->  IO ()
  removeQuery        :: HasCallStack => CBytes -> handle -> IO ()

  removeConnector'   :: HasCallStack => CBytes -> handle ->  IO ()
  removeConnector    :: HasCallStack => CBytes -> handle -> IO ()

withMaybeZHandle :: Maybe ZHandle -> (forall a. Persistence a => a -> IO b) -> IO b
withMaybeZHandle (Just zk) f = f zk
withMaybeZHandle Nothing   f = f (queryCollection, connectorsCollection)

createInsertPersistentQuery :: T.Text -> T.Text -> QueryType -> Maybe ZHandle -> IO (CBytes, Int64)
createInsertPersistentQuery taskName queryText queryType zkHandle = do
  MkSystemTime timestamp _ <- getSystemTime'
  let qid   = Z.Data.CBytes.pack (T.unpack taskName)
  withMaybeZHandle zkHandle $ insertQuery qid queryText timestamp queryType
  return (qid, timestamp)

--------------------------------------------------------------------------------

type PStoreMem   = (QueriesM, ConnectorsM)
type ConnectorsM = IORef (HM.HashMap CBytes PersistentConnector)
type QueriesM    = IORef (HM.HashMap CBytes PersistentQuery)

queryCollection :: QueriesM
queryCollection = unsafePerformIO $ newIORef HM.empty
{-# NOINLINE queryCollection #-}

connectorsCollection :: ConnectorsM
connectorsCollection = unsafePerformIO $ newIORef HM.empty
{-# NOINLINE connectorsCollection #-}

instance Persistence PStoreMem where
  insertQuery qid qSql qTime qType (refQ, _) = ifThrow FailedToRecordInfo $ do
    MkSystemTime timestamp _ <- getSystemTime'
    modifyIORef refQ $
      HM.insert (mkQueryPath qid) $ PersistentQuery qid (ZT.pack . T.unpack $ qSql) qTime qType Created timestamp

  insertConnector cid cSql cTime (_, refC) = ifThrow FailedToRecordInfo $ do
    MkSystemTime timestamp _ <- getSystemTime'
    modifyIORef refC $
      HM.insert (mkConnectorPath cid) $ PersistentConnector cid (ZT.pack . T.unpack $ cSql) cTime Created timestamp

  setQueryStatus qid newStatus (refQ, _) = ifThrow FailedToSetStatus $ do
    MkSystemTime timestamp _ <- getSystemTime'
    let f s query = query {queryStatus = s, queryTimeCkp = timestamp}
    modifyIORef refQ $ HM.adjust (f newStatus) (mkQueryPath qid)

  setConnectorStatus qid statusQ (_, refC) = ifThrow FailedToSetStatus $ do
    MkSystemTime timestamp _ <- getSystemTime'
    let f s connector = connector {connectorStatus = s, connectorTimeCkp = timestamp}
    modifyIORef refC $ HM.adjust (f statusQ) (mkConnectorPath qid)

  getQueryIds = ifThrow FailedToGet . (L.map queryId . HM.elems <$>) . readIORef . fst
  getQuery qid (refQ, _) = ifThrow FailedToGet $ do
    hmapQ <- readIORef refQ
    case HM.lookup (mkQueryPath qid) hmapQ of
      Nothing    -> throwIO QueryNotFound
      Just query -> return query

  getConnectorIds = ifThrow FailedToGet . (L.map connectorId . HM.elems <$>) . readIORef . snd
  getConnector cid (_, refC) = ifThrow FailedToGet $ do
    hmapC <- readIORef refC
    case HM.lookup (mkConnectorPath cid) hmapC of
      Nothing -> throwIO ConnectorNotFound
      Just c  -> return c

  removeQuery qid ref@(refQ, _) = ifThrow FailedToRemove $
    getQueryStatus qid ref >>= \case
      Terminated -> modifyIORef refQ . HM.delete . mkQueryPath $ qid
      _          -> throwIO QueryStillRunning

  removeQuery' qid (refQ, _) = ifThrow FailedToRemove $
    modifyIORef refQ $ HM.delete . mkQueryPath $ qid

  removeConnector cid ref@(_, refC) = ifThrow FailedToRemove $
    getConnectorStatus cid ref >>= \st -> do
    if st `elem` [Terminated, CreationAbort, ConnectionAbort]
      then modifyIORef refC . HM.delete . mkConnectorPath $ cid
      else throwIO ConnectorStillRunning

  removeConnector' cid (_, refC) = ifThrow FailedToRemove $
    modifyIORef refC $ HM.delete . mkConnectorPath $ cid

--------------------------------------------------------------------------------

defaultHandle :: HasCallStack => CBytes -> Resource ZHandle
defaultHandle network = zookeeperResInit network 5000 Nothing 0

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

initializeAncestors :: HasCallStack => ZHandle -> IO ()
initializeAncestors zk = mapM_ (tryCreate zk) ["/hstreamdb", "/hstreamdb/hstream", queriesPath, connectorsPath]

--------------------------------------------------------------------------------
createInsert :: HasCallStack => ZHandle -> CBytes -> Bytes -> IO ()
createInsert zk path contents =
  void $ zooCreate zk path (Just contents) zooOpenAclUnsafe ZooPersistent

setZkData :: HasCallStack => ZHandle -> CBytes -> Bytes -> IO ()
setZkData zk path contents =
  void $ zooSet zk path (Just contents) Nothing

tryCreate :: HasCallStack => ZHandle -> CBytes -> IO ()
tryCreate zk path = catch (createPath zk path) (\(_ :: ZNODEEXISTS) -> pure ())

createPath :: HasCallStack => ZHandle -> CBytes -> IO ()
createPath zk path =
  void $ zooCreate zk path Nothing zooOpenAclUnsafe ZooPersistent

decodeQ :: JSON a => DataCompletion -> a
decodeQ = (\case { Right x -> x ; _ -> throw FailedToDecode}) . snd . decode
        . (\case { Nothing -> ""; Just x -> x}) . dataCompletionValue

mkQueryPath :: CBytes -> CBytes
mkQueryPath x = queriesPath <> "/" <> x

mkConnectorPath :: CBytes -> CBytes
mkConnectorPath x = connectorsPath <> "/" <> x

ifThrow :: Exception e => e -> IO a -> IO a
ifThrow e = handle (\(_ :: ZooException) -> throwIO e)

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
