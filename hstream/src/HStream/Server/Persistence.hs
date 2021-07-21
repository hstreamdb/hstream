{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ParallelListComp    #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Persistence
  ( Query(..)
  , Connector(..)
  , Info(..)
  , Status(..)
  , PStatus(..)
  , QueryType (..)
  , queriesPath
  , connectorsPath
  , defaultHandle
  , Persistence (..)
  , initializeAncestors
  , withMaybeZHandle
  , ZooException
  , getSuffix
  , isViewQuery
  , isStreamQuery
  ,createInsertPersistentQuery,getRelatedStreams) where

import           Control.Exception                    (Exception, handle, throw)
import           Control.Monad                        (void)
import qualified Data.HashMap.Strict                  as HM
import           Data.IORef                           (IORef, modifyIORef,
                                                       newIORef, readIORef)
import           Data.Int                             (Int64)
import           Data.Text                            (Text, unpack)
import           Data.Text.Lazy                       (Text, toStrict)
import           GHC.Generics                         (Generic)
import           System.IO.Unsafe                     (unsafePerformIO)
import           Z.Data.CBytes                        (CBytes (..), pack,
                                                       unpack)
import           Z.Data.JSON                          (JSON, decode, encode)
import           Z.Data.Text                          (Text, pack)
import           Z.Data.Vector                        (Bytes)
import           Z.IO.Exception                       (HasCallStack, catch)
import           Z.IO.Time                            (SystemTime (..),
                                                       getSystemTime')
import           ZooKeeper
import           ZooKeeper.Exception
import           ZooKeeper.Types

import           HStream.Server.Persistence.Exception

type Id = CBytes
type TimeStamp    = Int64
type SqlStatement = Z.Data.Text.Text
type StreamName = CBytes
type ViewName = CBytes
type ViewSchema = [String]
type RelatedStreams = [StreamName]

data Query = Query {
    queryId        :: Id
  , queryInfo      :: Info
  , queryInfoExtra :: QueryType
  , queryStatus    :: Status
} deriving (Generic, Show)
instance JSON Query

data Connector = Connector {
    connectorId     :: Id
  , connectorInfo   :: Info
  , connectorStatus :: Status
} deriving (Generic, Show)
instance JSON Connector

data Info = Info {
    sqlStatement :: SqlStatement
  , createdTime  :: TimeStamp
} deriving (Generic, Show)
instance JSON Info

data Status = Status {
    status         :: PStatus
  , timeCheckpoint :: TimeStamp
} deriving (Generic, Show)
instance JSON Status

data PStatus = Created
  | Running
  | Terminated
  deriving (Show, Eq, Generic, Enum)
instance JSON PStatus

data QueryType
  = PlainQuery  RelatedStreams
  | StreamQuery RelatedStreams StreamName
  | ViewQuery   RelatedStreams ViewName ViewSchema
  deriving (Show, Eq, Generic)
instance JSON QueryType

queriesPath :: CBytes
queriesPath = "/hstreamdb/hstream/queries"

connectorsPath :: CBytes
connectorsPath = "/hstreamdb/hstream/connectors"

class Persistence handle where
  insertQuery        :: HasCallStack => Id -> Info -> QueryType -> handle -> IO ()
  insertConnector    :: HasCallStack => Id -> Info -> handle -> IO ()

  setQueryStatus     :: HasCallStack => Id -> PStatus -> handle -> IO ()
  setConnectorStatus :: HasCallStack => Id -> PStatus -> handle -> IO ()

  getQueries         :: HasCallStack => handle -> IO [Query]
  getConnectors      :: HasCallStack => handle -> IO [Connector]
  getQueryStatus     :: HasCallStack => Id -> handle -> IO PStatus
  getConnectorStatus :: HasCallStack => Id -> handle -> IO PStatus
  getQueryIds        :: HasCallStack => handle -> IO [CBytes]
  getConnectorIds    :: HasCallStack => handle -> IO [CBytes]

  removeQuery'       :: HasCallStack => Id -> Bool -> handle ->  IO ()
  removeQuery        :: HasCallStack => Id -> handle -> IO ()
  removeQuery cid = removeQuery' cid True
  {-# INLINE removeQuery #-}

  removeConnector'   :: HasCallStack => Id -> Bool -> handle ->  IO ()
  removeConnector    :: HasCallStack => Id -> handle -> IO ()
  removeConnector cid = removeConnector' cid True
  {-# INLINE removeConnector #-}

withMaybeZHandle :: Maybe ZHandle -> (forall a. Persistence a => a -> IO b) -> IO b
withMaybeZHandle (Just zk) f = f zk
withMaybeZHandle Nothing   f = f (queryCollection, connectorsCollection)

createInsertPersistentQuery :: Data.Text.Text -> Data.Text.Lazy.Text -> QueryType -> Maybe ZHandle -> IO (CBytes, Int64)
createInsertPersistentQuery taskName queryText extraInfo zkHandle = do
  MkSystemTime timestamp _ <- getSystemTime'
  let qid = case extraInfo of
        PlainQuery  _            -> ""
        StreamQuery _ streamName -> "stream_" <> streamName <> "-"
        ViewQuery   _ viewName _ -> "view_" <> viewName <> "-"
        <> Z.Data.CBytes.pack (Data.Text.unpack taskName)
      qinfo = Info (Z.Data.Text.pack $ Data.Text.unpack $ Data.Text.Lazy.toStrict queryText) timestamp
  withMaybeZHandle zkHandle $ insertQuery qid qinfo extraInfo
  return (qid, timestamp)

--------------------------------------------------------------------------------

type PStoreMem   = (QueriesM, ConnectorsM)
type ConnectorsM = IORef (HM.HashMap CBytes Connector)
type QueriesM    = IORef (HM.HashMap CBytes Query)

queryCollection :: QueriesM
queryCollection = unsafePerformIO $ newIORef HM.empty
{-# NOINLINE queryCollection #-}

connectorsCollection :: ConnectorsM
connectorsCollection = unsafePerformIO $ newIORef HM.empty
{-# NOINLINE connectorsCollection #-}

instance Persistence PStoreMem where
  insertQuery qid info extraInfo (refQ, _) = ifThrow FailedToRecordInfo $ do
    MkSystemTime timestamp _ <- getSystemTime'
    modifyIORef refQ $ HM.insert (mkQueryPath qid) $ Query qid info extraInfo (Status Created timestamp)

  insertConnector cid info (_, refC) = ifThrow FailedToRecordInfo $ do
    MkSystemTime timestamp _ <- getSystemTime'
    modifyIORef refC $ HM.insert (mkConnectorPath cid) $ Connector cid info (Status Created timestamp)

  setQueryStatus qid statusQ (refQ, _) = ifThrow FailedToSetStatus $ do
    MkSystemTime timestamp _ <- getSystemTime'
    let f s query = query {queryStatus = Status s timestamp}
    modifyIORef refQ $ HM.adjust (f statusQ) (mkQueryPath qid)

  setConnectorStatus qid statusQ (_, refC) = ifThrow FailedToSetStatus $ do
    MkSystemTime timestamp _ <- getSystemTime'
    let f s connector = connector {connectorStatus = Status s timestamp}
    modifyIORef refC $ HM.adjust (f statusQ) (mkConnectorPath qid)

  getQueries = ifThrow FailedToGet . (HM.elems <$>) . readIORef . fst

  getConnectors = ifThrow FailedToGet . (HM.elems <$>) . readIORef . snd

  getQueryStatus qid (refQ, _) = ifThrow FailedToGet $ do
    hmapQ <- readIORef refQ
    case HM.lookup (mkQueryPath qid) hmapQ of
      Nothing                         -> throwIO QueryNotFound
      Just (Query _ _ _ (Status x _)) -> return x

  getConnectorStatus cid (_, refC) = ifThrow FailedToGet $ do
    hmapC <- readIORef refC
    case HM.lookup (mkConnectorPath cid) hmapC of
      Nothing                           -> throwIO ConnectorNotFound
      Just (Connector _ _ (Status x _)) -> return x

  getQueryIds = ifThrow FailedToGet . (map queryId <$>) . getQueries

  getConnectorIds = ifThrow FailedToGet . (map connectorId <$>) . getConnectors

  removeQuery' qid ifCheck ref@(refQ, _) = ifThrow FailedToRemove $
    if ifCheck then getQueryStatus qid ref >>= \case
      Terminated -> modifyIORef refQ . HM.delete . mkQueryPath $ qid
      _          -> throwIO QueryStillRunning
    else modifyIORef refQ $ HM.delete . mkQueryPath $ qid

  removeConnector' cid ifCheck ref@(_, refC) = ifThrow FailedToRemove $
    if ifCheck then getConnectorStatus cid ref >>= \case
      Terminated -> modifyIORef refC . HM.delete . mkConnectorPath $ cid
      _          -> throwIO ConnectorStillRunning
    else modifyIORef refC $ HM.delete . mkConnectorPath $ cid

--------------------------------------------------------------------------------

defaultHandle :: HasCallStack => CBytes -> Resource ZHandle
defaultHandle network = zookeeperResInit network 5000 Nothing 0

instance Persistence ZHandle where
  insertQuery qid info@(Info _ timestamp) extraInfo zk = ifThrow FailedToRecordInfo $ do
    createPath   zk (mkQueryPath qid)
    createInsert zk (mkQueryPath qid <> "/details") (encode info)
    createInsert zk (mkQueryPath qid <> "/details/extra") (encode extraInfo)
    createInsert zk (mkQueryPath qid <> "/status")  (encode $ Status Created timestamp)

  setQueryStatus qid statusQ zk = ifThrow FailedToSetStatus $ do
    MkSystemTime timestamp _ <- getSystemTime'
    setZkData zk (mkQueryPath qid <> "/status") (encode $ Status statusQ timestamp)

  getQueries zk = ifThrow FailedToGet $ do
    StringsCompletion (StringVector qids) <- zooGetChildren zk queriesPath
    infos    <- mapM (getThenDecode "/details") qids
    extras   <- mapM (getThenDecode "/details/extra") qids
    statuses <- mapM (getThenDecode "/status") qids
    return [Query qid info extra statusQ
              | qid    <- qids
              | info   <- infos
              | extra  <- extras
              | statusQ <- statuses]
    where
      getThenDecode s = (decodeQ <$>) . zooGet zk . (<> s) . mkQueryPath

  insertConnector cid info@(Info _ timestamp) zk = ifThrow FailedToRecordInfo $ do
    createPath   zk (mkConnectorPath cid)
    createInsert zk (mkConnectorPath cid <> "/details") (encode info)
    createInsert zk (mkConnectorPath cid <> "/status")  (encode $ Status Created timestamp)

  setConnectorStatus cid statusC zk = ifThrow FailedToSetStatus $ do
    MkSystemTime timestamp _ <- getSystemTime'
    setZkData zk (mkConnectorPath cid <> "/status") (encode $ Status statusC timestamp)

  getConnectors zk = ifThrow FailedToGet $ do
    StringsCompletion (StringVector cids) <- zooGetChildren zk connectorsPath
    details <- mapM ((decodeQ <$>) . zooGet zk . (<> "/details") . mkConnectorPath) cids
    statusC  <- mapM ((decodeQ <$>) . zooGet zk . (<> "/status")  . mkConnectorPath) cids
    return $ zipWith ($) (zipWith ($) (Connector <$> cids) details) statusC

  getQueryStatus qid zk = ifThrow FailedToGet $ status . decodeQ <$> zooGet zk (mkQueryPath qid <> "/status")

  getConnectorStatus cid zk = ifThrow FailedToGet $ status . decodeQ <$> zooGet zk (mkConnectorPath cid <> "/status")

  getQueryIds = ifThrow FailedToGet . (unStrVec . strsCompletionValues <$>)  . flip zooGetChildren queriesPath

  getConnectorIds = ifThrow FailedToGet . (unStrVec . strsCompletionValues <$>)  . flip zooGetChildren connectorsPath

  removeQuery' qid ifCheck zk = ifThrow FailedToRemove $
    if ifCheck then getQueryStatus qid zk >>= \case
      Terminated -> zooDeleteAll zk (mkQueryPath qid)
      _          -> throwIO QueryStillRunning
    else zooDeleteAll zk (mkQueryPath qid)

  removeConnector' cid ifCheck zk = ifThrow FailedToRemove $
    if ifCheck then getConnectorStatus cid zk >>= \case
      Terminated -> zooDeleteAll zk (mkConnectorPath cid)
      _          -> throwIO ConnectorStillRunning
    else zooDeleteAll zk (mkConnectorPath cid)

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

mkQueryPath :: Id -> CBytes
mkQueryPath x = queriesPath <> "/" <> x

mkConnectorPath :: Id -> CBytes
mkConnectorPath x = connectorsPath <> "/" <> x

ifThrow :: Exception e => e -> IO a -> IO a
ifThrow e = handle (\(_ :: ZooException) -> throwIO e)

getSuffix :: CBytes -> String
getSuffix = reverse . drop 1 . dropWhile (/= '-') . reverse . Z.Data.CBytes.unpack

isViewQuery :: CBytes -> Bool
isViewQuery = (== "view") . take 4 . Z.Data.CBytes.unpack

isStreamQuery :: CBytes -> Bool
isStreamQuery = (== "stream") . take 6 . Z.Data.CBytes.unpack

getRelatedStreams :: QueryType -> RelatedStreams
getRelatedStreams (PlainQuery ss)    = ss
getRelatedStreams (StreamQuery ss _) = ss
getRelatedStreams (ViewQuery ss _ _) = ss
