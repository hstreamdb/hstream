{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}

module HStream.Server.Persistence
  ( Query(..)
  , Connector
  , Info(..)
  , Status(..)
  , PStatus(..)
  , queriesPath
  , connectorsPath
  , defaultHandle
  , Persistence (..)
  , initializeAncestors
  , withMaybeZHandle
  ) where

import           Control.Monad       (void)
import qualified Data.HashMap.Strict as HM
import           Data.IORef
import           Data.Int            (Int64)
import           GHC.Generics        (Generic)
import           System.IO.Unsafe    (unsafePerformIO)
import           Z.Data.CBytes       (CBytes (..))
import           Z.Data.JSON         (JSON, decode, encode)
import           Z.Data.Text         (Text)
import           Z.Data.Vector       (Bytes)
import           Z.IO.Exception      (HasCallStack, catch)
import           Z.IO.Time           (SystemTime (..), getSystemTime')
import           ZooKeeper
import           ZooKeeper.Exception
import           ZooKeeper.Types

type Id = CBytes
type TimeStamp    = Int64
type SqlStatement = Text

data Query = Query {
    queryId     :: Id
  , queryInfo   :: Info
  , queryStatus :: Status
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
  deriving (Show, Eq, Generic)
instance JSON PStatus

data PType = PQuery
  | PConnector

queriesPath :: CBytes
queriesPath = "/hstreamdb/hstream/queries"

connectorsPath :: CBytes
connectorsPath = "/hstreamdb/hstream/connectors"

class Persistence handle where
  insertQuery        :: HasCallStack => Id -> Info -> handle -> IO ()
  insertConnector    :: HasCallStack => Id -> Info -> handle -> IO ()
  setQueryStatus     :: HasCallStack => Id -> PStatus -> handle -> IO ()
  setConnectorStatus :: HasCallStack => Id -> PStatus -> handle -> IO ()
  getQueries         :: HasCallStack => handle -> IO [Query]
  getConnectors      :: HasCallStack => handle -> IO [Connector]

withMaybeZHandle :: Maybe ZHandle -> (forall a. Persistence a => a -> IO b) -> IO b
withMaybeZHandle (Just zk) f = f zk
withMaybeZHandle Nothing   f = f (queryCollection, connectorsCollection)

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
  insertQuery qid info ref = do
    MkSystemTime timestamp _ <- getSystemTime'
    modifyIORef (fst ref) $ HM.insert (mkQueryPath qid) $ Query qid info (Status Created timestamp)

  insertConnector cid info ref = do
    MkSystemTime timestamp _ <- getSystemTime'
    modifyIORef (snd ref) $ HM.insert (mkConnectorPath cid) $ Connector cid info (Status Created timestamp)

  setQueryStatus qid status ref = do
    MkSystemTime timestamp _ <- getSystemTime'
    let f s query = query {queryStatus = Status s timestamp}
    modifyIORef (fst ref) $ HM.adjust (f status) (mkQueryPath qid)

  setConnectorStatus qid status ref = do
    MkSystemTime timestamp _ <- getSystemTime'
    let f s connector = connector {connectorStatus = Status s timestamp}
    modifyIORef (snd ref) $ HM.adjust (f status) (mkConnectorPath qid)

  getQueries = (HM.elems <$>) . readIORef . fst

  getConnectors = (HM.elems <$>) . readIORef . snd

--------------------------------------------------------------------------------

defaultHandle :: HasCallStack => CBytes -> Resource ZHandle
defaultHandle network = zookeeperResInit network 5000 Nothing 0

instance Persistence ZHandle where
  insertQuery qid info@(Info _ timestamp) zk = do
    createPath   zk (mkQueryPath qid)
    createInsert zk (mkQueryPath qid <> "/details") (encode info)
    createInsert zk (mkQueryPath qid <> "/status")  (encode $ Status Created timestamp)

  setQueryStatus qid status zk = do
    MkSystemTime timestamp _ <- getSystemTime'
    setZkData zk (mkQueryPath qid <> "/status") (encode $ Status status timestamp)

  getQueries zk = do
    StringsCompletion (StringVector qids) <- zooGetChildren zk queriesPath
    details <- mapM ((decodeQ <$>) . zooGet zk . (<> "/details") . mkQueryPath) qids
    status  <- mapM ((decodeQ <$>) . zooGet zk . (<> "/status")  . mkQueryPath) qids
    return $ zipWith ($) (zipWith ($) (Query <$> qids) details) status

  insertConnector cid info@(Info _ timestamp) zk = do
    createPath   zk (mkConnectorPath cid)
    createInsert zk (mkConnectorPath cid <> "/details") (encode info)
    createInsert zk (mkConnectorPath cid <> "/status")  (encode $ Status Created timestamp)

  setConnectorStatus cid status zk = do
    MkSystemTime timestamp _ <- getSystemTime'
    setZkData zk (mkConnectorPath cid <> "/status") (encode $ Status status timestamp)

  getConnectors zk = do
    StringsCompletion (StringVector cids) <- zooGetChildren zk connectorsPath
    details <- mapM ((decodeQ <$>) . zooGet zk . (<> "/details") . mkConnectorPath) cids
    status  <- mapM ((decodeQ <$>) . zooGet zk . (<> "/status")  . mkConnectorPath) cids
    return $ zipWith ($) (zipWith ($) (Connector <$> cids) details) status

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
tryCreate zk path = catch (createPath zk path) (\e -> return $ const () (e :: ZNODEEXISTS))

createPath :: HasCallStack => ZHandle -> CBytes -> IO ()
createPath zk path =
  void $ zooCreate zk path Nothing zooOpenAclUnsafe ZooPersistent

decodeQ :: JSON a => DataCompletion -> a
decodeQ = (\case { Right x -> x ; _ -> error "decoding failed"}) . snd . decode
        . (\case { Nothing -> ""; Just x -> x}) . dataCompletionValue

mkQueryPath :: Id -> CBytes
mkQueryPath x = queriesPath <> "/" <> x

mkConnectorPath :: Id -> CBytes
mkConnectorPath x = connectorsPath <> "/" <> x
