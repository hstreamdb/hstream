{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}

module HStream.Server.Persistence
  ( Query(..)
  , QueryInfo(..)
  , QueryStatus(..)
  , QStatus(..)
  , queriesPath
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

type SqlStatement = Text
type QueryId      = CBytes
type TimeStamp    = Int64

data Query = Query {
    _QueryId     :: QueryId
  , _QueryInfo   :: QueryInfo
  , _QueryStatus :: QueryStatus
} deriving (Generic, Show)
instance JSON Query

data QueryInfo = QueryInfo {
    sqlStatement :: SqlStatement
  , createdTime  :: TimeStamp
} deriving (Generic, Show)
instance JSON QueryInfo

data QueryStatus = QueryStatus {
    queryStatus         :: QStatus
  , queryTimeCheckpoint :: TimeStamp
} deriving (Generic, Show)
instance JSON QueryStatus

data QStatus = QCreated
  | QRunning
  | QTerminated
  deriving (Show, Eq, Generic)
instance JSON QStatus

queriesPath :: CBytes
queriesPath = "/hstreamdb/hstream/queries"

class Persistence handle where
  insertQuery :: HasCallStack => QueryId -> QueryInfo -> handle -> IO ()
  setStatus   :: HasCallStack => QueryId -> QStatus -> handle -> IO ()
  getQueries  :: HasCallStack => handle -> IO [Query]

withMaybeZHandle :: Maybe ZHandle -> (forall a. Persistence a => a -> IO b) -> IO b
withMaybeZHandle (Just zk) f = f zk
withMaybeZHandle Nothing   f = f queryCollection

--------------------------------------------------------------------------------

type QueriesM = IORef (HM.HashMap CBytes Query)

queryCollection :: QueriesM
queryCollection = unsafePerformIO $ newIORef HM.empty
{-# NOINLINE queryCollection #-}

instance Persistence QueriesM where
  insertQuery qid info ref = do
    MkSystemTime timestamp _ <- getSystemTime'
    modifyIORef ref $ HM.insert (mkPath qid) $ Query qid info (QueryStatus QCreated timestamp)

  setStatus qid status ref = do
    MkSystemTime timestamp _ <- getSystemTime'
    let f s query = query {_QueryStatus = QueryStatus s timestamp}
    modifyIORef ref $ HM.adjust (f status) (mkPath qid)

  getQueries = (HM.elems <$>) . readIORef

--------------------------------------------------------------------------------
defaultHandle :: HasCallStack => CBytes -> Resource ZHandle
defaultHandle network = zookeeperResInit network 5000 Nothing 0

instance Persistence ZHandle where
  insertQuery qid info@(QueryInfo _ timestamp) zk = do
    createPath   zk (mkPath qid)
    createInsert zk (mkPath qid <> "/details") (encode info)
    createInsert zk (mkPath qid <> "/status")  (encode $ QueryStatus QCreated timestamp)

  setStatus qid status zk = do
    MkSystemTime timestamp _ <- getSystemTime'
    setQuery zk (mkPath qid <> "/status") (encode $ QueryStatus status timestamp)

  getQueries zk = do
    StringsCompletion (StringVector qids) <- zooGetChildren zk queriesPath
    details <- mapM ((decodeQ <$>) . zooGet zk . (<> "/details") . mkPath) qids
    status  <- mapM ((decodeQ <$>) . zooGet zk . (<> "/status")  . mkPath) qids
    return $ zipWith ($) (zipWith ($) (Query <$> qids) details) status

initializeAncestors :: HasCallStack => ZHandle -> IO ()
initializeAncestors zk = mapM_ (tryCreate zk) ["/hstreamdb", "/hstreamdb/hstream", queriesPath]

--------------------------------------------------------------------------------

createInsert :: HasCallStack => ZHandle -> CBytes -> Bytes -> IO ()
createInsert zk path contents =
  void $ zooCreate zk path (Just contents) zooOpenAclUnsafe ZooPersistent

setQuery :: HasCallStack => ZHandle -> CBytes -> Bytes -> IO ()
setQuery zk path contents =
  void $ zooSet zk path (Just contents) Nothing

tryCreate :: HasCallStack => ZHandle -> CBytes -> IO ()
tryCreate zk path = catch (createPath zk path) (\e -> return $ const () (e :: ZNODEEXISTS))

createPath :: HasCallStack => ZHandle -> CBytes -> IO ()
createPath zk path =
  void $ zooCreate zk path Nothing zooOpenAclUnsafe ZooPersistent

decodeQ :: JSON a => DataCompletion -> a
decodeQ = (\case { Right x -> x ; _ -> error "decoding failed"}) . snd . decode
        . (\case { Nothing -> ""; Just x -> x}) . dataCompletionValue

mkPath :: QueryId -> CBytes
mkPath x = queriesPath <> "/" <> x
