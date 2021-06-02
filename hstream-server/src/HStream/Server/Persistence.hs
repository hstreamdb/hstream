{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
module HStream.Server.Persistence where

import           Control.Exception
import           Control.Monad     (void)
import           Data.Int          (Int64)
import           GHC.Generics      (Generic)
import           Z.Data.CBytes     (CBytes)
import           Z.Data.JSON       (JSON, decode, encode)
import           Z.Data.Text       (Text)
import           Z.Data.Vector     (Bytes)
import           Z.IO.Exception    (HasCallStack)
import           Z.IO.Time         (SystemTime (..), getSystemTime')
import           ZooKeeper
import           ZooKeeper.Types

type SqlStatement = Text
type QueryId      = CBytes
type TimeStamp    = Int64

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

defaultHandle :: HasCallStack => Resource ZHandle
defaultHandle = zookeeperResInit "0.0.0.0:2182" 2500 Nothing 0

insertQuery :: HasCallStack => ZHandle -> QueryId -> QueryInfo -> IO ()
insertQuery zk qid info@(QueryInfo _ timestamp) = do
  createPath   zk (mkPath qid)
  createInsert zk (mkPath qid <> "/details") (encode info)
  createInsert zk (mkPath qid <> "/status")  (encode $ QueryStatus QCreated timestamp)

setStatus :: HasCallStack => ZHandle -> QueryId -> QStatus -> IO ()
setStatus zk qid status = do
    MkSystemTime timestamp _ <- getSystemTime'
    setQuery zk (qid <> "/status") (encode $ QueryStatus status timestamp)

getQueries :: HasCallStack => ZHandle -> IO [(QueryInfo, QueryStatus)]
getQueries zk = do
  StringsCompletion (StringVector qids) <- zooGetChildren zk queriesPath
  details <- mapM ((decodeQ <$>) . zooGet zk . (<> "/details") . mkPath) qids
  status  <- mapM ((decodeQ <$>) . zooGet zk . (<> "/status")  . mkPath) qids
  return $ zip details status

--------------------------------------------------------------------------------

initializeAncestors :: HasCallStack => ZHandle -> IO ()
initializeAncestors zk = createPath zk "/hstreamdb"
  >> createPath zk "/hstreamdb/hstream" >> createPath zk queriesPath

createInsert :: HasCallStack => ZHandle -> CBytes -> Bytes -> IO ()
createInsert zk path contents =
  void $ zooCreate zk path (Just contents) zooOpenAclUnsafe ZooPersistent

setQuery :: HasCallStack => ZHandle -> CBytes -> Bytes -> IO ()
setQuery zk path contents =
  void $ zooSet zk (queriesPath <> path) (Just contents) Nothing

--------------------------------------------------------------------------------

createPath :: HasCallStack => ZHandle -> CBytes -> IO ()
createPath zk path =
  void $ zooCreate zk path Nothing zooOpenAclUnsafe ZooPersistent

decodeQ :: JSON a => DataCompletion -> a
decodeQ = (\case { Right x -> x ; _ -> error "decoding failed"}) . snd . decode
        . (\case { Nothing -> ""; Just x -> x}) . dataCompletionValue

mkPath :: QueryId -> CBytes
mkPath x = queriesPath <> "/" <> x
