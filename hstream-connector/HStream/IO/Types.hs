{-# LANGUAGE TemplateHaskell #-}

module HStream.IO.Types where

import qualified Data.Aeson                 as J
import qualified Data.Text                  as T

import qualified Data.Aeson.TH              as JT
import qualified Data.ByteString.Lazy       as BSL
import qualified Data.ByteString.Lazy.Char8 as BSLC
import qualified Data.HashMap.Strict        as HM
import           Data.Maybe                 (isJust)
import           ZooKeeper.Types            (ZHandle)
import qualified HStream.Server.HStreamApi as API
import           HStream.Utils (textToMaybeValue, pairListToStruct)
import GHC.Base (join)
import           Control.Exception                (Exception)

data IOTaskType = SOURCE | SINK
  deriving (Show, Eq)

$(JT.deriveJSON JT.defaultOptions ''IOTaskType)


data TaskConfig = TaskConfig
  { tcImage :: T.Text
  }

$(JT.deriveJSON JT.defaultOptions ''TaskConfig)

data TaskInfo = TaskInfo
  { taskName        :: T.Text
  , taskType        :: IOTaskType
  , taskConfig      :: TaskConfig
  , connectorConfig :: J.Value
  , originSql       :: T.Text
  }

$(JT.deriveJSON JT.defaultOptions ''TaskInfo)

data IOTaskStatus
  = NEW
  | RUNNING
  | STOPPED
  | FAILED
  | COMPLETED
  | DELETED
  deriving (Show, Eq)

ioTaskStatusToText :: IOTaskStatus -> T.Text
ioTaskStatusToText = T.pack . show

ioTaskStatusToBS :: IOTaskStatus -> BSL.ByteString
ioTaskStatusToBS = BSLC.pack . show

$(JT.deriveJSON JT.defaultOptions ''IOTaskStatus)

-- TODO: spec the exceptions
class Kv kv where
  get :: kv -> T.Text -> IO (Maybe BSL.ByteString)
  insert :: kv -> T.Text -> BSL.ByteString -> IO ()
  update :: kv -> T.Text -> BSL.ByteString -> IO ()
  delete :: kv -> T.Text -> IO BSL.ByteString
  keys :: kv -> IO [T.Text]
  exists :: kv -> T.Text -> IO Bool
  exists kvv key = isJust <$> get kvv key

data KvConfig =
  -- ZkKvConfig ZHandle zkUrl rootPath
  ZkKvConfig ZHandle T.Text T.Text
  | FileKvConfig FilePath

class TaskJson cm where
  toTaskJson :: cm -> T.Text -> J.Value

instance TaskJson KvConfig where
  toTaskJson (ZkKvConfig _ zkUrl rootPath) taskId =
    J.object
      [ "type" J..= ("zk" :: T.Text)
      , "url" J..= zkUrl
      , "rootPath" J..= (rootPath <> "/kv/" <> taskId)
      ]
  toTaskJson (FileKvConfig filePath) _ =
    J.object
      [ "type" J..= ("file" :: T.Text)
      , "filePath" J..= filePath
      ]

data HStreamConfig = HStreamConfig
  { serviceUrl :: T.Text
  } deriving (Show)

instance TaskJson HStreamConfig where
  toTaskJson HStreamConfig {..} _ = J.object [ "serviceUrl" J..= serviceUrl]

mkConnector :: T.Text -> T.Text -> API.Connector
mkConnector name status = API.Connector. Just $
  pairListToStruct
    [ ("name", textToMaybeValue name)
    , ("status", textToMaybeValue status)
    ]

makeImage :: IOTaskType -> T.Text -> (T.Text, HM.HashMap T.Text J.Value)
makeImage SOURCE "mysql" = ("hstream/source-debezium", HM.fromList [("source", "mysql")])
makeImage SOURCE "postgres" = ("hstream/source-debezium", HM.fromList [("source", "postgres")])
makeImage SOURCE "sql-server" = ("hstream/source-debezium", HM.fromList [("source", "sql-server")])
makeImage _ _ = error "unimplemented"

-- doubleBind, for nested Monads
-- e.g. IO (Maybe a) (a -> IO (Maybe b))
(>>>=) :: (Monad m, Monad n, Traversable n) => m (n a) -> (a -> m (n b)) -> m (n b)
(>>>=) mv action = do
    b <- mv >>= mapM action
    return (join b)

data StopWorkerException = StopWorkerException deriving Show
instance Exception StopWorkerException
