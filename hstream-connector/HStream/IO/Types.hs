{-# LANGUAGE TemplateHaskell #-}

module HStream.IO.Types where

import qualified Data.Aeson                 as J
import qualified Data.Text                  as T

import           Control.Exception          (Exception, throw)
import qualified Data.Aeson.TH              as JT
import qualified Data.ByteString.Lazy       as BSL
import qualified Data.ByteString.Lazy.Char8 as BSLC
import qualified Data.HashMap.Strict        as HM
import           Data.Maybe                 (isJust)
import qualified HStream.Server.HStreamApi  as API
import           HStream.Utils              (pairListToStruct, textToMaybeValue)
import           ZooKeeper.Types            (ZHandle)

data IOTaskType = SOURCE | SINK
  deriving (Show, Eq)

$(JT.deriveJSON JT.defaultOptions ''IOTaskType)


data TaskConfig = TaskConfig
  { tcImage   :: T.Text
  , tcNetwork :: T.Text
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

-- TODO: read from config file
makeImage :: IOTaskType -> T.Text -> (T.Text, HM.HashMap T.Text J.Value)
makeImage SOURCE "mysql"      = ("hstreamdb/connector:source-mysql", HM.fromList [])
makeImage SOURCE "postgresql" = ("hstreamdb/connector:source-postgresql", HM.fromList [])
makeImage SOURCE "sqlserver"  = ("hstreamdb/connector:source-sqlserver", HM.fromList [])
makeImage SINK   "mysql"      = ("hstreamdb/connector:sink-mysql", HM.fromList [])
makeImage SINK   "postgresql" = ("hstreamdb/connector:sink-postgresql", HM.fromList [])
makeImage _ name              = throw $ UnimplementedConnectorException name

-- doubleBind, for nested Monads
-- e.g. IO (Maybe a) (a -> IO (Maybe b))
-- (>>>=) :: (Monad m, Monad n, Traversable n) => m (n a) -> (a -> m (n b)) -> m (n b)
-- (>>>=) mv action = do
--     b <- mv >>= mapM action
--     return (join b)

data StopWorkerException = StopWorkerException deriving Show
instance Exception StopWorkerException

newtype CheckFailedException = CheckFailedException T.Text
  deriving Show
instance Exception CheckFailedException

newtype WrongNodeException = WrongNodeException T.Text
  deriving Show
instance Exception WrongNodeException

newtype UnimplementedConnectorException = UnimplementedConnectorException T.Text
  deriving Show
instance Exception UnimplementedConnectorException

newtype ConnectorExistedException = ConnectorExistedException T.Text
  deriving Show
instance Exception ConnectorExistedException

newtype ConnectorNotExistException = ConnectorNotExistException T.Text
  deriving Show
instance Exception ConnectorNotExistException

newtype InvalidStatusException = InvalidStatusException IOTaskStatus
  deriving Show
instance Exception InvalidStatusException
