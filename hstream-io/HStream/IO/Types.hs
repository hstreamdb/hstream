{-# LANGUAGE TemplateHaskell       #-}
{-# OPTIONS_GHC -Wno-deferred-out-of-scope-variables #-}
{-# LANGUAGE DeriveAnyClass        #-}
{-# LANGUAGE MultiParamTypeClasses #-}

module HStream.IO.Types where

import qualified Control.Concurrent         as C
import           Control.Exception          (Exception)
import qualified Data.Aeson                 as J
import qualified Data.Aeson.TH              as JT
import qualified Data.ByteString.Lazy       as BSL
import qualified Data.ByteString.Lazy.Char8 as BSLC
import qualified Data.HashMap.Strict        as HM
import           Data.IORef                 (IORef)
import qualified Data.Text                  as T
import qualified GHC.IO.Handle              as IO
import qualified System.Process.Typed       as TP
import           ZooKeeper.Types            (ZHandle)

import           HStream.MetaStore.Types    (HasPath (..), MetaHandle,
                                             RHandle (..))
import qualified HStream.Server.HStreamApi  as API
import           HStream.Utils              (pairListToStruct, textToMaybeValue)

data IOTaskType = SOURCE | SINK
  deriving (Show, Eq)
$(JT.deriveJSON JT.defaultOptions ''IOTaskType)

data TaskConfig = TaskConfig
  { tcImage   :: T.Text
  , tcNetwork :: T.Text
  }
  deriving (Show)
$(JT.deriveJSON JT.defaultOptions ''TaskConfig)

data TaskInfo = TaskInfo
  { taskName        :: T.Text
  , taskType        :: IOTaskType
  , taskConfig      :: TaskConfig
  , connectorConfig :: J.Value
  , originSql       :: T.Text
  }
  deriving (Show)
$(JT.deriveJSON JT.defaultOptions ''TaskInfo)

data IOTaskStatus
  = NEW
  | RUNNING
  | STOPPED
  | FAILED
  | COMPLETED
  | DELETED
  deriving (Show, Eq)
$(JT.deriveJSON JT.defaultOptions ''IOTaskStatus)

data IOOptions = IOOptions
  { optTasksNetwork :: T.Text
  , optTasksPath    :: T.Text
  , optSourceImages :: HM.HashMap T.Text T.Text
  , optSinkImages   :: HM.HashMap T.Text T.Text
  } deriving (Show, Eq)

data IOTask = IOTask
  { taskId     :: T.Text
  , taskInfo   :: TaskInfo
  , taskPath   :: FilePath
  , taskHandle :: MetaHandle
  , process'   :: IORef (Maybe (TP.Process IO.Handle () ()))
  , statusM    :: C.MVar IOTaskStatus
  }

type ZkUrl = T.Text
type Path = T.Text
data ConnectorMetaConfig
  -- ZkKvConfig zkUrl rootPath
  = ZkKvConfig  ZkUrl Path
  | FileKvConfig FilePath

data HStreamConfig = HStreamConfig
  { serviceUrl :: T.Text
  } deriving (Show)

data Worker = Worker
  { connectorMetaCfg :: ConnectorMetaConfig
  , hsConfig         :: HStreamConfig
  , options          :: IOOptions
  , checkNode        :: T.Text -> IO Bool
  , ioTasksM         :: C.MVar (HM.HashMap T.Text IOTask)
  , monitorTid       :: IORef C.ThreadId
  , workerHandle     :: MetaHandle
  }

data TaskMeta = TaskMeta {
    taskInfoMeta  :: TaskInfo
  , taskStateMeta :: IOTaskStatus
  } deriving (Show)
$(JT.deriveJSON JT.defaultOptions ''TaskMeta)

newtype TaskIdMeta = TaskIdMeta {taskIdMeta :: T.Text}
  deriving (Show)
$(JT.deriveJSON JT.defaultOptions ''TaskIdMeta)

ioRootPath :: T.Text
ioRootPath = "/hstream/io"

instance HasPath TaskMeta ZHandle where
  myRootPath = ioRootPath <> "/tasks"

instance HasPath TaskIdMeta ZHandle where
  myRootPath = ioRootPath <> "/taskNames"

instance HasPath TaskMeta RHandle where
  myRootPath = "io-tasks"

instance HasPath TaskIdMeta RHandle where
  myRootPath = "io-taskNames"

-- TODO: spec the exceptions
class TaskJson cm where
  toTaskJson :: cm -> T.Text -> J.Value

instance TaskJson ConnectorMetaConfig where
  toTaskJson (ZkKvConfig zkUrl rootPath) taskId =
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

instance TaskJson HStreamConfig where
  toTaskJson HStreamConfig {..} _ = J.object [ "serviceUrl" J..= serviceUrl]

mkConnector :: T.Text -> T.Text -> API.Connector
mkConnector name status = API.Connector. Just $
  pairListToStruct
    [ ("name", textToMaybeValue name)
    , ("status", textToMaybeValue status)
    ]

convertTaskMeta :: TaskMeta -> API.Connector
convertTaskMeta TaskMeta {..} = mkConnector (taskName taskInfoMeta) (ioTaskStatusToText taskStateMeta)

ioTaskStatusToText :: IOTaskStatus -> T.Text
ioTaskStatusToText = T.pack . show

ioTaskStatusToBS :: IOTaskStatus -> BSL.ByteString
ioTaskStatusToBS = BSLC.pack . show

-- -------------------------------------------------------------------------- --

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
