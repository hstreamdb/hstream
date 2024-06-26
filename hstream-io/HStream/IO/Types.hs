{-# LANGUAGE DeriveAnyClass        #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}

module HStream.IO.Types where

import qualified Control.Concurrent             as C
import           Control.Exception              (Exception)
import qualified Control.Exception              as E
import qualified Data.Aeson                     as J
import qualified Data.Aeson.KeyMap              as J
import qualified Data.Aeson.Text                as J
import qualified Data.ByteString.Lazy           as BSL
import qualified Data.ByteString.Lazy.Char8     as BSLC
import qualified Data.HashMap.Strict            as HM
import           Data.IORef                     (IORef)
import qualified Data.Text                      as T
import qualified Data.Text.Lazy                 as TL
import qualified Data.Vector                    as Vector
import           GHC.Generics                   (Generic)
import qualified GHC.IO.Handle                  as IO
import qualified System.Process.Typed           as TP

import           HStream.Common.ZookeeperClient (ZookeeperClient)
import qualified HStream.Exception              as E
import qualified HStream.IO.LineReader          as LR
import           HStream.MetaStore.Types        (FHandle, HasPath (..),
                                                 MetaHandle, RHandle (..))
import qualified HStream.Server.HStreamApi      as API
import qualified HStream.Stats                  as Stats
import qualified HStream.ThirdParty.Protobuf    as Grpc
import qualified HStream.ThirdParty.Protobuf    as PB
import           Proto3.Suite                   (def)

data IOTaskType = SOURCE | SINK
  deriving (Show, Eq, Generic, J.FromJSON, J.ToJSON)

data TaskConfig = TaskConfig
  { tcImage   :: T.Text
  , tcNetwork :: T.Text
  } deriving (Show, Eq, Generic, J.FromJSON, J.ToJSON)

data TaskInfo = TaskInfo
  { taskName        :: T.Text
  , taskType        :: IOTaskType
  , taskTarget      :: T.Text
  , taskCreatedTime :: Grpc.Timestamp
  , taskConfig      :: TaskConfig
  , connectorConfig :: J.Object
  } deriving (Show, Eq, Generic, J.FromJSON, J.ToJSON)

data IOTaskStatus
  = NEW
  | RUNNING
  | STOPPED
  | FAILED
  | COMPLETED
  | DELETED
  deriving (Show, Eq, Generic, J.FromJSON, J.ToJSON)

data IOOptions = IOOptions
  { optTasksNetwork        :: T.Text
  , optTasksPath           :: T.Text
  , optSourceImages        :: HM.HashMap T.Text T.Text
  , optSinkImages          :: HM.HashMap T.Text T.Text
  , optExtraDockerArgs     :: T.Text
  , optFixedConnectorImage :: Bool
  } deriving (Show, Eq)

type TaskProcess = TP.Process IO.Handle IO.Handle ()

data IOTask = IOTask
  { taskId          :: T.Text
  , taskInfo        :: TaskInfo
  , taskPath        :: FilePath
  , taskHandle      :: MetaHandle
  , process'        :: IORef (Maybe TaskProcess)
  , statusM         :: C.MVar IOTaskStatus
  , taskStatsHolder :: Stats.StatsHolder
  , taskOffsetsM    :: C.MVar (Vector.Vector PB.Struct)
  , logReader       :: LR.LineReader
  , ioOptions       :: IOOptions
  }

instance Show IOTask where
  show IOTask {taskInfo=TaskInfo{..}, ..}
    = "{id=" <> show taskId
   <> ", name=" <> show taskName
   <> ", type=" <> show taskType
   <> ", target=" <> show taskTarget
   <> " }"

type ZkUrl = T.Text
type Path = T.Text
data ConnectorMetaConfig
  -- ZkKvConfig zkUrl rootPath
  = ZkKvConfig  ZkUrl Path
  | FileKvConfig FilePath

newtype HStreamConfig = HStreamConfig
  { serviceUrl :: T.Text
  } deriving (Show, Eq, Generic)
    deriving anyclass (J.FromJSON, J.ToJSON)

data Worker = Worker
  { hsConfig     :: HStreamConfig
  , options      :: IOOptions
  , ioTasksM     :: C.MVar (HM.HashMap T.Text IOTask)
  , monitorTid   :: IORef C.ThreadId
  , workerHandle :: MetaHandle
  , statsHolder  :: Stats.StatsHolder
  }

data TaskMeta = TaskMeta {
    taskInfoMeta  :: TaskInfo
  , taskStateMeta :: IOTaskStatus
  } deriving (Show, Eq, Generic)
    deriving anyclass (J.FromJSON, J.ToJSON)

newtype TaskIdMeta = TaskIdMeta {taskIdMeta :: T.Text}
  deriving (Show, Eq, Generic)
  deriving anyclass (J.FromJSON, J.ToJSON)

newtype TaskKvMeta = TaskKvMeta
  { value :: T.Text
  } deriving (Show, Eq, Generic)
    deriving anyclass (J.FromJSON, J.ToJSON)

ioRootPath :: T.Text
ioRootPath = "/hstream/io"

instance HasPath TaskMeta ZookeeperClient where
  myRootPath = ioRootPath <> "/tasks"

instance HasPath TaskIdMeta ZookeeperClient where
  myRootPath = ioRootPath <> "/taskNames"

instance HasPath TaskKvMeta ZookeeperClient where
  myRootPath = ioRootPath <> "/taskKvs"

instance HasPath TaskMeta RHandle where
  myRootPath = "ioTasks"

instance HasPath TaskIdMeta RHandle where
  myRootPath = "ioTaskNames"

instance HasPath TaskKvMeta RHandle where
  myRootPath = "ioTaskKvs"

instance HasPath TaskMeta FHandle where
  myRootPath = "ioTasks"

instance HasPath TaskIdMeta FHandle where
  myRootPath = "ioTaskNames"

instance HasPath TaskKvMeta FHandle where
  myRootPath = "ioTaskKvs"

convertTaskMeta :: (T.Text, TaskMeta) -> API.Connector
convertTaskMeta (taskId, TaskMeta {..}) =
  def { API.connectorName = taskName taskInfoMeta
      , API.connectorType = ioTaskTypeToText . taskType $ taskInfoMeta
      , API.connectorTarget = taskTarget taskInfoMeta
      , API.connectorCreationTime = Just . taskCreatedTime $ taskInfoMeta
      , API.connectorStatus = ioTaskStatusToText taskStateMeta
      , API.connectorTaskId = taskId
      , API.connectorConfig = TL.toStrict . J.encodeToLazyText . J.lookup "connector" $ connectorConfig taskInfoMeta
      }

ioTaskStatusToText :: IOTaskStatus -> T.Text
ioTaskStatusToText = T.pack . show

ioTaskStatusToBS :: IOTaskStatus -> BSL.ByteString
ioTaskStatusToBS = BSLC.pack . show

ioTaskTypeToText :: IOTaskType -> T.Text
ioTaskTypeToText typ = case typ of
  SOURCE -> "SOURCE"
  SINK   -> "SINK"

ioTaskTypeFromText :: T.Text -> IOTaskType
ioTaskTypeFromText typ = case T.toUpper typ of
  "SOURCE" -> SOURCE
  "SINK"   -> SINK
  _        -> E.throw (E.InvalidConnectorType typ)

-- -------------------------------------------------------------------------- --

data StopWorkerException = StopWorkerException deriving Show
instance Exception StopWorkerException
