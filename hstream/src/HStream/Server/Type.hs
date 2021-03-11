{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric  #-}

module HStream.Server.Type where

import           Control.Concurrent.Async
import           Data.Aeson               (FromJSON, ToJSON)
import qualified Data.ByteString          as BL
import           Data.Data                (Typeable)
import           Data.IORef
import           Data.Map                 (Map)
import           Data.Text                (Text)
import           Data.Time
import           GHC.Generics             (Generic)
import           HStream.Store
import           Z.Data.CBytes            (CBytes)

newtype Resp = OK Text
  deriving (Show, Eq, Generic, Typeable, FromJSON, ToJSON)

data ReqSQL = ReqSQL {sqlValue :: Text}
  deriving (Show, Eq, Generic, Typeable, FromJSON, ToJSON)

type RecordStream = BL.ByteString

type TaskID = Int

data TaskInfo
  = CreateTmpStream
      { taskid     :: Int,
        tasksql    :: Text,
        taskSource :: [Text],
        taskSink   :: Text,
        taskState  :: TaskState,
        createTime :: UTCTime
      }
  | CreateStream
      { taskid     :: Int,
        tasksql    :: Text,
        taskSource :: [Text],
        taskSink   :: Text,
        taskState  :: TaskState,
        createTime :: UTCTime
      }
  | CreateTopic
      { taskid     :: Int,
        tasksql    :: Text,
        taskStream :: Text,
        taskState  :: TaskState,
        createTime :: UTCTime
      }
  | InsertTopic
      { taskid     :: Int,
        tasksql    :: Text,
        taskStream :: Text,
        taskState  :: TaskState,
        createTime :: UTCTime
      }
  deriving (Show, Eq, Generic, Typeable, FromJSON, ToJSON)

data TaskState
  = Starting
  | ErrorHappened String
  | Running
  | Finished
  deriving (Show, Eq, Generic, Typeable, FromJSON, ToJSON)

data ServerConfig = ServerConfig
  { serverPort           :: Int,
    sLogDeviceConfigPath :: String,
    sTopicRepFactor      :: Int,
    sConsumBuffSize      :: Int,
    serverHost           :: String
  }
  deriving (Show)

data ClientConfig = ClientConfig
  { cHttpUrl    :: String,
    cServerPort :: Int
  }
  deriving (Show)

data State = State
  { taskMap             :: IORef (Map TaskID (Maybe (Async TaskState), TaskInfo)),
    taskNameMap         :: IORef (Map Text TaskID),
    asyncMap            :: IORef (Map (Async TaskState) TaskID),
    waitList            :: IORef [Async TaskState],
    taskIndex           :: IORef Int,
    logDeviceConfigPath :: CBytes,
    adminClient         :: AdminClient,
    producer            :: Producer,
    topicRepFactor      :: Int,
    consumBufferSize    :: Int
  }
