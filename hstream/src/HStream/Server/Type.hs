{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}

module HStream.Server.Type where

import Control.Concurrent.Async
import Data.Aeson (FromJSON, ToJSON)
import Data.Data (Typeable)
import Data.IORef
import Data.Map (Map)
import Data.Swagger (ToSchema)
import Data.Text (Text)
import Data.Time
import GHC.Generics (Generic)
import HStream.Store
import Z.Data.CBytes (CBytes)

newtype Resp = OK Text
  deriving (Show, Eq, Generic, Typeable, FromJSON, ToJSON)

data ReqSQL = ReqSQL Text
  deriving (Show, Eq, Generic, Typeable, FromJSON, ToJSON)

data RecordVal = RecordVal Text
  deriving (Show, Eq, Generic, Typeable, FromJSON, ToJSON)

type TaskID = Int

instance ToSchema Resp

instance ToSchema RecordVal

instance ToSchema ReqSQL

data TaskInfo
  = CreateTmpStream
      { taskid :: Int,
        tasksql :: Text,
        taskSource :: [Text],
        taskSink :: Text,
        taskState :: TaskState,
        createTime :: UTCTime
      }
  | CreateStream
      { taskid :: Int,
        tasksql :: Text,
        taskSource :: [Text],
        taskSink :: Text,
        taskState :: TaskState,
        createTime :: UTCTime
      }
  | CreateTopic
      { taskid :: Int,
        tasksql :: Text,
        taskTopic :: Text,
        taskState :: TaskState,
        createTime :: UTCTime
      }
  | InsertTopic
      { taskid :: Int,
        tasksql :: Text,
        taskTopic :: Text,
        taskState :: TaskState,
        createTime :: UTCTime
      }
  deriving (Show, Eq, Generic, Typeable, FromJSON, ToJSON)

data TaskState
  = Starting
  | ErrorHappened String
  | Running
  | Finished
  deriving (Show, Eq, Generic, Typeable, FromJSON, ToJSON)

data State = State
  { tasks :: IORef (Map TaskID TaskInfo),
    thids :: IORef (Map (Async TaskState) TaskID),
    waits :: IORef [Async TaskState],
    index :: IORef Int,
    lpath :: CBytes,
    admin :: AdminClient,
    produ :: Producer
  }
