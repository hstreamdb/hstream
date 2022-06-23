{-# LANGUAGE TemplateHaskell #-}

module HStream.IO.Types where

import qualified Data.Aeson                 as J
import qualified Data.Text                  as T

import qualified Data.Aeson.TH              as JT
import qualified Data.ByteString.Lazy       as BSL
import qualified Data.ByteString.Lazy.Char8 as BSLC

data IOTaskType = Source | Sink
  deriving (Show, Eq)

$(JT.deriveJSON JT.defaultOptions ''IOTaskType)

data HStreamConfig
  = HStreamConfig
    { taskId     :: T.Text
    , serviceUrl :: T.Text
    , zkUrl      :: T.Text
    , zkKvPath   :: T.Text
    }

$(JT.deriveJSON JT.defaultOptions ''HStreamConfig)

data TaskConfig
  = TaskConfig
    { hstreamConfig   :: HStreamConfig
    , connectorConfig :: J.Value
    }

$(JT.deriveJSON JT.defaultOptions ''TaskConfig)

data TaskInfo = TaskInfo
  { taskImage  :: T.Text
  , taskType   :: IOTaskType
  , taskConfig :: TaskConfig
  }

$(JT.deriveJSON JT.defaultOptions ''TaskInfo)


data IOTaskStatus
  = NEW
  | STARTED
  | RUNNING
  | COMPLETED
  | FAILED
  | STOPPED
  deriving (Show, Eq)

ioTaskStatusToBS :: IOTaskStatus -> BSL.ByteString
ioTaskStatusToBS = BSLC.pack . show

$(JT.deriveJSON JT.defaultOptions ''IOTaskStatus)

data IOTaskItem
  = IOTaskItem
      { iiTaskId :: T.Text
      , iiStatus :: T.Text
      -- , latestState :: J.Value
      } deriving (Show)

class Kv kv where
  get :: kv -> T.Text -> IO BSL.ByteString
  set :: kv -> T.Text -> BSL.ByteString -> IO ()
  keys :: kv -> IO [T.Text]
