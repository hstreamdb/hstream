module HStream.Common.Server.MetaData.Values
  ( rootPath
  , kafkaRootPath
  , clusterStartTimeId
  ) where

import           Data.Text (Text)

rootPath :: Text
rootPath = "/hstream"

kafkaRootPath :: Text
kafkaRootPath = "/hstream/kafka"

clusterStartTimeId :: Text
clusterStartTimeId = "Cluster_Uptime"
