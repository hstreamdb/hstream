{-# LANGUAGE CPP #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module HStream.Common.Server.MetaData
  ( rootPath
  , clusterStartTimeId
  , TaskAllocation (..), renderTaskAllocationsToTable

  , initKafkaZkPaths
  , initKafkaRqTables
  , initKafkaFileTables
  ) where

import           Control.Exception                     (handle)
import           Control.Monad                         (unless, void)
import           Data.Aeson                            (FromJSON (..),
                                                        ToJSON (..))
import qualified Data.Aeson                            as Aeson
import qualified Data.ByteString.Lazy                  as BSL
import           Data.Text                             (Text)
import           Data.Word
import           GHC.Generics                          (Generic)
import           GHC.Stack                             (HasCallStack)
import           System.Directory                      (doesFileExist)
import           System.FileLock                       (SharedExclusive (Exclusive),
                                                        withTryFileLock)
import           Z.Data.CBytes                         (CBytes)
import           ZooKeeper.Types                       (ZHandle)

import           HStream.Common.Server.MetaData.Values
import           HStream.Exception                     (RQLiteTableAlreadyExists)
import           HStream.MetaStore.FileUtils           (Contents, createTables)
import           HStream.MetaStore.RqliteUtils         (createTable)
import           HStream.MetaStore.Types               (FHandle,
                                                        HasPath (myRootPath),
                                                        RHandle (..))
import           HStream.MetaStore.ZookeeperUtils      (tryCreate)
import qualified HStream.ThirdParty.Protobuf           as Proto
import           HStream.Utils                         (textToCBytes)

-------------------------------------------------------------------------------

data TaskAllocation = TaskAllocation
  { taskAllocationEpoch    :: Word32
  , taskAllocationServerId :: Word32
  } deriving (Show, Generic)

instance FromJSON TaskAllocation
instance ToJSON TaskAllocation

instance HasPath TaskAllocation ZHandle where
  myRootPath = rootPath <> "/taskAllocations"

instance HasPath TaskAllocation RHandle where
  myRootPath = "taskAllocations"

instance HasPath TaskAllocation FHandle where
  myRootPath = "taskAllocations"

renderTaskAllocationsToTable :: [TaskAllocation] -> Aeson.Value
renderTaskAllocationsToTable relations =
  let headers = ["Server ID" :: Text]
      rows = map (\TaskAllocation{..} -> [taskAllocationServerId]) relations
   in Aeson.object ["headers" Aeson..= headers, "rows" Aeson..= rows]

-------------------------------------------------------------------------------

initializeZkPaths :: HasCallStack => ZHandle -> [CBytes] -> IO ()
initializeZkPaths zk = mapM_ (tryCreate zk)

initializeRqTables :: RHandle -> [Text] -> IO ()
initializeRqTables (RHandle m url) = mapM_ (handleExists . createTable m url)
  where
    handleExists = handle (\(_:: RQLiteTableAlreadyExists) -> pure ())

initializeFileTables :: FHandle -> [Text] -> IO ()
initializeFileTables fp paths = do
  fileExists <- doesFileExist fp
  unless fileExists $ void $ withTryFileLock fp Exclusive $ \_ ->
    BSL.writeFile fp (Aeson.encode (mempty :: Contents))
  createTables paths fp

-------------------------------------------------------------------------------
-- HStream metadatas for Kafka server

kafkaZkPaths :: [CBytes]
kafkaZkPaths =
  [ textToCBytes rootPath
  , textToCBytes kafkaRootPath
  , textToCBytes $ myRootPath @Proto.Timestamp @ZHandle
  , textToCBytes $ myRootPath @TaskAllocation @ZHandle
  ]

kafkaRqTables :: [Text]
kafkaRqTables =
  [ myRootPath @TaskAllocation @RHandle
  ]

kafkaFileTables :: [Text]
kafkaFileTables =
  [ myRootPath @TaskAllocation @FHandle
  ]

initKafkaZkPaths :: HasCallStack => ZHandle -> IO ()
initKafkaZkPaths zk = initializeZkPaths zk kafkaZkPaths

initKafkaRqTables :: RHandle -> IO ()
initKafkaRqTables rh = initializeRqTables rh kafkaRqTables

initKafkaFileTables :: FHandle -> IO ()
initKafkaFileTables fh = initializeFileTables fh kafkaFileTables

-------------------------------------------------------------------------------
-- TODO: HStream metadatas for hstream server
--
-- see HStream.Server.MetaData from hstream library

-------------------------------------------------------------------------------
-- Instances

instance HasPath Proto.Timestamp ZHandle where
  myRootPath = rootPath <> "/timestamp"
instance HasPath Proto.Timestamp RHandle where
  myRootPath = "timestamp"
instance HasPath Proto.Timestamp FHandle where
  myRootPath = "timestamp"
