{-# LANGUAGE CPP #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module HStream.Common.Server.MetaData
  ( rootPath
  , kafkaRootPath
  , clusterStartTimeId

  , TaskAllocation (..)
  , renderTaskAllocationsToTable
  , GroupMetadataValue(..)
  , MemberMetadataValue(..)

  , initializeZkPaths
  , initializeRqTables
  , initializeFileTables
  ) where

import           Control.Exception                     (handle)
import           Control.Monad
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

import           Data.Int                              (Int32)
import qualified Data.Text                             as T
import qualified Data.Vector                           as V
import           HStream.Common.Server.MetaData.Values
import qualified HStream.Exception                     as HE
import qualified HStream.MetaStore.FileUtils           as File
import qualified HStream.MetaStore.RqliteUtils         as Rqlite
import           HStream.MetaStore.Types               (FHandle,
                                                        HasPath (myRootPath),
                                                        RHandle (..))
import qualified HStream.MetaStore.ZookeeperUtils      as ZK
import qualified HStream.ThirdParty.Protobuf           as Proto

------------------------------------------------------------
-- metadata for task allocation
------------------------------------------------------------
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

------------------------------------------------------------
-- metadata for groups
------------------------------------------------------------
data GroupMetadataValue
  = GroupMetadataValue
  { groupId       :: T.Text
  , generationId  :: Int32

  -- protocol
  , protocolType  :: T.Text
  , prototcolName :: Maybe T.Text

  , leader        :: Maybe T.Text
  , members       :: V.Vector MemberMetadataValue
  } deriving (Show, Eq, Generic)

instance FromJSON GroupMetadataValue
instance ToJSON GroupMetadataValue

instance HasPath GroupMetadataValue ZHandle where
  myRootPath = rootPath <> "/groups"

instance HasPath GroupMetadataValue RHandle where
  myRootPath = "groups"

instance HasPath GroupMetadataValue FHandle where
  myRootPath = "groups"

data MemberMetadataValue
  = MemberMetadataValue
  { memberId         :: T.Text
  , clientId         :: T.Text
  , clientHost       :: T.Text
  , sessionTimeout   :: Int32
  , rebalanceTimeout :: Int32

  -- base64
  , subscription     :: T.Text
  , assignment       :: T.Text
  } deriving (Show, Eq, Generic)

instance FromJSON MemberMetadataValue
instance ToJSON MemberMetadataValue

------------------------------------------------------------
-- metadata for some common utils
------------------------------------------------------------
instance HasPath Proto.Timestamp ZHandle where
  myRootPath = rootPath <> "/timestamp"
instance HasPath Proto.Timestamp RHandle where
  myRootPath = "timestamp"
instance HasPath Proto.Timestamp FHandle where
  myRootPath = "timestamp"

------------------------------------------------------------
-- Metadata Initialization (common methods)
------------------------------------------------------------
initializeZkPaths :: HasCallStack => ZHandle -> [CBytes] -> IO ()
initializeZkPaths zk = mapM_ (ZK.tryCreate zk)

initializeRqTables :: RHandle -> [Text] -> IO ()
initializeRqTables (RHandle m url) = mapM_ (handleExists . Rqlite.createTable m url)
  where
    handleExists = handle (\(_:: HE.RQLiteTableAlreadyExists) -> pure ())

initializeFileTables :: FHandle -> [Text] -> IO ()
initializeFileTables fp paths = do
  fileExists <- doesFileExist fp
  unless fileExists $ void $ withTryFileLock fp Exclusive $ \_ ->
    BSL.writeFile fp (Aeson.encode (mempty :: File.Contents))
  File.createTables paths fp
