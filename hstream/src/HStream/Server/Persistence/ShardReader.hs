{-# OPTIONS_GHC -Wno-orphans #-}

module HStream.Server.Persistence.ShardReader where

import qualified Data.Text as T
import ZooKeeper.Types (ZHandle)
import HStream.Server.Persistence.Common (ReaderPersistence(..))
import HStream.Server.Persistence.Utils (createInsert, encodeValueToBytes, mkReaderPath, decodeZNodeValue, deletePath)
import HStream.Utils (textToCBytes)
import ZooKeeper.Exception
import HStream.Server.Persistence.Exception (ShardReaderIdExists(..))
import Control.Exception (handle)
import qualified Z.Data.CBytes as CB
import ZooKeeper (zooExists)
import Data.Maybe (isJust)

instance ReaderPersistence ZHandle where
  storeReader readerId reader zk = do
    handleExist $ createInsert zk (getReaderPath readerId) (encodeValueToBytes reader)
   where
    handleExist = handle (\(_ :: ZNODEEXISTS) -> throwIO $ ShardReaderIdExists readerId )

  getReader readerId zk = decodeZNodeValue zk (getReaderPath readerId)

  removeReader readerId zk = deletePath zk (getReaderPath readerId)

  readerExist readerId zk = isJust <$> zooExists zk (getReaderPath readerId)

getReaderPath :: T.Text -> CB.CBytes
getReaderPath = mkReaderPath . textToCBytes
