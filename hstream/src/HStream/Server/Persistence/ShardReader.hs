{-# OPTIONS_GHC -Wno-orphans #-}

module HStream.Server.Persistence.ShardReader where

import           Control.Exception                    (handle)
import           Data.Maybe                           (isJust)
import qualified Data.Text                            as T
import           HStream.Server.Persistence.Common    (ReaderPersistence (..))
import           HStream.Server.Persistence.Exception (ShardReaderIdExists (..))
import           HStream.Server.Persistence.Utils     (createInsert,
                                                       decodeZNodeValue,
                                                       deletePath,
                                                       encodeValueToBytes,
                                                       mkReaderPath)
import           HStream.Utils                        (textToCBytes)
import qualified Z.Data.CBytes                        as CB
import           ZooKeeper                            (zooExists)
import           ZooKeeper.Exception
import           ZooKeeper.Types                      (ZHandle)

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
