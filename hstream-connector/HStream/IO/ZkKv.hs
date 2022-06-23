{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE LambdaCase         #-}
{-# LANGUAGE OverloadedStrings  #-}

module HStream.IO.ZkKv where

import           Control.Monad    (void)
import           HStream.IO.Types
import qualified HStream.Utils    as UC
import qualified Z.Data.CBytes    as ZCB
import           ZooKeeper        (zooCreate, zooExists, zooGet, zooGetChildren,
                                   zooSet)
import           ZooKeeper.Types

data ZkKv =
  ZkKv {
    zk       :: ZHandle,
    rootPath :: ZCB.CBytes
  }

instance Kv ZkKv where
  get ZkKv{..} key = do
    let path = rootPath <> "/" <> UC.textToCBytes key
    (DataCompletion (Just valBytes) _) <- zooGet zk path
    return $ UC.bytesToLazyByteString valBytes

  set ZkKv{..} key val = do
    let path = rootPath <> "/" <> UC.textToCBytes key
        valBytes = UC.lazyByteStringToBytes val
    zooExists zk path >>= \case
      Nothing -> void $ zooCreate zk path (Just valBytes) zooOpenAclUnsafe ZooPersistent
      Just _ -> void $ zooSet zk path (Just valBytes) Nothing

  keys ZkKv{..} = do
    map UC.cBytesToText . unStrVec . strsCompletionValues <$> zooGetChildren zk rootPath


