{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE LambdaCase         #-}
{-# LANGUAGE OverloadedStrings  #-}

module HStream.IO.ZkKv where

import           Control.Monad    (void)
import           Data.Maybe       (isJust)
import           HStream.IO.Types
import qualified HStream.Utils    as UC
import qualified Z.Data.CBytes    as ZCB
import           ZooKeeper        (zooCreate, zooDelete, zooExists, zooGet,
                                   zooGetChildren, zooSet)
import           ZooKeeper.Types

data ZkKv =
  ZkKv {
    zk       :: ZHandle,
    rootPath :: ZCB.CBytes
  }

instance Kv ZkKv where
  get ZkKv{..} key = do
    let path = rootPath <> "/" <> UC.textToCBytes key
    zooGet zk path >>= \case
      (DataCompletion Nothing _) -> return Nothing
      (DataCompletion (Just valBytes) _) -> return . Just $ UC.bytesToLazyByteString valBytes

  insert ZkKv{..} key val = do
    let path = rootPath <> "/" <> UC.textToCBytes key
        valBytes = UC.lazyByteStringToBytes val
    void $ zooCreate zk path (Just valBytes) zooOpenAclUnsafe ZooPersistent

  update ZkKv{..} key val = do
    let path = rootPath <> "/" <> UC.textToCBytes key
        valBytes = UC.lazyByteStringToBytes val
    void $ zooSet zk path (Just valBytes) Nothing

  -- TODO: atomic read and delete
  delete zkKv@ZkKv{..} key = do
    let path = rootPath <> "/" <> UC.textToCBytes key
    Just val <- get zkKv key
    void $ zooDelete zk path Nothing
    return val

  keys ZkKv{..} = do
    map UC.cBytesToText . unStrVec . strsCompletionValues <$> zooGetChildren zk rootPath

  exists ZkKv{..} key = do
    isJust <$> zooExists zk (rootPath <> UC.textToCBytes key)
