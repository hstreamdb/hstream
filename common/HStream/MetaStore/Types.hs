{-# LANGUAGE AllowAmbiguousTypes   #-}
{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE UndecidableInstances  #-}

module HStream.MetaStore.Types where

import           Data.Aeson                       (FromJSON, ToJSON)
import           Data.Maybe                       (catMaybes, isJust)
import qualified Data.Text                        as T
import qualified GHC.IO.Handle                    as IO
import           GHC.Stack                        (HasCallStack)
import           HStream.MetaStore.ZookeeperUtils (createInsertZK,
                                                   decodeZNodeValue,
                                                   deleteZKPath, setZkData)
import           HStream.Utils                    (cBytesToText, textToCBytes)
import           ZooKeeper                        (zooExists, zooGetChildren)
import           ZooKeeper.Types                  (StringVector (..),
                                                   StringsCompletion (..),
                                                   ZHandle)


type Key = T.Text
type Path = T.Text
type Version = Int
class (MetaStore value handle, HasPath value handle) => MetaType value handle
instance (MetaStore value handle, HasPath value handle) => MetaType value handle

data MetaHandle
  = ZkHandle ZHandle
--   | RLHandle RHandle
--   | LocalHandle IO.Handle
-- data RHandle = RHandle

class (ToJSON a, FromJSON a, Show a) => HasPath a handle where
  myRootPath :: T.Text
  myPath     :: T.Text -> T.Text
  myPath x = myRootPath @a @handle <> "/" <> x

class MetaStore value handle where
  insertMeta :: (HasPath value handle, HasCallStack) => T.Text -> value -> handle -> IO ()
  updateMeta :: (HasPath value handle, HasCallStack) => T.Text -> value -> Maybe Version -> handle -> IO ()
  deleteMeta :: (HasPath value handle, HasCallStack) => T.Text -> Maybe Version -> handle  -> IO ()
  listMeta   :: (HasPath value handle, HasCallStack) => handle -> IO [value]
  getMeta    :: (HasPath value handle, HasCallStack) => T.Text -> handle -> IO (Maybe value)
  checkMetaExists :: (HasPath value handle) => T.Text -> handle -> IO Bool

  updateMetaWith :: (HasPath value handle) => T.Text -> (Maybe value -> value) -> Maybe Version -> handle -> IO ()
  updateMetaWith mid f mv h = getMeta @value mid h >>= \x -> updateMeta mid (f x) mv h

instance MetaStore value ZHandle where
  insertMeta mid x zk    = createInsertZK zk (myPath @value @ZHandle mid) x
  updateMeta mid x mv zk = setZkData      zk (myPath @value @ZHandle mid) x mv
  deleteMeta mid   mv zk = deleteZKPath   zk (myPath @value @ZHandle mid) mv
  checkMetaExists mid zk = isJust <$> zooExists zk (textToCBytes (myPath @value @ZHandle mid))
  getMeta mid zk = decodeZNodeValue zk (myPath @value @ZHandle mid)
  listMeta zk = do
    let path = textToCBytes $ myRootPath @value @ZHandle
    ids <- unStrVec . strsCompletionValues <$> zooGetChildren zk path
    catMaybes <$> mapM (flip (getMeta @value) zk . cBytesToText) ids

instance (ToJSON a, FromJSON a, HasPath a ZHandle, Show a) => HasPath a MetaHandle
instance (HasPath value ZHandle) => MetaStore value MetaHandle where
  -- insertMeta mid x h = case h of ZkHandle zk -> insertMeta @value @ZHandle mid x zk

#define USE_WHICH_HANDLE(handle, action) \
case handle of ZkHandle zk -> action zk

  listMeta            h = USE_WHICH_HANDLE(h, listMeta @value)
  insertMeta mid x    h = USE_WHICH_HANDLE(h, insertMeta @value mid x)
  updateMeta mid x mv h = USE_WHICH_HANDLE(h, updateMeta @value mid x mv)
  deleteMeta mid   mv h = USE_WHICH_HANDLE(h, deleteMeta @value mid mv)
  checkMetaExists mid h = USE_WHICH_HANDLE(h, checkMetaExists @value mid)
  getMeta mid         h = USE_WHICH_HANDLE(h, getMeta @value mid)
