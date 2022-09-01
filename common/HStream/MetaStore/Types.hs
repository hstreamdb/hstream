{-# LANGUAGE AllowAmbiguousTypes   #-}
{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE UndecidableInstances  #-}

module HStream.MetaStore.Types where

import           Control.Monad                    (void)
import           Data.Aeson                       (FromJSON, ToJSON)
import qualified Data.Aeson                       as A
import qualified Data.ByteString                  as BS
import qualified Data.ByteString.Lazy             as BL
import           Data.Maybe                       (catMaybes, isJust)
import qualified Data.Text                        as T
import           GHC.Stack                        (HasCallStack)
import qualified Z.Foreign                        as ZF
import qualified ZooKeeper                        as Z
import qualified ZooKeeper.Types                  as Z
import           ZooKeeper.Types                  (ZHandle)

import           HStream.MetaStore.ZookeeperUtils (createInsertZK,
                                                   decodeZNodeValue,
                                                   deleteZKPath, setZkData)
import           HStream.Utils                    (cBytesToText, textToCBytes)

type Key = T.Text
type Path = T.Text
type Version = Int
class (MetaStore value handle, HasPath value handle) => MetaType value handle
instance (MetaStore value handle, HasPath value handle) => MetaType value handle

data MetaHandle
  = ZkHandle ZHandle
-- TODO
--   | RLHandle RHandle
--   | LocalHandle IO.Handle
-- data RHandle = RHandle

data MetaOp
  = InsertOp Path Key BS.ByteString
  | UpdateOp Path Key BS.ByteString (Maybe Version)
  | DeleteOp Path Key (Maybe Version)
  | CheckOp  Path Key Version

class (ToJSON a, FromJSON a, Show a) => HasPath a handle where
  myRootPath :: T.Text
  myPath     :: T.Text -> T.Text
  myPath x = myRootPath @a @handle <> "/" <> x

class MetaStore value handle where
  insertMeta :: (HasPath value handle, HasCallStack) => Key -> value -> handle -> IO ()
  updateMeta :: (HasPath value handle, HasCallStack) => Key -> value -> Maybe Version -> handle -> IO ()
  deleteMeta :: (HasPath value handle, HasCallStack) => Key -> Maybe Version -> handle  -> IO ()
  listMeta   :: (HasPath value handle, HasCallStack) => handle -> IO [value]
  getMeta    :: (HasPath value handle, HasCallStack) => Key -> handle -> IO (Maybe value)
  checkMetaExists :: (HasPath value handle, HasCallStack) => Key -> handle -> IO Bool
  metaMulti :: [MetaOp] -> handle -> IO ()

  updateMetaWith  :: (HasPath value handle, HasCallStack) => Key -> (Maybe value -> value) -> Maybe Version -> handle -> IO ()
  updateMetaWith mid f mv h = getMeta @value mid h >>= \x -> updateMeta mid (f x) mv h

  insertMetaOp :: HasPath value handle => Key -> value -> handle -> MetaOp
  updateMetaOp :: HasPath value handle => Key -> value -> Maybe Version -> handle -> MetaOp
  deleteMetaOp :: HasPath value handle => Key -> Maybe Version -> handle -> MetaOp
  checkOp :: HasPath value handle => Key -> Version -> handle -> MetaOp
  insertMetaOp mid value    _ = InsertOp (myRootPath @value @handle) mid (BL.toStrict $ A.encode value)
  updateMetaOp mid value mv _ = UpdateOp (myRootPath @value @handle) mid (BL.toStrict $ A.encode value) mv
  deleteMetaOp mid mv       _ = DeleteOp (myRootPath @value @handle) mid mv
  checkOp mid v             _ = CheckOp  (myRootPath @value @handle) mid v

instance MetaStore value ZHandle where
  insertMeta mid x zk    = createInsertZK zk (myPath @value @ZHandle mid) x
  updateMeta mid x mv zk = setZkData      zk (myPath @value @ZHandle mid) x mv
  deleteMeta mid   mv zk = deleteZKPath   zk (myPath @value @ZHandle mid) mv
  checkMetaExists mid zk = isJust <$> Z.zooExists zk (textToCBytes (myPath @value @ZHandle mid))
  getMeta mid zk = decodeZNodeValue zk (myPath @value @ZHandle mid)
  listMeta zk = do
    let path = textToCBytes $ myRootPath @value @ZHandle
    ids <- Z.unStrVec . Z.strsCompletionValues <$> Z.zooGetChildren zk path
    catMaybes <$> mapM (flip (getMeta @value) zk . cBytesToText) ids

  metaMulti ops zk = do
    let zOps = map opToZ ops
    void $ Z.zooMulti zk zOps
    where
      opToZ op = case op of
        InsertOp p k v    -> Z.zooCreateOpInit (textToCBytes $ p <> "/" <> k) (Just $ ZF.fromByteString v) 0 Z.zooOpenAclUnsafe Z.ZooPersistent
        UpdateOp p k v mv -> Z.zooSetOpInit    (textToCBytes $ p <> "/" <> k) (Just $ ZF.fromByteString v) (fromIntegral <$> mv)
        DeleteOp p k mv   -> Z.zooDeleteOpInit (textToCBytes $ p <> "/" <> k) (fromIntegral <$> mv)
        CheckOp  p k v    -> Z.zooCheckOpInit  (textToCBytes $ p <> "/" <> k) (fromIntegral v)

#define USE_WHICH_HANDLE(handle, action) \
  case handle of ZkHandle zk -> action zk

instance (ToJSON a, FromJSON a, HasPath a ZHandle, Show a) => HasPath a MetaHandle
instance (HasPath value ZHandle) => MetaStore value MetaHandle where
  -- listMeta h = case h of ZkHandle zk -> insertMeta @value zk
  listMeta            h = USE_WHICH_HANDLE(h, listMeta @value)
  insertMeta mid x    h = USE_WHICH_HANDLE(h, insertMeta mid x)
  updateMeta mid x mv h = USE_WHICH_HANDLE(h, updateMeta mid x mv)
  deleteMeta mid   mv h = USE_WHICH_HANDLE(h, deleteMeta @value mid mv)
  checkMetaExists mid h = USE_WHICH_HANDLE(h, checkMetaExists @value mid)
  getMeta mid         h = USE_WHICH_HANDLE(h, getMeta @value mid)
  metaMulti ops       h = USE_WHICH_HANDLE(h, metaMulti ops)

  insertMetaOp mid value    h = USE_WHICH_HANDLE(h, insertMetaOp mid value)
  updateMetaOp mid value mv h = USE_WHICH_HANDLE(h, updateMetaOp mid value mv)
  deleteMetaOp mid mv       h = USE_WHICH_HANDLE(h, deleteMetaOp @value mid mv)
  checkOp mid v             h = USE_WHICH_HANDLE(h, checkOp @value mid v)
