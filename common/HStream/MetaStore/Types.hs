{-# LANGUAGE AllowAmbiguousTypes   #-}
{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE TupleSections         #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE UndecidableInstances  #-}

module HStream.MetaStore.Types where

import           Control.Monad                    (void)
import           Data.Aeson                       (FromJSON, ToJSON)
import qualified Data.Aeson                       as A
import qualified Data.ByteString                  as BS
import qualified Data.ByteString.Lazy             as BL
import           Data.Functor                     ((<&>))
import qualified Data.Map.Strict                  as Map
import           Data.Maybe                       (catMaybes, isJust)
import qualified Data.Text                        as T
import           GHC.Stack                        (HasCallStack)
import           Network.HTTP.Client              (Manager)
import qualified Z.Foreign                        as ZF
import qualified ZooKeeper                        as Z
import qualified ZooKeeper.Types                  as Z
import           ZooKeeper.Types                  (ZHandle)

import           HStream.MetaStore.RqliteUtils    (ROp (..), deleteFrom,
                                                   insertInto, selectFrom,
                                                   transaction, updateSet,
                                                   upsert)
import           HStream.MetaStore.ZookeeperUtils (createInsertZK,
                                                   decodeZNodeValue,
                                                   deleteZKPath, setZkData,
                                                   upsertZkData)
import           HStream.Utils                    (cBytesToText, textToCBytes)

type Key = T.Text
type Path = T.Text
type Url = T.Text
type Version = Int
class (MetaStore value handle, HasPath value handle) => MetaType value handle
instance (MetaStore value handle, HasPath value handle) => MetaType value handle

data RHandle = RHandle Manager Url
data MetaHandle
  = ZkHandle ZHandle
  | RLHandle RHandle
-- TODO
--  | LocalHandle IO.Handle

data MetaOp
  = InsertOp Path Key BS.ByteString
  | UpdateOp Path Key BS.ByteString (Maybe Version)
  | DeleteOp Path Key (Maybe Version)
  | CheckOp  Path Key Version

class (ToJSON a, FromJSON a, Show a) => HasPath a handle where
  myRootPath :: T.Text

class MetaStore value handle where
  myPath     :: HasPath value handle => T.Text -> T.Text
  insertMeta :: (HasPath value handle, HasCallStack) => Key -> value -> handle -> IO ()
  updateMeta :: (HasPath value handle, HasCallStack) => Key -> value -> Maybe Version -> handle -> IO ()
  upsertMeta  :: (HasPath value handle, HasCallStack) => Key -> value -> handle -> IO ()
  deleteMeta :: (HasPath value handle, HasCallStack) => Key -> Maybe Version -> handle  -> IO ()
  listMeta   :: (HasPath value handle, HasCallStack) => handle -> IO [value]
  getMeta    :: (HasPath value handle, HasCallStack) => Key -> handle -> IO (Maybe value)
  getAllMeta :: (HasPath value handle, HasCallStack) => handle -> IO (Map.Map Key value)
  checkMetaExists :: (HasPath value handle, HasCallStack) => Key -> handle -> IO Bool

  -- FIXME: The Operation is not atomic
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

class MetaMulti handle where
  metaMulti :: [MetaOp] -> handle -> IO ()

instance MetaStore value ZHandle where
  myPath mid = myRootPath @value @ZHandle <> "/" <> mid
  insertMeta mid x zk    = createInsertZK zk (myPath @value @ZHandle mid) x
  updateMeta mid x mv zk = setZkData      zk (myPath @value @ZHandle mid) x mv
  upsertMeta mid x zk    = upsertZkData   zk (myPath @value @ZHandle mid) x
  deleteMeta mid   mv zk = deleteZKPath   zk (myPath @value @ZHandle mid) mv
  checkMetaExists mid zk = isJust <$> Z.zooExists zk (textToCBytes (myPath @value @ZHandle mid))
  getMeta mid zk = decodeZNodeValue zk (myPath @value @ZHandle mid)
  getAllMeta zk = do
    let path = textToCBytes $ myRootPath @value @ZHandle
    ids <- Z.unStrVec . Z.strsCompletionValues <$> Z.zooGetChildren zk path
    idAndValues <- catMaybes <$> mapM (\x -> let x' = cBytesToText x in getMeta @value x' zk <&> fmap (x',)) ids
    pure $ Map.fromList idAndValues
  listMeta zk = do
    let path = textToCBytes $ myRootPath @value @ZHandle
    ids <- Z.unStrVec . Z.strsCompletionValues <$> Z.zooGetChildren zk path
    catMaybes <$> mapM (flip (getMeta @value) zk . cBytesToText) ids

instance MetaMulti ZHandle where
  metaMulti ops zk = do
    let zOps = map opToZ ops
    void $ Z.zooMulti zk zOps
    where
      opToZ op = case op of
        InsertOp p k v    -> Z.zooCreateOpInit (textToCBytes $ p <> "/" <> k) (Just $ ZF.fromByteString v) 0 Z.zooOpenAclUnsafe Z.ZooPersistent
        UpdateOp p k v mv -> Z.zooSetOpInit    (textToCBytes $ p <> "/" <> k) (Just $ ZF.fromByteString v) (fromIntegral <$> mv)
        DeleteOp p k mv   -> Z.zooDeleteOpInit (textToCBytes $ p <> "/" <> k) (fromIntegral <$> mv)
        CheckOp  p k v    -> Z.zooCheckOpInit  (textToCBytes $ p <> "/" <> k) (fromIntegral v)

instance MetaStore value RHandle where
  myPath _ = myRootPath @value @RHandle
  insertMeta mid x    (RHandle m url) = insertInto m url (myRootPath @value @RHandle) mid x
  updateMeta mid x mv (RHandle m url) = updateSet  m url (myRootPath @value @RHandle) mid x mv
  upsertMeta mid x    (RHandle m url) = upsert     m url (myRootPath @value @RHandle) mid x
  deleteMeta mid   mv (RHandle m url) = deleteFrom m url (myRootPath @value @RHandle) mid mv
  checkMetaExists mid (RHandle m url) = selectFrom @value m url (myRootPath @value @RHandle) (Just mid)
                                        <&> not . Map.null
  getMeta mid  (RHandle m url) = selectFrom m url (myRootPath @value @RHandle) (Just mid)
                              <&> Map.lookup mid
  getAllMeta   (RHandle m url) = selectFrom m url (myRootPath @value @RHandle) Nothing
  listMeta     (RHandle m url) = Map.elems <$> selectFrom m url (myRootPath @value @RHandle) Nothing

instance MetaMulti RHandle where
  metaMulti ops (RHandle m url) = do
    let zOps = concatMap opToR ops
    -- TODO: if failing show which operation failed
    transaction m url zOps
    where
      opToR op = case op of
        InsertOp p k v    -> [InsertROp p k v]
        UpdateOp p k v mv -> let ops' = [ExistROp p k, UpdateROp p k v]
                              in maybe ops' (\version -> CheckROp p k version: ops') mv
        DeleteOp p k mv   -> let ops' = [ExistROp p k, DeleteROp p k]
                              in maybe ops' (\version -> CheckROp p k version: ops') mv
        CheckOp  p k v    -> [CheckROp p k v]

instance (ToJSON a, FromJSON a, HasPath a ZHandle, HasPath a RHandle, Show a) => HasPath a MetaHandle

#define USE_WHICH_HANDLE(handle, action) \
  case handle of ZkHandle zk -> action zk; RLHandle rq -> action rq

instance (HasPath value ZHandle, HasPath value RHandle) => MetaStore value MetaHandle where
  myPath = undefined
  listMeta            h = USE_WHICH_HANDLE(h, listMeta @value)
  insertMeta mid x    h = USE_WHICH_HANDLE(h, insertMeta mid x)
  updateMeta mid x mv h = USE_WHICH_HANDLE(h, updateMeta mid x mv)
  upsertMeta mid x    h = USE_WHICH_HANDLE(h, upsertMeta mid x)
  deleteMeta mid   mv h = USE_WHICH_HANDLE(h, deleteMeta @value mid mv)
  checkMetaExists mid h = USE_WHICH_HANDLE(h, checkMetaExists @value mid)
  getMeta mid         h = USE_WHICH_HANDLE(h, getMeta @value mid)
  getAllMeta h = USE_WHICH_HANDLE(h, getAllMeta @value)

  insertMetaOp mid value    h = USE_WHICH_HANDLE(h, insertMetaOp mid value)
  updateMetaOp mid value mv h = USE_WHICH_HANDLE(h, updateMetaOp mid value mv)
  deleteMetaOp mid mv       h = USE_WHICH_HANDLE(h, deleteMetaOp @value mid mv)
  checkOp mid v             h = USE_WHICH_HANDLE(h, checkOp @value mid v)

instance MetaMulti MetaHandle where
  metaMulti ops h = USE_WHICH_HANDLE(h, metaMulti ops)
