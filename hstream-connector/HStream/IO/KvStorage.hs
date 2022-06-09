{-# LANGUAGE DeriveGeneric, OverloadedStrings, DeriveAnyClass, DerivingStrategies, LambdaCase #-}

module HStream.IO.KvStorage where

import qualified Data.Map.Strict as M
import qualified Data.ByteString as BS
import GHC.IORef (IORef, readIORef, writeIORef, newIORef)
import Control.Concurrent (MVar, withMVar, newMVar)
import qualified Data.Text as T

type Key = T.Text

data KvStorage = 
  KvStorage {
    mVar :: MVar (),
    m :: IORef (M.Map Key BS.ByteString)
  }

newKvStorage :: IO KvStorage
newKvStorage = KvStorage <$> newMVar () <*> newIORef M.empty

lookup :: Key -> KvStorage -> IO (Maybe BS.ByteString)
lookup k KvStorage{..} =
  withMVar mVar $ \_ -> do
    s <- readIORef m
    return (M.lookup k s)

get :: KvStorage -> Key -> IO BS.ByteString
get KvStorage{..} k =
  withMVar mVar $ \_ -> do
    s <- readIORef m
    return (s M.! k)

set :: KvStorage -> Key -> BS.ByteString -> IO ()
set KvStorage{..} k v =
  withMVar mVar $ \_ -> do
    s <- readIORef m
    writeIORef m (M.insert k v s)

