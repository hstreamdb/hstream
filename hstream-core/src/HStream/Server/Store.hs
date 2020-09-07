{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Store
  ( readEntries
  , appendEntry
  ) where

import           Control.Exception      (Exception, try)
import           Control.Monad.IO.Class (MonadIO, liftIO)
import           Control.Monad.Reader   (runReaderT)
import           Data.ByteString        (ByteString)
import           Data.Sequence          (Seq)
import qualified Data.Sequence          as Seq
import           Data.Text              (pack)

import qualified HStream.LogStore.Base  as LogStore
import           HStream.Server.Types   (Element)
import           HStream.Utils          (bs2str)

-------------------------------------------------------------------------------

-- | Get a "snapshot" of elements.
readEntries
  :: (Exception e, MonadIO m)
  => LogStore.Context          -- ^ db context
  -> ByteString                -- ^ topic
  -> Maybe LogStore.EntryID    -- ^ start entry id
  -> Maybe LogStore.EntryID    -- ^ end entry id
  -> Int                       -- ^ offset
  -> Int                       -- ^ max size
  -> m (Either e (Seq Element))
readEntries db topic start end offset maxn = liftIO . try $ do
  let realMaxn = offset + maxn
  let cutR = maybe id (\x -> Seq.takeWhileL (\(i, _payload) -> i <= x)) end
  handle <- openRead db topic
  cutR . Seq.drop offset <$>
    runReaderT (LogStore.readEntriesByCount handle start realMaxn) db

-- | Put an element to a stream.
appendEntry
  :: (Exception e, MonadIO m)
  => LogStore.Context    -- ^ db context
  -> ByteString          -- ^ topic
  -> ByteString          -- ^ payload
  -> m (Either e LogStore.EntryID)
appendEntry db topic payload = liftIO . try $ do
  handle <- openWrite db topic
  runReaderT (LogStore.appendEntry handle payload) db

-------------------------------------------------------------------------------
-- Log-store

openRead :: MonadIO m
         => LogStore.Context
         -> ByteString
         -> m LogStore.LogHandle
openRead db topic = runReaderT (LogStore.open (pack key) wopts) db
  where
    key = bs2str topic
    wopts = LogStore.defaultOpenOptions { LogStore.readMode        = True
                                        , LogStore.writeMode       = False
                                        , LogStore.createIfMissing = True
                                        }

openWrite :: MonadIO m
          => LogStore.Context
          -> ByteString
          -> m LogStore.LogHandle
openWrite db topic = runReaderT (LogStore.open (pack key) wopts) db
  where
    key = bs2str topic
    wopts = LogStore.defaultOpenOptions { LogStore.readMode        = False
                                        , LogStore.writeMode       = True
                                        , LogStore.createIfMissing = True
                                        }
