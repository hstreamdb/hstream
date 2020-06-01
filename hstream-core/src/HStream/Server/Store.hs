{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Store
  ( sgetAll

  , sput
  , sputs
  , sputsAtom

  , readEntries
  , readEntriesByCount
  ) where

import           Control.Exception      (Exception, try)
import           Control.Monad.IO.Class (MonadIO)
import           Control.Monad.Reader   (runReaderT)
import           Data.ByteString        (ByteString)
import           Data.Either            (fromLeft, fromRight, isLeft)
import           Data.Sequence          (Seq)
import qualified Data.Sequence          as Seq
import           Data.Text              (pack)
import           Data.Vector            (Vector)
import qualified Data.Vector            as V

import qualified HStream.LogStore.Base  as LogStore
import           HStream.Server.Types   (Element, lastEntryIDInElemSeq)
import           HStream.Utils          (bs2str)

-------------------------------------------------------------------------------

-- | Get a "snapshot" of elements.
--
-- Warning: working on large lists could be very inefficient.
sgetAll :: Exception e
        => LogStore.Context          -- ^ db context
        -> ByteString                -- ^ topic
        -> Maybe LogStore.EntryID    -- ^ start entry id
        -> Maybe LogStore.EntryID    -- ^ end entry id
        -> Int                       -- ^ max size
        -> Int                       -- ^ offset
        -> IO (Either e (Seq Element))
sgetAll db topic start end maxn offset = try $ do
  dropped <- readEntriesByCount db topic start (offset + 1)
  let cut = case end of
        Nothing -> id
        Just x  -> Seq.takeWhileL (\(i, _) -> i <= x)
  let m_newsid = lastEntryIDInElemSeq dropped
  case m_newsid of
    Nothing     -> return $ Seq.empty
    Just newsid -> case Seq.length dropped == offset + 1 of
      False -> return Seq.empty
      True  -> cut <$> readEntriesByCount db topic (Just newsid) maxn

-- | Put an element to a stream.
sput :: Exception e
     => LogStore.Context    -- ^ db context
     -> ByteString          -- ^ topic
     -> ByteString          -- ^ payload
     -> IO (Either e LogStore.EntryID)
sput db topic payload = try $ do
  handle <- openWrite db topic
  appendEntry db handle payload

-- | Put elements to a stream.
--
-- If SomeException happens, the rest elements are ignored.
sputs :: Exception e
      => LogStore.Context    -- ^ db context
      -> ByteString          -- ^ topic
      -> Vector ByteString   -- ^ payloads
      -> IO (Either (Vector LogStore.EntryID, e) (Vector LogStore.EntryID))
sputs db topic payloads = do
  ehandle <- try $ openWrite db topic
  case ehandle of
    Left e -> return $ Left (V.empty, e)
    Right handle -> do
      rs <- V.mapM (try . appendEntry db handle) payloads
      case V.findIndex isLeft rs of
        Just i  ->
          let xs = V.unsafeSlice 0 i rs
              ex = V.unsafeIndex rs i
           in return $ Left (V.map fromRight' xs, fromLeft' ex)
        Nothing -> return $ Right $ V.map fromRight' rs

-- | Put elements to a stream.
--
-- The operation is atomic.
sputsAtom :: Exception e
          => LogStore.Context    -- ^ db context
          -> ByteString          -- ^ topic
          -> Vector ByteString   -- ^ payloads
          -> IO (Either (Vector LogStore.EntryID, e) (Vector LogStore.EntryID))
sputsAtom db topic payloads = do
  rs <- try $ openWrite db topic >>= \h -> appendEntriesAtom db h payloads
  case rs of
    Left e   -> return $ Left (V.empty, e)
    Right xs -> return $ Right xs

-------------------------------------------------------------------------------
-- Log-store

readEntries :: MonadIO m
            => LogStore.Context
            -> ByteString
            -> Maybe LogStore.EntryID
            -> Maybe LogStore.EntryID
            -> m (Seq Element)
readEntries db topic start end = runReaderT f db
  where
    f = LogStore.open (pack key) ropts >>= \hd -> LogStore.readEntries hd start end
    key = bs2str topic
    ropts = LogStore.defaultOpenOptions

readEntriesByCount
  :: MonadIO m
  => LogStore.Context
  -> ByteString
  -> Maybe LogStore.EntryID
  -> Int
  -> m (Seq Element)
readEntriesByCount db topic start maxn = runReaderT f db
  where
    f = LogStore.open (pack key) ropts >>= \hd -> LogStore.readEntriesByCount hd start maxn
    key = bs2str topic
    ropts = LogStore.defaultOpenOptions

appendEntry :: MonadIO m
            => LogStore.Context
            -> LogStore.LogHandle
            -> ByteString
            -> m LogStore.EntryID
appendEntry db handle payload = runReaderT f db
  where
    f = LogStore.appendEntry handle payload

appendEntriesAtom :: MonadIO m
                  => LogStore.Context
                  -> LogStore.LogHandle
                  -> Vector ByteString
                  -> m (Vector LogStore.EntryID)
appendEntriesAtom db handle payloads = runReaderT f db
  where
    f = LogStore.appendEntries handle payloads

openWrite :: MonadIO m
          => LogStore.Context
          -> ByteString
          -> m LogStore.LogHandle
openWrite db topic = runReaderT (LogStore.open (pack key) wopts) db
  where
    key = bs2str topic
    wopts = LogStore.defaultOpenOptions { LogStore.writeMode       = True
                                        , LogStore.createIfMissing = True
                                        }

-------------------------------------------------------------------------------

fromLeft' :: Either a b -> a
fromLeft' = fromLeft (error "this should never happen")

fromRight' :: Either a b -> b
fromRight' = fromRight (error "this should never happen")
