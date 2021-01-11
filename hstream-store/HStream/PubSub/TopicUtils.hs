{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module HStream.PubSub.TopicUtils where

import           Control.Exception
import           Control.Monad
import           Control.Monad.Except
import           Data.IORef
import           Data.Word                      (Word64)
import qualified HStream.PubSub.Internal.PubSub as PS
import           HStream.PubSub.TopicMap
import           HStream.PubSub.Types
import qualified HStream.Store                  as S
import           System.Random
import           Z.Data.CBytes                  as CB
import           Z.Data.Text
import qualified Z.Data.Text.Extra              as E
import           Z.IO.Time

mkTopic :: Text -> Topic
mkTopic = Topic . E.intercalateElem '/' . Prelude.filter (/= "") . E.splitWith (== '/')

topicToCbytes :: Topic -> CBytes
topicToCbytes (Topic t) = fromBytes (getUTF8Bytes t)

topicTail :: CBytes
topicTail = "_topic$tail"

try' :: IO a -> EIO a
try' = ExceptT . try

try'' :: IO a -> EIO ()
try'' = ExceptT . try . void

-- | create logID random
getRandomLogID :: IO Word64
getRandomLogID = do
  t <- getSystemTime'
  let i = fromIntegral $ systemSeconds t
  r <- randomRIO @Word64 (1, 100000)
  return (i * 100000 + r)

createTopic ::
  GlobalTM -> -- global topic map
  S.StreamClient ->
  Topic ->
  Int -> -- Replication Factor
  EIO S.TopicID
createTopic gtm client topic rf = do
  at <- liftIO S.newTopicAttributes
  liftIO $ S.setTopicReplicationFactor at rf
  logID <- liftIO getRandomLogID
  let li = S.mkTopicID logID
  try'' $ S.makeTopicGroupSync client (topicToCbytes topic <> topicTail) li li at True
  liftIO $ modifyIORef' gtm $ insertTM topic li
  return li

getTopicID :: GlobalTM -> S.StreamClient -> Topic -> EIO S.TopicID
getTopicID gtm client topic = do
  tmp <- liftIO $ readIORef gtm
  case lookupTM topic tmp of
    Just tid -> return tid
    Nothing -> do
      gs <- try' $ S.getTopicGroupSync client (topicToCbytes topic <> topicTail)
      (li, _) <- liftIO $ S.topicGroupGetRange gs
      liftIO $ modifyIORef' gtm $ insertTM topic li
      return li

-- | pub message
pub ::
  GlobalTM -> -- global topic map
  S.StreamClient -> -- client
  Topic -> --- Topic
  Message -> -- Message
  EIO S.SequenceNum
pub gtm client topic message = do
  tid <- getTopicID gtm client topic
  liftIO $ PS.pub client tid message

-- | sub topic
sub ::
  GlobalTM -> -- global topic map
  S.StreamClient ->
  Int -> -- max sub number
  [Topic] -> -- topic list
  EIO S.StreamReader
sub gtm client ms tps = do
  sreader <- liftIO $ S.newStreamReader client (fromIntegral ms) 4096
  forM_ tps $ \tp -> do
    tid <- getTopicID gtm client tp
    end <- liftIO $ S.getTailSequenceNum client tid
    liftIO $ S.readerStartReading sreader tid (end + 1) maxBound
  return sreader
