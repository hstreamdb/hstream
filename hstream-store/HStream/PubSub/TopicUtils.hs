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
import           HStream.PubSub.Types
import qualified HStream.Store                  as S
import           System.Random
import           Z.Data.CBytes                  as CB
import           Z.Data.Text
import qualified Z.Data.Text.Extra              as E
import           Z.IO.Time

try' :: IO a -> EIO a
try' = ExceptT . try

try'' :: IO a -> EIO ()
try'' = ExceptT . try . void

-- | create logID random
createTopic ::
  S.StreamClient ->
  S.Topic ->
  Int -> -- Replication Factor
  EIO ()
createTopic client topic rf = do
  try'' $ S.createTopicSync client topic (S.TopicAttrs rf)


getTopicID :: S.StreamClient -> S.Topic -> EIO S.TopicID
getTopicID client topic = liftIO $ S.getTopicIDByName client topic

-- | pub message
pub ::
  S.StreamClient -> -- client
  S.Topic -> --- Topic
  Message -> -- Message
  EIO S.SequenceNum
pub client topic message = do
  tid <- getTopicID client topic
  liftIO $ PS.pub client tid message

-- | sub topic
sub ::
  S.StreamClient ->
  Int -> -- max sub number
  [S.Topic] -> -- topic list
  EIO S.StreamReader
sub client ms tps = do
  sreader <- liftIO $ S.newStreamReader client (fromIntegral ms) 4096
  forM_ tps $ \tp -> do
    tid <- getTopicID client tp
    end <- liftIO $ S.getTailSequenceNum client tid
    liftIO $ S.readerStartReading sreader tid (end + 1) maxBound
  return sreader
