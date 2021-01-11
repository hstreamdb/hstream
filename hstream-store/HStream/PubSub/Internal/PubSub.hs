{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module HStream.PubSub.Internal.PubSub where

import           Control.Monad
import           Data.Int      (Int32)
import qualified HStream.Store as S
import           Z.Data.Vector

-- | pub a message to topicID, Blocks until operation completes.
pub ::
  S.StreamClient -> -- client
  S.TopicID -> --- TopicID
  Bytes -> -- Message
  IO S.SequenceNum
pub client topicID message = S.appendSync client topicID message Nothing

-- | sub some topicIDs
sub ::
  S.StreamClient ->
  (Int, [S.TopicID]) -> -- (max sub number, topicID list)
  IO S.StreamReader
sub client (ms, tps) = do
  sreader <- S.newStreamReader client (fromIntegral ms) 4096
  forM_ tps $ \tp -> do
    end <- S.getTailSequenceNum client tp
    S.readerStartReading sreader tp (end + 1) maxBound
  return sreader

-- | poll value, You can specify the batch size
poll :: S.StreamReader -> Int -> IO [S.DataRecord]
poll sreader m = S.readerRead sreader m

-- | poll value, You can specify the batch size and timeout
pollWithTimeout :: S.StreamReader -> Int -> Int32 -> IO [S.DataRecord]
pollWithTimeout sreader m timeout = S.readerSetTimeout sreader timeout >> S.readerRead sreader m
