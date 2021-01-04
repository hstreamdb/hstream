{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module HStream.PubSub.PubSub (Topic (..), pubMessage, sub, subEnd, poll) where

import Control.Concurrent
import Control.Exception
import Data.Word (Word64)
import qualified HStream.Store as S
import System.Random
import Z.Data.CBytes as CB
import Z.Data.Text
import Z.Data.Vector
import Z.IO.Time

topicToCbytes :: Topic -> CBytes
topicToCbytes (Topic t) = fromBytes (getUTF8Bytes t)

topicTail :: CBytes
topicTail = "_emqx$tail"

-- | mqtt Topic
-- e: "a/a/a/a", "a/b"
data Topic = Topic Text deriving (Show, Eq, Ord)

type Message = Bytes

data Filter = Filter Text deriving (Show, Eq, Ord)

-- | create logID random
getRandomLogID :: IO Word64
getRandomLogID = do
  t <- getSystemTime'
  let i = fromIntegral $ systemSeconds t
  r <- randomRIO @Word64 (1, 100000)
  return (i * 100000 + r)

-- | create topic and record message
-- when topic first arrive, we create a topic logGroup. this logGroup include two logId: a , a+1
-- a: record log message
-- a+1: record log metadata (e: commite message)
--
-- e:
-- fun :: IO ()
-- fun = do
--   _ <- S.setLoggerlevelError
--   client <- S.newStreamClient "/data/logdevice/logdevice.conf"
--   forever $ do
--     t : m : _ <- Prelude.words <$> getLine
--     let tp = Topic (Z.Data.Text.pack t)
--     v <- pubMessage client tp (packASCII m) 2 1000000
--     print v
pubMessage ::
  S.StreamClient -> -- client
  Topic -> --- Topic
  Message -> -- Message
  Int -> -- Replication Factor
  Int -> -- sleep time after logGroup created
  IO (S.SequenceNum)
pubMessage client topic message rf slt = do
  try (S.getTopicGroupSync client (topicToCbytes topic <> topicTail)) >>= \case
    Left (_ :: SomeException) -> do
      at <- S.newTopicAttributes
      S.setTopicReplicationFactor at rf
      logID <- getRandomLogID
      let li = S.mkTopicID logID
      v <- try $ S.makeTopicGroupSync client (topicToCbytes topic <> topicTail) li (li + 1) at True
      case v of
        Left (_ :: SomeException) -> do
          pubMessage client topic message rf slt
        Right _ -> do
          threadDelay slt
          pubMessage client topic message rf slt
    Right gs -> do
      (a, _) <- S.topicGroupGetRange gs
      S.appendSync client a message Nothing

-- | sub a Topic, return the StreamReader. You need to specify a valid start SequenceNum
sub ::
  S.StreamClient ->
  Topic ->
  S.SequenceNum -> -- start SequenceNum
  IO (Either String S.StreamReader)
sub client tp start = do
  sreader <- S.newStreamReader client 1 4096
  try (S.getTopicGroupSync client (topicToCbytes tp <> topicTail)) >>= \case
    Left (e :: SomeException) -> return $ Left (show e)
    Right gs -> do
      (a, _) <- S.topicGroupGetRange gs
      _ <- S.startReading sreader a start maxBound
      return $ Right sreader

-- | sub a topic, return the StreamReader. You follow the tail value
subEnd ::
  S.StreamClient ->
  Topic ->
  IO (Either String S.StreamReader)
subEnd client tp = do
  sreader <- S.newStreamReader client 1 4096
  try (S.getTopicGroupSync client (topicToCbytes tp <> topicTail)) >>= \case
    Left (e :: SomeException) -> return $ Left (show e)
    Right gs -> do
      (a, _) <- S.topicGroupGetRange gs
      end <- S.getTailSequenceNum client a
      i <- S.startReading sreader a (end + 1) maxBound
      case i of
        0 -> return $ Right sreader
        _ -> return $ Left $ "sub error " ++ show i

-- | poll value, You can specify the batch size
-- e:
-- fun :: IO ()
-- fun = do
--   _ <- S.setLoggerlevelError
--   client <- S.newStreamClient "/data/logdevice/logdevice.conf"
--   Right sreader <- subEnd client (Topic "a/a")
--   record <- poll sreader 1  -- poll a value,
--   print record
poll :: S.StreamReader -> Int -> IO (Maybe [S.DataRecord])
poll sreader m = S.read sreader m
