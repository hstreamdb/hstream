{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module HStream.PubSub where --(Topic (..), pubMessage, sub, subEnd, poll) where

import           Control.Concurrent
import           Control.Exception
import           Control.Monad
import           Data.Int                 (Int32)
import qualified Data.Map.Strict          as Map
import           Data.Word                (Word64)
import qualified HStream.Store            as S
import           HStream.Store.Checkpoint
import           System.Random
import           Z.Data.Builder
import           Z.Data.CBytes            as CB
import           Z.Data.Parser            as P
import           Z.Data.Text
import           Z.Data.Vector
import           Z.IO.Time

topicToCbytes :: Topic -> CBytes
topicToCbytes (Topic t) = fromBytes (getUTF8Bytes t)

topicTail :: CBytes
topicTail = "_topic$tail"

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

type ClientID = Text

createTopic ::
  S.StreamClient ->
  Topic ->
  Int -> -- Replication Factor
  IO (Either String S.StreamTopicGroup)
createTopic client topic rf = do
  at <- S.newTopicAttributes
  S.setTopicReplicationFactor at rf
  logID <- getRandomLogID
  let li = S.mkTopicID logID
  v <- try $ S.makeTopicGroupSync client (topicToCbytes topic <> topicTail) li li at True
  case v of
    Left (e :: SomeException) -> do
      return $ Left $ show e
    Right v0 -> do
      return $ Right v0

-- | create topic and record message
pub ::
  S.StreamClient -> -- client
  Topic -> --- Topic
  Message -> -- Message
  IO (Either String S.SequenceNum)
pub client topic message = do
  try (S.getTopicGroupSync client (topicToCbytes topic <> topicTail)) >>= \case
    Left (e :: SomeException) -> return $ Left (show e)
    Right gs -> do
      (a, _) <- S.topicGroupGetRange gs
      Right <$> S.appendSync client a message Nothing

-- | sub a topic, return the StreamReader. You follow the tail value
sub ::
  S.StreamClient ->
  Topic ->
  IO (Either String S.StreamReader)
sub client tp = do
  sreader <- S.newStreamReader client 1 4096
  try (S.getTopicGroupSync client (topicToCbytes tp <> topicTail)) >>= \case
    Left (e :: SomeException) -> return $ Left (show e)
    Right gs -> do
      (a, _) <- S.topicGroupGetRange gs
      end <- S.getTailSequenceNum client a
      S.readerStartReading sreader a (end + 1) maxBound
      return $ Right sreader

subs ::
  S.StreamClient ->
  Int -> -- max sub number
  [Topic] ->
  IO ([Either String S.StreamReader])
subs client ms tps = do
  sreader <- S.newStreamReader client (fromIntegral ms) 4096
  forM tps $ \tp -> do
    try (S.getTopicGroupSync client (topicToCbytes tp <> topicTail)) >>= \case
      Left (e :: SomeException) -> return $ Left (show e)
      Right gs -> do
        (a, _) <- S.topicGroupGetRange gs
        end <- S.getTailSequenceNum client a
        S.readerStartReading sreader a (end + 1) maxBound
        return $ Right sreader

-- | poll value, You can specify the batch size
poll :: S.StreamReader -> Int -> IO [S.DataRecord]
poll sreader m = S.readerRead sreader m

pollWithTimeout :: S.StreamReader -> Int -> Int32 -> IO [S.DataRecord]
pollWithTimeout sreader m timeout = S.readerSetTimeout sreader timeout >> S.readerRead sreader m

createClientID ::
  S.StreamClient ->
  Topic ->
  ClientID ->
  Int ->
  IO (Either String S.StreamTopicGroup)
createClientID client (Topic tp) cid rf =
  createTopic client (Topic $ tp <> "_clientID_" <> cid) rf

-- | commit a topic ClientID's SequenceNum
commit ::
  S.StreamClient ->
  Topic ->
  ClientID ->
  S.SequenceNum ->
  IO (Either String S.SequenceNum)
commit client (Topic topic) cid (S.SequenceNum seqN) =
  pub
    client
    (Topic $ topic <> "_clientID_" <> cid)
    (build $ encodePrim seqN)

-- | read last commit SequenceNum
readLastCommit ::
  S.StreamClient ->
  Topic ->
  ClientID ->
  IO (Either String S.SequenceNum)
readLastCommit client (Topic tp) cid = do
  sreader <- S.newStreamReader client 1 1024
  try (S.getTopicGroupSync client ((topicToCbytes $ Topic $ tp <> "_clientID_" <> cid) <> topicTail)) >>= \case
    Left (e :: SomeException) -> return $ Left (show e)
    Right gs -> do
      (a, _) <- S.topicGroupGetRange gs
      end <- S.getTailSequenceNum client a
      S.readerStartReading sreader a end maxBound
      v <- S.readerRead sreader 1
      case v of
        [S.DataRecord _ _ bs] ->
          case P.parse' @Word64 P.decodePrim bs of
            Left e  -> return $ Left (show e)
            Right s -> return $ Right (S.SequenceNum s)
        _ -> return $ Left "can't find commit"

-- local checkpoint
-- create checkpoint
createCheckpoint :: ClientID -> IO CheckpointStore
createCheckpoint cid = newFileBasedCheckpointStore $ fromBytes $ getUTF8Bytes cid

getTopicID :: S.StreamClient -> Topic -> IO (Either SomeException S.TopicID)
getTopicID client topic = do
  try (S.getTopicGroupSync client (topicToCbytes topic <> topicTail)) >>= \case
    Left (e :: SomeException) -> return $ Left e
    Right gs                  -> Right . fst <$> S.topicGroupGetRange gs

updateCheckpoint :: S.StreamClient -> CheckpointStore -> ClientID -> Topic -> S.SequenceNum -> IO ()
updateCheckpoint client cp cid topic seqN =
  getTopicID client topic >>= \case
    Left s -> throwIO s
    Right tid -> updateSequenceNumSync cp (fromBytes $ getUTF8Bytes cid) tid seqN

updateCheckpoints :: S.StreamClient -> CheckpointStore -> ClientID -> [(Topic, S.SequenceNum)] -> IO ()
updateCheckpoints client cp cid ls = do
  tpids <- forM ls $ \(t, s) ->
    getTopicID client t >>= \case
      Left (e :: SomeException) -> throwIO e
      Right tid                 -> return (tid, s)
  updateMultiSequenceNumSync cp (fromBytes $ getUTF8Bytes cid) (Map.fromList tpids)

readCheckpoint :: S.StreamClient -> CheckpointStore -> ClientID -> Topic -> IO (Either SomeException S.SequenceNum)
readCheckpoint client cp cid topic =
  getTopicID client topic >>= \case
    Left s -> return $ Left s
    Right tid ->
      (try $ getSequenceNumSync cp (fromBytes $ getUTF8Bytes cid) tid) >>= \case
        Left (e :: SomeException) -> return $ Left e
        Right seqN                -> return $ Right seqN
