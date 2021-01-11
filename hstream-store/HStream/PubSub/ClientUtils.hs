{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module HStream.PubSub.ClientUtils where

import           Control.Monad
import           Control.Monad.Except
import qualified Data.Map.Strict           as Map
import           Data.Word                 (Word64)
import           HStream.PubSub.TopicUtils
import           HStream.PubSub.Types
import           HStream.Store
import           Z.Data.Builder
import           Z.Data.CBytes
import           Z.Data.Parser             as P
import           Z.Data.Text

createClientID ::
  GlobalTM ->
  StreamClient ->
  Topic ->
  ClientID ->
  Int ->
  EIO TopicID
createClientID gtm client (Topic tp) cid rf =
  createTopic gtm client (Topic $ tp <> "_clientID_" <> cid) rf

-- | commit a topic ClientID's SequenceNum
commit ::
  GlobalTM ->
  StreamClient ->
  Topic ->
  ClientID ->
  SequenceNum ->
  EIO SequenceNum
commit gtm client (Topic topic) cid (SequenceNum seqN) =
  pub
    gtm
    client
    (Topic $ topic <> "_clientID_" <> cid)
    (build $ encodePrim seqN)

-- | read last commit SequenceNum
readLastCommit ::
  GlobalTM ->
  StreamClient ->
  Topic ->
  ClientID ->
  EIO (Either String SequenceNum)
readLastCommit gtm client (Topic tp) cid = do
  sreader <- liftIO $ newStreamReader client 1 1024
  tid <- getTopicID gtm client $ Topic $ tp <> "_clientID_" <> cid
  end <- liftIO $ getTailSequenceNum client tid
  liftIO $ readerStartReading sreader tid end maxBound
  v <- liftIO $ readerRead sreader 1
  case v of
    [DataRecord _ _ bs] ->
      case P.parse' @Word64 P.decodePrim bs of
        Left e  -> return $ Left $ show e
        Right s -> return $ Right $ (SequenceNum s)
    _ -> return $ Left "can't find commit"

-- local checkpoint
-- create checkpoint
createCheckpoint :: ClientID -> IO CheckpointStore
createCheckpoint cid = newFileBasedCheckpointStore $ fromBytes $ getUTF8Bytes cid

updateCheckpoint :: GlobalTM -> StreamClient -> CheckpointStore -> ClientID -> Topic -> SequenceNum -> EIO ()
updateCheckpoint gtm client cp cid topic seqN = do
  tid <- getTopicID gtm client topic
  liftIO $ updateSequenceNumSync cp (fromBytes $ getUTF8Bytes cid) tid seqN

updateCheckpoints :: GlobalTM -> StreamClient -> CheckpointStore -> ClientID -> [(Topic, SequenceNum)] -> EIO ()
updateCheckpoints gtm client cp cid ls = do
  tpids <- forM ls $ \(t, s) -> do
    tid <- getTopicID gtm client t
    return (tid, s)
  liftIO $ updateMultiSequenceNumSync cp (fromBytes $ getUTF8Bytes cid) (Map.fromList tpids)

readCheckpoint :: GlobalTM -> StreamClient -> CheckpointStore -> ClientID -> Topic -> EIO SequenceNum
readCheckpoint gtm client cp cid topic = do
  tid <- getTopicID gtm client topic
  try' $ getSequenceNumSync cp (fromBytes $ getUTF8Bytes cid) tid
