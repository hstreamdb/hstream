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

-- local checkpoint
-- create checkpoint
createCheckpoint :: ClientID -> IO CheckpointStore
createCheckpoint cid = newFileBasedCheckpointStore $ fromBytes $ getUTF8Bytes cid

updateCheckpoint :: StreamClient -> CheckpointStore -> ClientID -> Topic -> SequenceNum -> EIO ()
updateCheckpoint client cp cid topic seqN = do
  tid <- getTopicID client topic
  liftIO $ updateSequenceNumSync cp (fromBytes $ getUTF8Bytes cid) tid seqN

updateCheckpoints :: StreamClient -> CheckpointStore -> ClientID -> [(Topic, SequenceNum)] -> EIO ()
updateCheckpoints client cp cid ls = do
  tpids <- forM ls $ \(t, s) -> do
    tid <- getTopicID client t
    return (tid, s)
  liftIO $ updateMultiSequenceNumSync cp (fromBytes $ getUTF8Bytes cid) (Map.fromList tpids)

readCheckpoint :: StreamClient -> CheckpointStore -> ClientID -> Topic -> EIO SequenceNum
readCheckpoint client cp cid topic = do
  tid <- getTopicID client topic
  try' $ getSequenceNumSync cp (fromBytes $ getUTF8Bytes cid) tid
