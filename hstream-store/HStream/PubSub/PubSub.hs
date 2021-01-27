{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module HStream.PubSub.PubSub where

import           Control.Exception
import           Control.Monad.Except
import           Data.IORef
import           Data.Int                       (Int32)
import qualified HStream.PubSub.ClientUtils     as C
import qualified HStream.PubSub.Internal.PubSub as I
import qualified HStream.PubSub.TopicUtils      as T
import           HStream.PubSub.Types
import           HStream.Store
import           HStream.Store.Exception
import           Z.Data.CBytes
import           Z.Data.Text

createTopic ::
  StreamClient ->
  Topic ->
  Int ->
  IO (Either SomeStreamException ())
createTopic client topic rf = runExceptT $ T.createTopic client topic rf

-- | create topic and record message
pub ::
  StreamClient -> -- client
  Topic -> --- Topic
  Message -> -- Message
  IO (Either SomeStreamException SequenceNum)
pub client topic message = runExceptT $ T.pub client topic message

-- -- | sub a topic, return the StreamReader. You follow the tail value
sub ::
  StreamClient ->
  Int ->
  [Topic] ->
  IO (Either SomeStreamException StreamReader)
sub client ms tps = runExceptT $ T.sub client ms tps

-- | sub topic
seek' ::
  StreamClient ->
  StreamReader ->
  Topic -> -- topic list
  SequenceNum ->
  EIO ()
seek' client sreader tp offset = do
  tid <- T.getTopicID client tp
  liftIO $ readerStartReading sreader tid offset maxBound
  return ()

seek1 ::
  StreamClient ->
  StreamReader ->
  Topic -> -- topic list
  SequenceNum ->
  IO ()
seek1 client sreader tp offset = do
  v <- runExceptT $ seek' client sreader tp offset
  case v of
    Left e  -> throwIO e
    Right _ -> return ()

-- | poll value, You can specify the batch size
poll :: StreamReader -> Int -> IO [DataRecord]
poll = I.poll

pollWithTimeout :: StreamReader -> Int -> Int32 -> IO [DataRecord]
pollWithTimeout = I.pollWithTimeout

createCheckpoint :: ClientID -> IO CheckpointStore
createCheckpoint cid = newFileBasedCheckpointStore $ fromBytes $ getUTF8Bytes cid

updateCheckpoint :: StreamClient -> CheckpointStore -> ClientID -> Topic -> SequenceNum -> IO (Either SomeStreamException ())
updateCheckpoint client cp cid topic seqN = runExceptT $ C.updateCheckpoint client cp cid topic seqN

updateCheckpoints :: StreamClient -> CheckpointStore -> ClientID -> [(Topic, SequenceNum)] -> IO (Either SomeStreamException ())
updateCheckpoints  b c d e = runExceptT $ C.updateCheckpoints b c d e

readCheckpoint :: StreamClient -> CheckpointStore -> ClientID -> Topic -> IO (Either SomeStreamException SequenceNum)
readCheckpoint client cp cid topic = runExceptT $ C.readCheckpoint client cp cid topic
