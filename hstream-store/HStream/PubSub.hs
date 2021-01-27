{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module HStream.PubSub where

import           Control.Exception
import           Control.Monad.Except
import           Data.IORef
import           Data.Int                       (Int32)
import qualified HStream.PubSub.ClientUtils     as C
import qualified HStream.PubSub.Internal.PubSub as I
import           HStream.PubSub.TopicMap
import qualified HStream.PubSub.TopicUtils      as T
import           HStream.PubSub.Types
import           HStream.Store
import           HStream.Store.Exception
import           Z.Data.CBytes
import           Z.Data.Text

initGlobalTM :: IO GlobalTM
initGlobalTM = newIORef emptyTM

createTopic ::
  GlobalTM ->
  StreamClient ->
  Topic ->
  Int ->
  IO (Either SomeStreamException TopicID)
createTopic gtm client topic rf = runExceptT $ T.createTopic gtm client topic rf

-- | create topic and record message
pub ::
  GlobalTM ->
  StreamClient -> -- client
  Topic -> --- Topic
  Message -> -- Message
  IO (Either SomeStreamException SequenceNum)
pub gtm client topic message = runExceptT $ T.pub gtm client topic message

-- -- | sub a topic, return the StreamReader. You follow the tail value
sub ::
  GlobalTM ->
  StreamClient ->
  Int ->
  [Topic] ->
  IO (Either SomeStreamException StreamReader)
sub gtm client ms tps = runExceptT $ T.sub gtm client ms tps

-- | sub topic
seek' ::
  GlobalTM -> -- global topic map
  StreamClient ->
  StreamReader ->
  Topic -> -- topic list
  SequenceNum ->
  EIO ()
seek' gtm client sreader tp offset = do
  tid <- T.getTopicID gtm client tp
  liftIO $ readerStartReading sreader tid offset maxBound
  return ()

seek1 ::
  GlobalTM -> -- global topic map
  StreamClient ->
  StreamReader ->
  Topic -> -- topic list
  SequenceNum ->
  IO ()
seek1 gtm client sreader tp offset = do
  v <- runExceptT $ seek' gtm client sreader tp offset
  case v of
    Left e  -> throwIO e
    Right _ -> return ()

-- | poll value, You can specify the batch size
poll :: StreamReader -> Int -> IO [DataRecord]
poll = I.poll

pollWithTimeout :: StreamReader -> Int -> Int32 -> IO [DataRecord]
pollWithTimeout = I.pollWithTimeout

createClientID ::
  GlobalTM ->
  StreamClient ->
  Topic ->
  ClientID ->
  Int ->
  IO (Either SomeStreamException TopicID)
createClientID gtm client tp cid rf = runExceptT $ C.createClientID gtm client tp cid rf

-- | commit a topic ClientID's SequenceNum
commit ::
  GlobalTM ->
  StreamClient ->
  Topic ->
  ClientID ->
  SequenceNum ->
  IO (Either SomeStreamException SequenceNum)
commit gtm client topic cid seqN = runExceptT $ C.commit gtm client topic cid seqN

-- | read last commit SequenceNum
readLastCommit ::
  GlobalTM ->
  StreamClient ->
  Topic ->
  ClientID ->
  IO (Either String SequenceNum)
readLastCommit gtm client tp cid = do
  v <- runExceptT $ C.readLastCommit gtm client tp cid
  case v of
    Left e  -> return $ Left $ show e
    Right a -> return a

createCheckpoint :: ClientID -> IO CheckpointStore
createCheckpoint cid = newFileBasedCheckpointStore $ fromBytes $ getUTF8Bytes cid

updateCheckpoint :: GlobalTM -> StreamClient -> CheckpointStore -> ClientID -> Topic -> SequenceNum -> IO (Either SomeStreamException ())
updateCheckpoint gtm client cp cid topic seqN = runExceptT $ C.updateCheckpoint gtm client cp cid topic seqN

updateCheckpoints :: GlobalTM -> StreamClient -> CheckpointStore -> ClientID -> [(Topic, SequenceNum)] -> IO (Either SomeStreamException ())
updateCheckpoints a b c d e = runExceptT $ C.updateCheckpoints a b c d e

readCheckpoint :: GlobalTM -> StreamClient -> CheckpointStore -> ClientID -> Topic -> IO (Either SomeStreamException SequenceNum)
readCheckpoint gtm client cp cid topic = runExceptT $ C.readCheckpoint gtm client cp cid topic
