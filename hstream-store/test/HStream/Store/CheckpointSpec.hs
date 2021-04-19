{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Store.CheckpointSpec where

import qualified Data.Map             as Map
import qualified HStream.Store.Stream as S
import           Test.Hspec

spec :: Spec
spec = describe "HStream.Store.Checkpoint" $ do
  fileCheckpointStore
  asyncFileCheckpointStore
  rsmCheckpointStore

fileCheckpointStore :: Spec
fileCheckpointStore = do
  it "FileBasedCheckpointStore: the offset of writing/reading shoule be equal" $ do
    client <- S.newStreamClient "/data/store/logdevice.conf"
    reader <- S.newStreamReader client 1 (-1)
    let readerName = "reader_name_ckp_1"
    let ckpPath = "/tmp/ckp"
    let logid = S.mkTopicID 1

    checkpointStore <- S.newFileBasedCheckpointStore ckpPath
    ckpReader <- S.newStreamSyncCheckpointedReader readerName reader checkpointStore 10
    _ <- S.append client logid "hello" Nothing
    until_lsn <- S.getTailSequenceNum client logid
    S.writeCheckpointsSync ckpReader (Map.fromList [(logid, until_lsn)])

    checkpointStore' <- S.newFileBasedCheckpointStore ckpPath
    S.getSequenceNum checkpointStore' readerName logid `shouldReturn` until_lsn

asyncFileCheckpointStore :: Spec
asyncFileCheckpointStore = do
  it "Async LSN get and update" $ do
    client <- S.newStreamClient "/data/store/logdevice.conf"
    reader <- S.newStreamReader client 1 (-1)
    let readerName = "reader_name_ckp_1"
    let ckpPath = "/tmp/ckp"
    let logid = S.mkTopicID 1

    checkpointStore <- S.newFileBasedCheckpointStore ckpPath
    ckpReader <- S.newStreamSyncCheckpointedReader readerName reader checkpointStore 10
    _ <- S.append client logid "hello" Nothing
    until_lsn <- S.getTailSequenceNum client logid
    S.writeCheckpoints ckpReader (Map.fromList [(logid, until_lsn)])

    checkpointStore' <- S.newFileBasedCheckpointStore ckpPath
    S.getSequenceNum checkpointStore' readerName logid `shouldReturn` until_lsn
    let random_lsn = 0x1020304050607080
    S.updateSequenceNum checkpointStore' readerName logid random_lsn
    S.getSequenceNum checkpointStore' readerName logid `shouldReturn` random_lsn

    S.checkpointedReaderStartReading ckpReader logid until_lsn until_lsn
    _ <- S.checkpointedReaderRead ckpReader 10
    S.writeLastCheckpoints ckpReader [logid]
    S.getSequenceNum checkpointStore' readerName logid `shouldReturn` until_lsn

rsmCheckpointStore :: Spec
rsmCheckpointStore = do
  it "newRSMBasedCheckpointStore" $ do
    client <- S.newStreamClient "/data/store/logdevice.conf"
    reader <- S.newStreamReader client 1 (-1)
    let readerName = "reader_name_ckp_2"
    let logid = S.mkTopicID 3
    let ckLogID = S.mkTopicID 4

    checkpointStore <- S.newRSMBasedCheckpointStore client ckLogID 5000
    ckpReader <- S.newStreamSyncCheckpointedReader readerName reader checkpointStore 10

    _ <- S.appendSync client logid "hello" Nothing
    until_lsn <- S.getTailSequenceNum client logid
    S.writeCheckpointsSync ckpReader (Map.fromList [(logid, until_lsn)])

    checkpointStore' <- S.newRSMBasedCheckpointStore client ckLogID 5000
    S.getSequenceNumSync checkpointStore' readerName logid `shouldReturn` until_lsn

    _ <- S.appendSync client logid "hello" Nothing
    until_lsn' <- S.getTailSequenceNum client logid
    S.checkpointedReaderStartReading ckpReader logid until_lsn' until_lsn'
    _ <- S.checkpointedReaderRead ckpReader 10
    S.writeLastCheckpointsSync ckpReader [logid]
    checkpointStore'' <- S.newRSMBasedCheckpointStore client ckLogID 5000
    S.getSequenceNumSync checkpointStore'' readerName logid `shouldReturn` until_lsn'
