{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Store.CheckpointSpec where

import qualified Data.Map             as Map
import qualified HStream.Store.Stream as S
import           Test.Hspec

spec :: Spec
spec = describe "HStream.Store.Checkpoint" $ do
  it "newRSMbasedCheckpointStore" $ do
    client <- S.newStreamClient "/data/store/logdevice.conf"
    reader <- S.newStreamReader client 1 (-1)
    let readerName = "reader_name"
    let ckLogID = S.mkTopicID 4
    checkpointStore <- S.newRSMBasedCheckpointStore client ckLogID 5000
    ckpReader <- S.newStreamSyncCheckpointedReader readerName reader checkpointStore 10
    let logid = S.mkTopicID 3

    _ <- S.appendSync client logid "hello" Nothing
    until_lsn <- S.getTailSequenceNum client logid
    S.writeCheckpointsSync ckpReader (Map.fromList [(logid, until_lsn)])
    checkpointStore' <- S.newRSMBasedCheckpointStore client ckLogID 5000
    S.getSequenceNumSync checkpointStore' readerName logid `shouldReturn` until_lsn

    _ <- S.appendSync client logid "hello" Nothing
    until_lsn' <- S.getTailSequenceNum client logid
    S.checkpointedReaderStartReading ckpReader logid until_lsn' until_lsn'
    xs <- S.checkpointedReaderRead ckpReader 10
    S.writeLastCheckpointsSync ckpReader [logid]
    checkpointStore'' <- S.newRSMBasedCheckpointStore client ckLogID 5000
    S.getSequenceNumSync checkpointStore'' readerName logid `shouldReturn` until_lsn'
