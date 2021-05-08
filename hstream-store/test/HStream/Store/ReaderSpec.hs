{-# LANGUAGE BangPatterns      #-}
{-# LANGUAGE OverloadedStrings #-}

module HStream.Store.ReaderSpec (spec) where

import           Control.Monad    (void)
import qualified Data.Map.Strict  as Map
import qualified HStream.Store    as S
import           System.IO.Unsafe (unsafePerformIO)
import           System.Timeout   (timeout)
import           Test.Hspec

client :: S.LDClient
client = unsafePerformIO $ S.newLDClient "/data/store/logdevice.conf"
{-# NOINLINE client #-}

spec :: Spec
spec = describe "Stream Reader" $ do
  fileBased
  rsmBased
  misc

fileBased :: Spec
fileBased = context "FileBasedCheckpointedReader" $ do
  let readerName = "reader_name_ckp_1"
  let ckpPath = "/tmp/ckp"
  let logid = 1

  let !ckpReader = unsafePerformIO $ S.newLDFileCkpReader client readerName ckpPath 1 Nothing 10
  let !checkpointStore = unsafePerformIO $ S.newFileBasedCheckpointStore ckpPath

  it "the checkpoint of writing/reading shoule be equal" $ do
    _ <- S.append client logid "hello" Nothing
    until_lsn <- S.getTailLSN client logid
    S.writeCheckpoints ckpReader (Map.fromList [(logid, until_lsn)])
    S.ckpStoreGetLSN checkpointStore readerName logid `shouldReturn` until_lsn

  it "read with checkpoint" $ do
    start_lsn <- S.appendCbLSN <$> S.append client logid "1" Nothing
    _ <- S.append client logid "2" Nothing
    end_lsn <- S.appendCbLSN <$> S.append client logid "3" Nothing

    S.ckpReaderStartReading ckpReader logid start_lsn end_lsn
    [record_1] <- S.ckpReaderRead ckpReader 1
    S.recordPayload record_1 `shouldBe` "1"
    S.recordLSN record_1 `shouldBe` start_lsn

    -- last read checkpoint: start_lsn
    S.writeLastCheckpoints ckpReader [logid]

    [record_2] <- S.ckpReaderRead ckpReader 1
    S.recordPayload record_2 `shouldBe` "2"

    S.startReadingFromCheckpoint ckpReader logid end_lsn
    [record'] <- S.ckpReaderRead ckpReader 1
    S.recordPayload record' `shouldBe` "2"

rsmBased :: Spec
rsmBased = context "RSMBasedCheckpointedReader" $ do
  let readerName = "reader_name_ckp_2"
  let ckpLogID = 90
  let logid = 1

  let !ckpReader = unsafePerformIO $ S.newLDRsmCkpReader client readerName ckpLogID 5000 1 Nothing 10
  let !checkpointStore = unsafePerformIO $ S.newRSMBasedCheckpointStore client ckpLogID 5000

  it "the checkpoint of writing/reading shoule be equal" $ do
    _ <- S.append client logid "hello" Nothing
    until_lsn <- S.getTailLSN client logid
    S.writeCheckpoints ckpReader (Map.fromList [(logid, until_lsn)])
    S.ckpStoreGetLSN checkpointStore readerName logid `shouldReturn` until_lsn

  it "read with checkpoint" $ do
    start_lsn <- S.appendCbLSN <$> S.append client logid "1" Nothing
    _ <- S.append client logid "2" Nothing
    end_lsn <- S.appendCbLSN <$> S.append client logid "3" Nothing

    S.ckpReaderStartReading ckpReader logid start_lsn end_lsn
    [record_1] <- S.ckpReaderRead ckpReader 1
    S.recordPayload record_1 `shouldBe` "1"
    S.recordLSN record_1 `shouldBe` start_lsn

    -- last read checkpoint: start_lsn
    S.writeLastCheckpoints ckpReader [logid]

    [record_2] <- S.ckpReaderRead ckpReader 1
    S.recordPayload record_2 `shouldBe` "2"

    S.startReadingFromCheckpoint ckpReader logid end_lsn
    [record'] <- S.ckpReaderRead ckpReader 1
    S.recordPayload record' `shouldBe` "2"

misc :: Spec
misc = do
  let logid = 1

  it "read timeout should return an empty results" $ do
    reader <- S.newLDReader client 1 Nothing
    sn <- S.getTailLSN client logid
    S.readerStartReading reader logid sn sn
    void $ S.readerSetTimeout reader 0
    timeout 1000000 (S.readerRead reader 1) `shouldReturn` Just []
