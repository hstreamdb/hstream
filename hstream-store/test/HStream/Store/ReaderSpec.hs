{-# LANGUAGE OverloadedStrings #-}

module HStream.Store.ReaderSpec (spec) where

import           Control.Monad           (void)
import           Data.Bits               (shiftL)
import qualified Data.Map.Strict         as Map
import qualified HStream.Store           as S
import           System.Timeout          (timeout)
import           Test.Hspec

import           HStream.Store.SpecUtils

spec :: Spec
spec = describe "Stream Reader" $ do
  fileBased
  preRsmBased >> rsmBased
  misc

fileBased :: Spec
fileBased = context "FileBasedCheckpointedReader" $ do
  let readerName = "reader_name_ckp_1"
  let ckpPath = "/tmp/ckp"
  let logid = 1

  ckpReader <- runIO $ S.newLDFileCkpReader client readerName ckpPath 1 Nothing 10

  it "the checkpoint of writing/reading shoule be equal" $ do
    checkpointStore <- S.newFileBasedCheckpointStore ckpPath
    _ <- S.append client logid "hello" Nothing
    until_lsn <- S.getTailLSN client logid
    S.writeCheckpoints ckpReader (Map.fromList [(logid, until_lsn)])
    S.ckpStoreGetLSN checkpointStore readerName logid `shouldReturn` until_lsn

  it "read with checkpoint" $ do
    start_lsn <- S.appendCompLSN <$> S.append client logid "1" Nothing
    _ <- S.append client logid "2" Nothing
    end_lsn <- S.appendCompLSN <$> S.append client logid "3" Nothing

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

preRsmBased :: Spec
preRsmBased = context "Pre-RSMBasedCheckpointedReader" $ do
  it "get the logid for checkpointStore" $ do
    let attrs = S.LogAttrs S.HsLogAttrs { S.logReplicationFactor = 1
                                        , S.logExtraAttrs = Map.empty
                                        }
    S.initCheckpointStoreLogID client attrs `shouldReturn` (1 `shiftL` 56)

rsmBased :: Spec
rsmBased = context "RSMBasedCheckpointedReader" $ do
  let readerName = "reader_name_ckp_2"
  let logid = 1
  ckpReader <- runIO $ S.newLDRsmCkpReader client readerName S.checkpointStoreLogID 5000 1 Nothing 10

  it "the checkpoint of writing/reading shoule be equal" $ do
    _ <- S.append client logid "hello" Nothing
    until_lsn <- S.getTailLSN client logid
    S.writeCheckpoints ckpReader (Map.fromList [(logid, until_lsn)])
    checkpointStore <- S.newRSMBasedCheckpointStore client S.checkpointStoreLogID 5000
    S.ckpStoreGetLSN checkpointStore readerName logid `shouldReturn` until_lsn

  it "read with checkpoint" $ do
    start_lsn <- S.appendCompLSN <$> S.append client logid "1" Nothing
    _ <- S.append client logid "2" Nothing
    end_lsn <- S.appendCompLSN <$> S.append client logid "3" Nothing

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

  -- TODO
  -- it "Set IncludeByteOffset" $ do
  --   reader <- S.newLDReader client 1 Nothing
  --   sn <- S.getTailLSN client logid
  --   S.readerStartReading reader logid sn sn
  --   S.recordByteOffset . head <$> S.readerRead reader 1 `shouldReturn` S.RecordByteOffsetInvalid

  --   S.readerSetIncludeByteOffset reader
  --   S.readerStartReading reader logid sn sn
  --   S.recordByteOffset . head <$> S.readerRead reader 1
