{-# LANGUAGE OverloadedStrings #-}

module HStream.Store.CheckpointStoreSpec (spec) where

import qualified Data.Vector.Primitive   as VP
import           Test.Hspec

import qualified HStream.Store           as S
import           HStream.Store.SpecUtils

spec :: Spec
spec = describe "CheckpointStore" $ do
  fileBased
  rsmBased

fileBased :: Spec
fileBased = context "FileBasedCheckpointStore" $ do
  let ckpPath = "/tmp/ckp"
  storeSpec $ S.newFileBasedCheckpointStore ckpPath

rsmBased :: Spec
rsmBased = context "RSMBasedCheckpointedReader" $ do
  storeSpec $ S.newRSMBasedCheckpointStore client S.checkpointStoreLogID 5000

storeSpec :: IO S.LDCheckpointStore -> Spec
storeSpec new_ckp_store = do
  let logid = 1
  checkpointStore <- runIO new_ckp_store

  it "write & read should be same" $ do
    S.ckpStoreUpdateLSN checkpointStore "customer1" logid 2
    S.ckpStoreGetLSN checkpointStore "customer1" logid `shouldReturn` 2

  it "remove checkpoints" $ do
    S.ckpStoreGetLSN checkpointStore "customer2" logid `shouldThrow` S.isNOTFOUND
    S.ckpStoreUpdateLSN checkpointStore "customer2" logid 2
    S.ckpStoreGetLSN checkpointStore "customer2" logid `shouldReturn` 2
    S.ckpStoreRemoveCheckpoints checkpointStore "customer2" (VP.singleton logid)
    S.ckpStoreGetLSN checkpointStore "customer2" logid `shouldThrow` S.isNOTFOUND

  it "remove all checkpoints" $ do
    S.ckpStoreGetLSN checkpointStore "customer2" logid `shouldThrow` S.isNOTFOUND
    S.ckpStoreUpdateLSN checkpointStore "customer2" logid 2
    S.ckpStoreGetLSN checkpointStore "customer2" logid `shouldReturn` 2
    S.ckpStoreRemoveAllCheckpoints checkpointStore "customer2"
    S.ckpStoreGetLSN checkpointStore "customer2" logid `shouldThrow` S.isNOTFOUND
