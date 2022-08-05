module HStream.AckSpec (spec) where

import           Data.Map.Strict            as Map
import           Data.Maybe                 (fromJust)
import           HStream.Server.Core.Common (getCommitRecordId,
                                             insertAckedRecordId, isSuccessor)
import           HStream.Server.Types
import           Test.Hspec

spec :: Spec
spec = describe "HStream.AckSpec" $ do
  isSuccessorSpec
  insertAckSpec

isSuccessorSpec :: Spec
isSuccessorSpec =
  describe "isSuccessor" $ do

    it "in same batch match" $ do
      let r1 = ShardRecordId 1 2
      let r2 = ShardRecordId 1 1
      let batchNumMap = Map.singleton 1 100
      isSuccessor r1 r2 batchNumMap `shouldBe` True
    it "in same batch not match" $ do
      let r1 = ShardRecordId 1 3
      let r2 = ShardRecordId 1 1
      let batchNumMap = Map.singleton 1 100
      isSuccessor r1 r2 batchNumMap `shouldBe` False
    it "not same batch match" $ do
      let r1 = ShardRecordId 2 0
      let r2 = ShardRecordId 1 99
      let batchNumMap = Map.singleton 1 100
      isSuccessor r1 r2 batchNumMap `shouldBe` True
    it "not same batch not match" $ do
      let r1 = ShardRecordId 2 1
      let r2 = ShardRecordId 1 99
      let batchNumMap = Map.singleton 1 100
      isSuccessor r1 r2 batchNumMap `shouldBe` False
    it "minBound as r1" $ do
      let r1 = minBound
      let r2 = ShardRecordId 1 99
      let batchNumMap = Map.singleton 1 100
      isSuccessor r1 r2 batchNumMap `shouldBe` False
    it "minBound as r2" $ do
      let r1 = ShardRecordId 1 2
      let r2 = minBound
      let batchNumMap = Map.singleton 1 100
      isSuccessor r1 r2 batchNumMap `shouldBe` False
    it "maxBound as r1" $ do
      let r1 = maxBound
      let r2 = ShardRecordId 1 99
      let batchNumMap = Map.singleton 1 100
      isSuccessor r1 r2 batchNumMap `shouldBe` False
    it "maxBound as r2" $ do
      let r1 = ShardRecordId 1 2
      let r2 = maxBound
      let batchNumMap = Map.singleton 1 100
      isSuccessor r1 r2 batchNumMap `shouldBe` False

insertAckSpec :: Spec
insertAckSpec =
  describe "insertAckedRecordId" $ do
    it "inset to empty" $ do
      let oldRanges = Map.empty
      let r = ShardRecordId 1 1
      let batchNumMap = Map.singleton 1 3
      let lowerBound = ShardRecordId minBound minBound
      fromJust (insertAckedRecordId r lowerBound oldRanges batchNumMap) `shouldBe` Map.singleton r (ShardRecordIdRange r r)
    it "no merge" $ do
      let range1L = ShardRecordId 1 0
      let range1R = ShardRecordId 2 0

      let range2L = ShardRecordId 3 1
      let range2R = ShardRecordId 3 2

      let oldRanges = Map.fromList [(range1L, ShardRecordIdRange range1L range1R), (range2L, ShardRecordIdRange range2L range2R)]
      let newR = ShardRecordId 2 2
      let newRanges = Map.insert newR (ShardRecordIdRange newR newR) oldRanges
      let batchNumMap = Map.fromList [(1, 5), (2, 5), (3, 5)]
      let lowerBound = ShardRecordId minBound minBound
      fromJust (insertAckedRecordId newR lowerBound oldRanges batchNumMap) `shouldBe` newRanges
    it "no merge 2" $ do
      let range1L = ShardRecordId 1 0
      let range1R = ShardRecordId 2 0

      let range2L = ShardRecordId 3 1
      let range2R = ShardRecordId 3 2

      let oldRanges = Map.fromList [(range1L, ShardRecordIdRange range1L range1R), (range2L, ShardRecordIdRange range2L range2R)]
      let newR = ShardRecordId 4 2
      let newRanges = Map.fromList [(range1L, ShardRecordIdRange range1L range1R), (range2L, ShardRecordIdRange range2L range2R), (newR, ShardRecordIdRange newR newR)]
      let batchNumMap = Map.fromList [(1, 5), (2, 5), (3, 5), (4, 5)]
      let lowerBound = ShardRecordId minBound minBound
      fromJust (insertAckedRecordId newR lowerBound oldRanges batchNumMap) `shouldBe` newRanges
    it "no merge 3" $ do
      let range1L = ShardRecordId 1 2
      let range1R = ShardRecordId 2 0

      let range2L = ShardRecordId 3 1
      let range2R = ShardRecordId 3 2

      let oldRanges = Map.fromList [(range1L, ShardRecordIdRange range1L range1R), (range2L, ShardRecordIdRange range2L range2R)]
      let newR = ShardRecordId 1 0
      let newRanges = Map.fromList [(newR, ShardRecordIdRange newR newR), (range1L, ShardRecordIdRange range1L range1R), (range2L, ShardRecordIdRange range2L range2R)]
      let batchNumMap = Map.fromList [(1, 5), (2, 5), (3, 5), (4, 5)]
      let lowerBound = ShardRecordId minBound minBound
      fromJust (insertAckedRecordId newR lowerBound oldRanges batchNumMap) `shouldBe` newRanges
    it "merge to left" $ do
      let range1L = ShardRecordId 1 0
      let range1R = ShardRecordId 2 0

      let range2L = ShardRecordId 3 1
      let range2R = ShardRecordId 3 2

      let oldRanges = Map.fromList [(range1L, ShardRecordIdRange range1L range1R), (range2L, ShardRecordIdRange range2L range2R)]
      let newR = ShardRecordId 2 1
      let newRanges = Map.fromList [(range1L, ShardRecordIdRange range1L newR), (range2L, ShardRecordIdRange range2L range2R)]
      let batchNumMap = Map.fromList [(1, 5), (2, 5), (3, 5)]
      let lowerBound = ShardRecordId minBound minBound
      fromJust (insertAckedRecordId newR lowerBound oldRanges batchNumMap) `shouldBe` newRanges
    it "merge to left 1" $ do
      let range1L = ShardRecordId 1 0
      let range1R = ShardRecordId 2 0

      let oldRanges = Map.fromList [(range1L, ShardRecordIdRange range1L range1R)]
      let newR = ShardRecordId 2 1
      let newRanges = Map.fromList [(range1L, ShardRecordIdRange range1L newR)]
      let batchNumMap = Map.fromList [(1, 5), (2, 5), (3, 5)]
      let lowerBound = ShardRecordId minBound minBound
      fromJust (insertAckedRecordId newR lowerBound oldRanges batchNumMap) `shouldBe` newRanges
    it "merge to right" $ do
      let range1L = ShardRecordId 1 0
      let range1R = ShardRecordId 2 0

      let range2L = ShardRecordId 3 1
      let range2R = ShardRecordId 3 2

      let oldRanges = Map.fromList [(range1L, ShardRecordIdRange range1L range1R), (range2L, ShardRecordIdRange range2L range2R)]
      let newR = ShardRecordId 3 0
      let newRanges = Map.fromList [(range1L, ShardRecordIdRange range1L range1R), (newR, ShardRecordIdRange newR range2R)]
      let batchNumMap = Map.fromList [(1, 5), (2, 5), (3, 5)]
      let lowerBound = ShardRecordId minBound minBound
      fromJust (insertAckedRecordId newR lowerBound oldRanges batchNumMap) `shouldBe` newRanges
    it "merge to right 2" $ do
      let range1L = ShardRecordId 1 0
      let range1R = ShardRecordId 2 0

      let range2L = ShardRecordId 3 1
      let range2R = ShardRecordId 3 2

      let oldRanges = Map.fromList [(range1L, ShardRecordIdRange range1L range1R), (range2L, ShardRecordIdRange range2L range2R)]
      let newR = ShardRecordId 3 3
      let newRanges = Map.fromList [(range1L, ShardRecordIdRange range1L range1R), (range2L, ShardRecordIdRange range2L newR)]
      let batchNumMap = Map.fromList [(1, 5), (2, 5), (3, 5)]
      let lowerBound = ShardRecordId minBound minBound
      fromJust (insertAckedRecordId newR lowerBound oldRanges batchNumMap) `shouldBe` newRanges
    it "merge to right 3" $ do
      let range2L = ShardRecordId 3 1
      let range2R = ShardRecordId 3 2

      let oldRanges = Map.fromList [(range2L, ShardRecordIdRange range2L range2R)]
      let newR = ShardRecordId 3 0
      let newRanges = Map.fromList [(newR, ShardRecordIdRange newR range2R)]
      let batchNumMap = Map.fromList [(1, 5), (2, 5), (3, 5)]
      let lowerBound = ShardRecordId minBound minBound
      fromJust (insertAckedRecordId newR lowerBound oldRanges batchNumMap) `shouldBe` newRanges
    it "merge to left and right" $ do
      let range1L = ShardRecordId 1 0
      let range1R = ShardRecordId 2 4

      let range2L = ShardRecordId 3 1
      let range2R = ShardRecordId 3 2

      let oldRanges = Map.fromList [(range1L, ShardRecordIdRange range1L range1R), (range2L, ShardRecordIdRange range2L range2R)]
      let newR = ShardRecordId 3 0
      let newRanges = Map.fromList [(range1L, ShardRecordIdRange range1L range2R)]
      let batchNumMap = Map.fromList [(1, 5), (2, 5), (3, 5)]
      let lowerBound = ShardRecordId minBound minBound
      fromJust (insertAckedRecordId newR lowerBound oldRanges batchNumMap) `shouldBe` newRanges
    it "no merge out of lowerBound" $ do
      let range1L = ShardRecordId 2 0
      let range1R = ShardRecordId 3 4
      let lowerBound = ShardRecordId 2 0

      let oldRanges = Map.fromList [(range1L, ShardRecordIdRange range1L range1R)]
      let newL = ShardRecordId 1 1
      let batchNumMap = Map.empty
      insertAckedRecordId newL lowerBound oldRanges batchNumMap `shouldBe` Nothing
    it "no merge invalid record" $ do
      let range1L = ShardRecordId 1 0
      let range1R = ShardRecordId 2 4

      let range2L = ShardRecordId 3 1
      let range2R = ShardRecordId 3 2

      let oldRanges = Map.fromList [(range1L, ShardRecordIdRange range1L range1R), (range2L, ShardRecordIdRange range2L range2R)]
      let invalidLSN = ShardRecordId 4 0
      let invalidIdx = ShardRecordId 2 8
      let batchNumMap = Map.fromList [(1, 5), (2, 5), (3, 5)]
      let lowerBound = ShardRecordId minBound minBound
      insertAckedRecordId invalidIdx lowerBound oldRanges batchNumMap `shouldBe` Nothing
      insertAckedRecordId invalidLSN lowerBound oldRanges batchNumMap `shouldBe` Nothing
    it "merge after gap record" $ do
      let recordInsert = ShardRecordId 10 0
      let lowerBound = ShardRecordId minBound minBound

      let gapL = ShardRecordId 5 minBound
      let gapR = ShardRecordId 9 maxBound

      let batchNumMap = Map.fromList [(9, 0), (10, 3)]
      let oldRanges = Map.fromList [(gapL, ShardRecordIdRange gapL gapR)]
      let newRanges = Map.fromList [(gapL, ShardRecordIdRange gapL recordInsert)]
      fromJust (insertAckedRecordId recordInsert lowerBound oldRanges batchNumMap) `shouldBe` newRanges
    it "merge before gap record" $ do
      let recordInsert = ShardRecordId 4 3
      let lowerBound = ShardRecordId minBound minBound

      let gapL = ShardRecordId 5 minBound
      let gapR = ShardRecordId 9 maxBound

      let batchNumMap = Map.fromList [(4, 4), (5, 0)]
      let oldRanges = Map.fromList [(gapL, ShardRecordIdRange gapL gapR)]
      let newRanges = Map.fromList [(recordInsert, ShardRecordIdRange recordInsert gapR)]
      fromJust (insertAckedRecordId recordInsert lowerBound oldRanges batchNumMap) `shouldBe` newRanges
    it "merge between gap record" $ do
      let recordInsert = ShardRecordId 5 0
      let lowerBound = ShardRecordId minBound minBound

      let gapLL = ShardRecordId 2 minBound
      let gapLR = ShardRecordId 4 maxBound
      let gapRL = ShardRecordId 6 minBound
      let gapRR = ShardRecordId 12 maxBound

      let batchNumMap = Map.fromList [(4, 0), (5, 1), (6, 0)]
      let oldRanges = Map.fromList [(gapLL, ShardRecordIdRange gapLL gapLR), (gapRL, ShardRecordIdRange gapRL gapRR)]
      let newRanges = Map.fromList [(gapLL, ShardRecordIdRange gapLL gapRR)]
      fromJust (insertAckedRecordId recordInsert lowerBound oldRanges batchNumMap) `shouldBe` newRanges
    it "filter duplicate ack record" $ do
      let recordInsert1 = ShardRecordId 5 0
      let recordInsert2 = ShardRecordId 7 3
      let lowerBound = ShardRecordId minBound minBound

      let rangeLL = ShardRecordId 2 0
      let rangeLR = ShardRecordId 5 8
      let rangeRL = ShardRecordId 6 0
      let rangeRR = ShardRecordId 12 5

      let batchNumMap = Map.fromList [(2, 5), (5, 10), (6, 7), (12, 9)]
      let oldRanges = Map.fromList [(rangeLL, ShardRecordIdRange rangeLL rangeLR), (rangeRL, ShardRecordIdRange rangeRL rangeRR)]
      insertAckedRecordId recordInsert1 lowerBound oldRanges batchNumMap `shouldBe` Nothing
      insertAckedRecordId recordInsert2 lowerBound oldRanges batchNumMap `shouldBe` Nothing
    it "test getCommitRecordId" $ do
        let ranges = Map.empty
        let batchNumMap = Map.empty
        getCommitRecordId ranges batchNumMap `shouldBe` Nothing

        let shardRecordId1 = ShardRecordId 3 0
        let shardRecordId2 = ShardRecordId 5 7
        let ranges2 = Map.fromList [(shardRecordId1, ShardRecordIdRange shardRecordId1 shardRecordId2)]
        let batchNumMap2 = Map.fromList [(2, 4), (3, 5), (4, 2), (5, 8)]
        getCommitRecordId ranges2 batchNumMap2 `shouldBe` Just shardRecordId2

        let shardRecordId3 = ShardRecordId 6 2
        let ranges3 = Map.insert shardRecordId3 (ShardRecordIdRange shardRecordId3 shardRecordId3) ranges2
        let batchNumMap3 = Map.insert 6 8 batchNumMap2
        getCommitRecordId ranges3 batchNumMap3 `shouldBe` Just shardRecordId2
        getCommitRecordId ranges3 Map.empty `shouldBe` Nothing
