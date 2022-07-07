{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}

module HStream.ShardSpec where

import           Control.Monad         (when)
import           Data.Either           (isLeft, isRight, rights)
import           Data.Foldable         (foldl')
import qualified Data.Map.Strict       as M
import           Data.Maybe            (fromJust)
import           Data.Word             (Word64)
import qualified HStream.Logger        as Log
import           HStream.Server.Shard  (Shard (..), ShardKey, ShardMap,
                                        deleteShard, getShard, getShardMapIdx,
                                        insertShard, mergeShard, mkShard,
                                        mkShardMap, splitShardByKey)
import qualified HStream.Store         as S
import           Test.Hspec
import           Test.Hspec.QuickCheck
import           Test.QuickCheck
import           Test.QuickCheck.Gen   (chooseUpTo, chooseWord64)
import qualified Z.Data.CBytes         as CB

genChar :: Gen Char
genChar = elements ['a'..'z']

genOrderPair :: Gen (Word64, Word64)
genOrderPair = do
  first <- chooseUpTo (maxBound `div` 5000)
  second <- chooseWord64 (first, maxBound)
  return (first, second)

instance Arbitrary S.StreamId where
  arbitrary = do
    streamName <- CB.pack <$> vectorOf 25 genChar
    return $ S.mkStreamId S.StreamTypeStream streamName

instance Arbitrary Shard where
  arbitrary = do
    (first, second) <- genOrderPair
    logId           <- arbitrary
    streamId        <- arbitrary
    let startKey = fromIntegral first
        endKey   = fromIntegral second
    mkShard logId streamId startKey endKey <$> arbitrary

genScopedShardKey :: ShardKey -> ShardKey -> IO ShardKey
genScopedShardKey startKey endKey = generate $ frequency
  [ (1, fromIntegral <$> chooseWord64(0, fromIntegral startKey))
  , (5, fromIntegral <$> chooseWord64(fromIntegral startKey, fromIntegral endKey))
  , (1, fromIntegral <$> chooseWord64(fromIntegral endKey, maxBound))
  ]

spec :: SpecWith ()
spec = describe "HStream.ShardSpec" $ do
    shardMapIdxSpec
    shardSpec

shardMapIdxSpec :: SpecWith ()
shardMapIdxSpec = describe "test get shardMap index" $ do
  prop "calculate shardMap index" $ do
    \x -> getShardMapIdx x `shouldSatisfy` (\n -> n >= 0 && n < 16)

shardSpec :: SpecWith ()
shardSpec = describe "test manipulate shard" $ do
  prop "Split shard" $ \shard@Shard{..} -> do
    key <- genScopedShardKey startKey endKey
    if startKey <= key && endKey >= key
      then do
        let res = splitShardByKey shard key
        isRight res `shouldBe` True
        let (s1, s2) = head . rights $ [res]
        (id1, id2) <- generate genOrderPair
        let s1'@Shard{startKey=startKey1} = s1 {logId = id1}
        let s2'@Shard{startKey=startKey2, endKey=endKey2} = s2 {logId = id2}
        let mergeRes = mergeShard s1' s2'
        when (isLeft mergeRes) $
          Log.fatal $ "merge shard failed:"
                   <> " s1= " <> Log.buildString' (show s1')
                   <> " s2= " <> Log.buildString' (show s2')
                   <> " mergeRes = " <> Log.buildString' (show mergeRes)
        isRight mergeRes `shouldBe` True
        let (Shard{startKey=startKey3, endKey=endKey3}, removedKey) = head . rights $ [mergeRes]
        removedKey `shouldBe` startKey2
        startKey3 `shouldBe` startKey1
        endKey3 `shouldBe` endKey2
      else do
        isLeft (splitShardByKey shard key) `shouldBe` True

  prop "Mixed manipulate shard" $ \shard@Shard{startKey=osKey, endKey=oeKey} -> do
      let shardMp = mkShardMap [(osKey, shard)]
      newMap <- loop 50 shardMp
      let shards = map snd $ M.toAscList newMap
      let Shard{startKey=fsKey, endKey=feKey} = mergeAllShards shards
      fsKey `shouldBe` osKey
      feKey `shouldBe` oeKey
   where
     randomShard shardMp = do
       key <- generate (elements . M.keys $ shardMp)
       case getShard shardMp key of
         Just shard -> return shard
         Nothing    -> do
           Log.fatal $ "randomShard error, key = " <> Log.buildString' (show key) <> ", mp = " <> Log.buildString' (show shardMp)
           error "get randomShard error"

     randomSplitKey startKey endKey = generate $ fromIntegral <$> chooseWord64 (fromIntegral startKey, fromIntegral endKey)

     mergeAllShards (first : shards) = foldl' (\acc shard -> fst . head . rights $ [mergeShard acc shard]) first shards
     mergeAllShards [] = error "shards should not empty"

     loop :: Int -> ShardMap -> IO ShardMap
     loop cnt shardMp
       | cnt <= 0 = return shardMp
       | otherwise = do
          split <- generate $ elements [True, False]
          if M.size shardMp == 1 || split
            then do
              shard'@Shard{..} <- randomShard shardMp
              splitKey <- randomSplitKey startKey endKey
              let res = splitShardByKey shard' splitKey
              isRight res `shouldBe` True
              let (s1, s2) = head . rights $ [res]
              (id1, id2) <- generate genOrderPair
              let s1'@Shard{startKey=sk1} = s1 {logId = id1}
              let s2'@Shard{startKey=sk2} = s2 {logId = id2}
              let mp1 = insertShard sk1 s1' shardMp
                  mp2 = insertShard sk2 s2' mp1
              loop (cnt - 1) mp2
            else do
              shard1@Shard{startKey=sk1, endKey=ek1, logId=id1} <- randomShard shardMp
              let shard2@Shard{startKey=sk2, logId=id2} = fromJust $ getShard shardMp (ek1 + 1)
              if sk1 == sk2
                 then loop cnt shardMp
                 else do
                   let res = mergeShard shard1 shard2
                   when (isLeft res) $ do
                     Log.fatal $ "merge shard failed:"
                              <> " s1= " <> Log.buildString' (show shard1)
                              <> " s2= " <> Log.buildString' (show shard2)
                              <> " mergeRes = " <> Log.buildString' (show res)
                     Log.fatal $ "shardMp = " <> Log.buildString' (show shardMp)
                   isRight res `shouldBe` True
                   let (shard'@Shard{startKey=sk}, removedKey) = head . rights $ [res]
                   let mp1 = deleteShard removedKey shardMp
                       mp2 = insertShard sk shard'{logId = id1 + id2} mp1
                   loop (cnt - 1) mp2
