{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}

module HStream.ConsistentHashingSpec where

import qualified Data.Map.Strict                  as M
import qualified Data.Map.Strict                  as Map
import qualified Data.Text                        as T
import           Data.Text.Encoding               (encodeUtf8)
import           Test.Hspec                       (SpecWith, describe, shouldBe,
                                                   xdescribe)
import           Test.Hspec.QuickCheck            (prop)
import           Test.QuickCheck                  (Arbitrary (..), Gen,
                                                   Property, Testable,
                                                   arbitrarySizedNatural,
                                                   choose, elements, forAll,
                                                   label, listOf, listOf1,
                                                   scale, shuffle, sublistOf,
                                                   suchThat, (===), (==>))


import           HStream.Common.ConsistentHashing (ServerMap,
                                                   constructServerMap,
                                                   getAllocatedNode)
import qualified HStream.Common.ConsistentHashing as CH
import           HStream.Server.HStreamInternal   (ServerNode (..))

type ServerId = T.Text

instance Arbitrary ServerNode where
  arbitrary = do
    serverNodeId <- T.pack <$> (listOf . elements) (['a'..'z'] <> ['A'..'Z'] <> ['1'..'9'])
    serverNodePort <- arbitrarySizedNatural
    serverNodeGossipPort <- arbitrarySizedNatural
    serverNodeAdvertisedAddress <- genAddr
    serverNodeGossipAddress <- genAddr
    let serverNodeAdvertisedListeners = Map.empty
    return ServerNode {..}
    where
      aByteGen = show <$> (choose (0, 255) :: Gen Int)
      genAddr = do
        x <- aByteGen
        y <- aByteGen
        z <- aByteGen
        w <- aByteGen
        return $ encodeUtf8 $ T.pack (x <> "." <> y <> "." <> z <> "." <> w)


newtype N = N ServerMap deriving (Show, Eq)

instance Arbitrary N where
  arbitrary = do
    serverNodes <- listOf1 (arbitrary :: Gen ServerNode)
    return $ N (constructServerMap serverNodes)

newtype A = A {unA :: T.Text} deriving (Show, Eq)

instance Arbitrary A where
  arbitrary = do
    name <- elements (['a'..'z'] <> ['A'..'Z'])
    append <- shuffle "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890-_" >>= sublistOf
    return $ A $ T.pack (name : append)

spec :: SpecWith ()
spec = describe "HStream.ConsistentHashingSpec" $ do
  insertSpec
  constructSpec
  deleteSpec
  getNodeSpec
  distributeSpec
  reallocationSpec

insertSpec :: SpecWith ()
insertSpec = describe "insert" $ do
  prop "Insert a node in empty HashRing" $ do
    \x -> CH.insert x M.empty `shouldBe` CH.insert x M.empty

  prop "Insert a node twice in HashRing" $ do
    \x -> CH.insert x (CH.insert x M.empty) `shouldBe` CH.insert x M.empty

constructSpec :: SpecWith ()
constructSpec = describe "construct" $ do
  prop "Same list of nodes should construct same HashRing" $ do
    \x -> constructServerMap x `shouldBe` constructServerMap x

deleteSpec :: SpecWith ()
deleteSpec = describe "delete" $ do
  prop "Delete a node in HashRing" $ do
    \(x, N y) -> CH.delete x y `shouldBe` CH.delete x y

  prop "Insert then delete a node in HashRing" $ do
    \(x, N y) -> CH.delete x (CH.insert x (CH.delete x y)) `shouldBe` CH.delete x y

getNodeSpec :: SpecWith ()
getNodeSpec = describe "lookup" $ do
  prop "use the same key should get the same node from the same hash ring" $ do
    \(N x, A y) -> getAllocatedNode x y `shouldBe` getAllocatedNode x y

distributeSpec :: SpecWith ()
distributeSpec = describe "distribute" $ do
  -- prop "Task should not all be allocated to the same node" $ do
  --   let examples = arbitrary `suchThat`
  --         (\(xs, N hr) -> length xs > CH.size hr * CH.size hr && CH.size hr > 2)
  --   let getUniqueServersNum hr = length . L.nub . map (getAllocatedNode hr)
  --   {-verbose $ -}
  --   forAllShow examples
  --     (\(xs, N hr) -> show (length xs, getUniqueServersNum hr (unA <$> xs), CH.size hr) )
  --     (\(xs, N hr) -> verbose $ getUniqueServersNum hr (unA <$> xs) == CH.size hr)

  prop "Task allocation should mostly be even" $ do
    let examples = do
          (xs, N hr) <- scale (*10) arbitrary `suchThat`
            (\(xs, N hr) -> length xs > 30 * CH.size hr && CH.size hr > 3)
          let frequency = getFrequency hr $ unA <$> xs
          return (frequency, getStdDev frequency)
    forAll (examples :: Gen (M.Map ServerId Int, Double)) (\(_, stdDev) -> labelStdDev stdDev True)

reallocationSpec :: SpecWith ()
reallocationSpec = xdescribe "reallocation" $ do
  prop "Number of reallocation of key should be less than `length xs / size node`" $ do
    \(node, xs, N hr) ->
      let oldFrqElem = M.elems $ getFrequency hr (unA <$> xs) in
      let newHr = CH.insert node hr in
      let newFrq = getFrequency newHr (unA <$> xs) in
      let nid = serverNodeId node in
      let reallocation = (sum oldFrqElem) - (sum . M.elems . M.delete nid $ newFrq) in
      let maxAllowed = length xs `div` M.size hr + 1 in
        newHr /= hr && maxAllowed > 1 ==>
          label (if maxAllowed >= reallocation then "Normal" else "Too Many") (reallocation <=  length xs `div` M.size hr + 1)

  prop "Number of reallocation of key when delete a new node should be the same as the deleted node" $ do
    \(node, xs, N hr) ->
      let oldFrq = getFrequency hr $ unA <$> xs in
      let newHr = CH.delete node hr in
      let newFrqElem = M.elems $ getFrequency newHr $ unA <$> xs in
      let nid = serverNodeId node in
      let Just oldNodeKeyNum = M.lookup nid oldFrq in
        newHr /= hr && newHr /= M.empty ==>
        (oldNodeKeyNum, length xs) ===
          (abs . sum $ zipWith (-) newFrqElem (M.elems (M.delete nid oldFrq)),
          sum newFrqElem)

-- -------------------------------------------------------------------------- --

getStdDev :: M.Map ServerId Int -> Double
getStdDev m =
  let values = map fromIntegral $ M.elems m in
  let sizeM = fromIntegral (M.size m) in
  let avg = sum values / sizeM
   in (/avg) . sqrt . (/ sizeM) . sum $ map (\x -> (x - avg) * (x - avg)) values

labelStdDev :: Testable prop => Double -> prop -> Property
labelStdDev x
  | x <= 0.2  = label "Less Than 0.2"
  | otherwise = label "Greater than 0.2"

getFrequency :: ServerMap -> [T.Text] -> M.Map ServerId Int
getFrequency nodes = let base = M.fromList $ zip (M.keys nodes) (repeat 0)
  in foldr ((\x y -> M.insertWith (+) x 1 y) . serverNodeId . getAllocatedNode nodes) base
