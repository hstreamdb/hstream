{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE OverloadedStrings #-}
module HStream.ConsistentHashingSpec where

import qualified Data.HashMap.Strict              as HM
import qualified Data.List                        as L
import qualified Data.Map.Strict                  as M
import qualified Data.Text                        as T
import           Data.Word                        (Word32)
import           Test.Hspec                       (SpecWith, describe, shouldBe)
import           Test.Hspec.QuickCheck            (prop)
import           Test.QuickCheck                  (Arbitrary (..), Gen,
                                                   Property, Testable,
                                                   arbitrarySizedNatural,
                                                   chooseInt, elements, forAll,
                                                   forAllShow, label, listOf1,
                                                   scale, shuffle, sublistOf,
                                                   suchThat, vectorOf, (===),
                                                   (==>))

import           HStream.Common.ConsistentHashing (HashRing, constructHashRing,
                                                   getAllocatedNode, lookupKey)
import qualified HStream.Common.ConsistentHashing as CH
import           HStream.Server.HStreamApi        (ServerNode (..))

instance Arbitrary ServerNode where
  arbitrary = do
    serverNodeId <- arbitrarySizedNatural
    serverNodePort <- arbitrarySizedNatural
    xs <- fmap (map (T.pack . show)) . vectorOf 4 $ chooseInt (0,255)
    let [a,b,c,d] = xs
    let serverNodeHost = a <> "." <> b <> "." <> c <> "." <> d
    return ServerNode {..}

instance Arbitrary HashRing where
  arbitrary = do
    serverNodes <- listOf1 (arbitrary :: Gen ServerNode)
    return (constructHashRing serverNodes)

instance Arbitrary CH.Key where
  arbitrary = do
    name <- elements "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    append <- shuffle "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890-_" >>= sublistOf
    return (CH.Key $ T.pack (name : append))

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
    \x -> CH.insert x CH.empty `shouldBe` CH.insert x CH.empty

  prop "Insert a node twice in HashRing" $ do
    \x -> CH.insert x (CH.insert x CH.empty) `shouldBe` CH.insert x CH.empty

constructSpec :: SpecWith ()
constructSpec = describe "construct" $ do
  prop "Same list of nodes should construct same HashRing" $ do
    \x -> constructHashRing x `shouldBe` constructHashRing x

deleteSpec :: SpecWith ()
deleteSpec = describe "delete" $ do
  prop "Delete a node in HashRing" $ do
    \(x, y) -> CH.delete x y `shouldBe` CH.delete x y

  prop "Insert then delete a node in HashRing" $ do
    \(x, y) -> CH.delete x (CH.insert x (CH.delete x y)) `shouldBe` CH.delete x y

getNodeSpec :: SpecWith ()
getNodeSpec = describe "lookup" $ do
  prop "use the same key should get the same node from the same hash ring" $ do
    \(x, CH.Key y) -> getAllocatedNode x y `shouldBe` getAllocatedNode x y

distributeSpec :: SpecWith ()
distributeSpec = describe "distribute" $ do
  prop "Task should not all be allocated to the same node" $ do
    let examples = arbitrary `suchThat`
          (\(xs, hr) -> length xs > 100 && CH.size hr > 1)
    let getUniqueServersNum hr = length . L.nub . map (`lookupKey` hr)
    forAllShow examples
      (\(xs, hr) -> show (getUniqueServersNum hr xs, CH.size hr) )
      (\(xs, hr) -> getUniqueServersNum hr xs > 1)

  prop "Task allocation should mostly be even" $ do
    let examples = do
          (xs, hr) <- scale (*10) arbitrary `suchThat`
            (\(xs, hr) -> length xs > 30 * CH.size hr && CH.size hr > 3)
          let frequency = getFrequency hr xs
          return (frequency, getStdDev frequency)
    forAll (examples :: Gen (M.Map Word32 Int, Double)) (\(_, stdDev) -> labelStdDev stdDev True)

reallocationSpec :: SpecWith ()
reallocationSpec = describe "reallocation" $ do
  prop "Number of reallocation of key when add a new node should be the same as the new node" $ do
    \(node, xs, hr) ->
      let oldFrqElem = M.elems $ getFrequency hr xs in
      let newHr = CH.insert node hr in
      let newFrq = getFrequency newHr xs in
      let nid = serverNodeId node in
      let Just newNodeKeyNum = M.lookup nid newFrq in
        newHr /= hr ==>
        (newNodeKeyNum, length xs) ===
          (abs . sum $ zipWith (-) oldFrqElem (M.elems (M.delete nid newFrq)),
          sum oldFrqElem)

  prop "Number of reallocation of key when delete a new node should be the same as the deleted node" $ do
    \(node, xs, hr) ->
      let oldFrq = getFrequency hr xs in
      let newHr = CH.delete node hr in
      let newFrqElem = M.elems $ getFrequency newHr xs in
      let nid = serverNodeId node in
      let Just oldNodeKeyNum = M.lookup nid oldFrq in
        newHr /= hr && newHr /= CH.empty ==>
        (oldNodeKeyNum, length xs) ===
          (abs . sum $ zipWith (-) newFrqElem (M.elems (M.delete nid oldFrq)),
          sum newFrqElem)

-- -------------------------------------------------------------------------- --

getStdDev :: M.Map Word32 Int -> Double
getStdDev m =
  let values = map fromIntegral $ M.elems m in
  let sizeM = fromIntegral (M.size m) in
  let avg = sum values / sizeM
   in sqrt . (/ sizeM) . sum $ map (\x -> (x - avg) * (x - avg)) values

labelStdDev :: Testable prop => Double -> prop -> Property
labelStdDev x
  | x <= 10   = label "Std Dev Less Than 10"
  | x <= 20   = label "Std Dev Less Than 20"
  | x <= 30   = label "Std Dev Less Than 30"
  | otherwise = label "Std Dev Greater than 40"

getFrequency :: HashRing -> [CH.Key] -> M.Map Word32 Int
getFrequency hr = let base = M.fromList $ zip (HM.keys (CH.vnodes hr)) (repeat 0)
  in foldr ((\x y -> M.insertWith (+) x 1 y) . serverNodeId . (`lookupKey` hr)) base
