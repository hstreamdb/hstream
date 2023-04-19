module HStream.ValidationSpec where

import           Data.Char                (isLetter)
import           Data.Either              (isLeft, isRight)
import qualified Data.List                as L
import           Data.Text                (Text)
import qualified Data.Text                as T
import           Test.Hspec
import           Test.Hspec.QuickCheck    (prop)
import           Test.QuickCheck

import           HStream.Utils.Validation (validateChar, validateNameText)

spec :: Spec
spec = describe "HStream.ValidationSpec" $ do
  prop "test Validation function" $ do
    forAll validText (\x -> validateNameText x === Right x)
  prop "test Validation function on invalid char" $ do
    forAll invalidHead (isLeft . validateNameText)
  prop "test Validation function on invalid head" $ do
    forAll invalidText (isLeft . validateNameText)
  prop "test Validation function on invalid length" $ do
    forAll invalidLength (isLeft . validateNameText)

alphabet :: Gen Char
alphabet = elements $ ['a'..'z'] ++ ['A'..'Z']

identChar :: Gen Char
identChar = elements $ ['a'..'z'] ++ ['A'..'Z'] ++ ['0'..'9'] ++ "-_"

validText :: Gen Text
validText = T.pack <$> someString `suchThat` ((<= 255) . length)

someString :: Gen String
someString = do
      x <- alphabet;
      xs <- listOf identChar
      return (x:xs)

invalidHead :: Gen Text
invalidHead = (T.pack .) . (:) <$> arbitrary `suchThat` (not . isLetter) <*> someString

invalidText :: Gen Text
invalidText = do
  x <- someString
  y <- arbitrary `suchThat` (not . isRight . validateChar)
  i <- choose (0, length x - 1)
  let (a,b) = L.splitAt i x
  return $ T.pack $ a ++ y : b

invalidLength :: Gen Text
invalidLength = T.pack <$> resize 255 someString `suchThat` ((>255) . length)
