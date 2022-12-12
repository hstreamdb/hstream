module HStream.ExceptionSpec (spec) where

import           Control.Exception
import           Test.Hspec

import qualified HStream.Exception as HE

spec :: Spec
spec = describe "ExceptionSpec" $ do
  it "NoRecordHeader should be a NoRecordHeader" $
    throw HE.NoRecordHeader `shouldThrow` noRecordHeaderEx
  it "NoRecordHeader should also be an InvalidArgument" $
    throw HE.NoRecordHeader `shouldThrow` anyInvalidArgument
  it "NoRecordHeader should also be an HServerException" $
    throw HE.NoRecordHeader `shouldThrow` anyHServerException
  it "NoRecordHeader should also be an HStreamException" $
    throw HE.NoRecordHeader `shouldThrow` anyHStreamException

noRecordHeaderEx :: Selector HE.NoRecordHeader
noRecordHeaderEx = const True

anyInvalidArgument :: Selector HE.SomeInvalidArgument
anyInvalidArgument = const True

anyHServerException :: Selector HE.SomeHServerException
anyHServerException = const True

anyHStreamException :: Selector HE.SomeHStreamException
anyHStreamException = const True
