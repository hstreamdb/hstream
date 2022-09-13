module HStream.ExceptionSpec (spec) where

import           Control.Exception
import           Test.Hspec

import qualified HStream.Exception as HE

spec :: Spec
spec = describe "ExceptionSpec" $ do
  it "NoRecordHeader should be a HStreamEncodingException" $
    throw HE.NoRecordHeader `shouldThrow` anyHStreamEncodingException
  it "NoRecordHeader should be a SomeHStreamException" $
    throw HE.NoRecordHeader `shouldThrow` anyHStreamException

anyHStreamException :: Selector HE.SomeHStreamException
anyHStreamException = const True

anyHStreamEncodingException :: Selector HE.HStreamEncodingException
anyHStreamEncodingException = const True
