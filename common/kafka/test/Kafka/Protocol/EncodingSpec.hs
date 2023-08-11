module Kafka.Protocol.EncodingSpec (spec) where

import           Data.ByteString            (ByteString)
import           Data.Int
import           Data.Text                  (Text)
import           Data.Word
import           Test.Hspec
import           Test.Hspec.QuickCheck
import           Test.QuickCheck
import           Test.QuickCheck.Instances  ()
import           Test.QuickCheck.Special

import           Kafka.Protocol.Encoding
import           Kafka.QuickCheck.Instances ()

spec :: Spec
spec = do
  baseSpec

-- We need quickcheck-special to generate edge cases
-- See: https://github.com/nick8325/quickcheck/issues/98

baseSpec :: Spec
baseSpec = describe "Kafka.Protocol.Encoding" $ do
  prop "Bool" $ \(x :: Bool) -> encodingProp x
  prop "Int8" $ \(Special @Int8 x) -> encodingProp x
  prop "Int16" $ \(Special @Int16 x) -> encodingProp x
  prop "Int32" $ \(Special @Int32 x) -> encodingProp x
  prop "Int64" $ \(Special @Int64 x) -> encodingProp x
  prop "Word32" $ \(Special @Word32 x) -> encodingProp x
  prop "Double" $ \(Special @Double x) ->
    if isNaN x then ioProperty $ isNaN @Double <$> runGet (runPut x)
               else encodingProp x
  prop "NullableString" $ \(Special @NullableString x) -> encodingProp x
  prop "NullableBytes" $ \(Special @NullableBytes x) -> encodingProp x
  prop "Text" $ \(Special @Text x) -> encodingProp x
  prop "ByteString" $ \(Special @ByteString x) -> encodingProp x
  prop "VarInt32" $ \(Special @VarInt32 x) -> encodingProp x
  prop "VarInt64" $ \(Special @VarInt64 x) -> encodingProp x
  prop "CompactString" $ \(Special @CompactString x) -> encodingProp x
  prop "CompactNullableString" $ \(Special @CompactNullableString x) -> encodingProp x
  prop "CompactBytes" $ \(Special @CompactBytes x) -> encodingProp x
  prop "CompactNullableBytes" $ \(Special @CompactNullableBytes x) -> encodingProp x

  prop "KaArray Bool" $ \(x :: KaArray Bool) -> encodingProp x
  prop "KaArray Int8" $ \(x :: KaArray Int8) -> encodingProp x
  prop "KaArray NullableBytes" $ \(x :: KaArray NullableBytes) -> encodingProp x
  prop "KaArray NullableString" $ \(x :: KaArray NullableString) -> encodingProp x
  prop "KaArray VarInt32" $ \(x :: KaArray VarInt32) -> encodingProp x
  prop "KaArray CompactBytes" $ \(x :: KaArray CompactBytes) -> encodingProp x
  prop "KaArray KaArray CompactBytes" $ \(x :: KaArray (KaArray CompactBytes)) -> encodingProp x

encodingProp :: (Eq a, Show a, Serializable a) => a -> Property
encodingProp x = ioProperty $ (x ===) <$> runGet (runPut x)
