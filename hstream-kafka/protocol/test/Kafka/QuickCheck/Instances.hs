{-# OPTIONS_GHC -Wno-orphans #-}

module Kafka.QuickCheck.Instances where

import           Kafka.Protocol.Encoding
import           Test.QuickCheck
import           Test.QuickCheck.Instances ()
import           Test.QuickCheck.Special

deriving instance Arbitrary VarInt32
deriving instance Arbitrary VarInt64
deriving instance Arbitrary CompactString
deriving instance Arbitrary CompactNullableString
deriving instance Arbitrary CompactBytes
deriving instance Arbitrary CompactNullableBytes
deriving instance Arbitrary a => Arbitrary (KaArray a)
deriving instance Arbitrary a => Arbitrary (CompactKaArray a)

deriving instance SpecialValues VarInt32
deriving instance SpecialValues VarInt64
deriving instance SpecialValues CompactString
deriving instance SpecialValues CompactNullableString
deriving instance SpecialValues CompactBytes
deriving instance SpecialValues CompactNullableBytes
