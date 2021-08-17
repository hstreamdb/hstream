{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleInstances         #-}
{-# LANGUAGE NoImplicitPrelude         #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE StrictData                #-}
{-# LANGUAGE TypeSynonymInstances      #-}

module HStream.Processing.Encoding
  ( voidSerializer,
    voidDeserializer,
    Deserializer (..),
    Serializer (..),
    Serde (..),
    Serialized(..)
  )
where

import qualified Data.Aeson          as Aeson
import qualified Data.HashMap.Strict as HM
import           RIO
import qualified RIO.ByteString.Lazy as BL

newtype Deserializer a s = Deserializer {runDeser :: s -> a}

newtype Serializer a s = Serializer {runSer :: a -> s}

data Serde a s = Serde
  { serializer :: Serializer a s,
    deserializer :: Deserializer a s
  }

class (Ord s, Typeable s) => Serialized s where
  compose  :: (s, s) -> s
  separate :: s -> (s, s)

instance Serialized BL.ByteString where
  compose (winBytes, keyBytes) = winBytes <> keyBytes
  separate = BL.splitAt 16 -- 2 * Int64

instance Serialized Aeson.Object where
  compose (winObj, keyObj) = winObj `HM.union` keyObj
  separate obj =
    let startValue = (HM.!) obj "winStart"
        endValue   = (HM.!) obj "winEnd"
        winObj = HM.fromList [("winStart", startValue), ("winEnd", endValue)]
        keyObj = HM.difference obj winObj
     in (winObj, keyObj)

voidDeserializer :: Maybe (Deserializer Void BL.ByteString)
voidDeserializer = Nothing

voidSerializer :: Maybe (Serializer Void BL.ByteString)
voidSerializer = Nothing
