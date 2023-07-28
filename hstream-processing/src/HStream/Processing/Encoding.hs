{-# LANGUAGE CPP                       #-}
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
    Serialized (..),
  )
where

import qualified Data.Aeson          as Aeson
import qualified Data.HashMap.Strict as HM
import           Data.Maybe          (fromJust)
import           RIO
import qualified RIO.ByteString.Lazy as BL
#if MIN_VERSION_aeson(2,0,0)
import qualified Data.Aeson.Key      as Key
import qualified Data.Aeson.KeyMap   as KeyMap
#endif

newtype Deserializer a s = Deserializer {runDeser :: s -> a}

newtype Serializer a s = Serializer {runSer :: a -> s}

data Serde a s = Serde
  { serializer   :: Serializer a s,
    deserializer :: Deserializer a s
  }

class (Ord s, Typeable s) => Serialized s where
  compose :: (s, s) -> s
  separate :: s -> (s, s)

instance Serialized BL.ByteString where
  compose (winBytes, keyBytes) = winBytes <> keyBytes
  separate = BL.splitAt 16 -- 2 * Int64

#if MIN_VERSION_aeson(2,0,0)
instance Serialized Aeson.Object where
  compose (winObj, keyObj) = winObj `KeyMap.union` keyObj
  separate obj =
    let startValue = fromJust $ (KeyMap.lookup) "window_start" obj
        endValue   = fromJust $ (KeyMap.lookup) "window_end"   obj
        winObj     = KeyMap.fromList [ (Key.fromText "window_start", startValue)
                                     , (Key.fromText "window_end", endValue)
                                     ]
        keyObj = KeyMap.difference obj winObj
     in (winObj, keyObj)
#else
instance Serialized Aeson.Object where
  compose (winObj, keyObj) = winObj `HM.union` keyObj
  separate obj =
    let startValue = (HM.!) obj "window_start"
        endValue   = (HM.!) obj "window_end"
        winObj = HM.fromList [("window_start", startValue), ("window_end", endValue)]
        keyObj = HM.difference obj winObj
     in (winObj, keyObj)
#endif

voidDeserializer :: Maybe (Deserializer Void BL.ByteString)
voidDeserializer = Nothing

voidSerializer :: Maybe (Serializer Void BL.ByteString)
voidSerializer = Nothing
