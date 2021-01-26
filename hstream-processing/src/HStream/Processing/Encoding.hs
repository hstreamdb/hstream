{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE NoImplicitPrelude         #-}
{-# LANGUAGE StrictData                #-}

module HStream.Processing.Encoding
  ( voidSerializer,
    voidDeserializer,
    Deserializer (..),
    Serializer (..),
    Serde (..),
  )
where

import           RIO
import qualified RIO.ByteString.Lazy as BL

newtype Deserializer a = Deserializer {runDeser :: BL.ByteString -> a}

newtype Serializer a = Serializer {runSer :: a -> BL.ByteString}

data Serde a
  = Serde
      { serializer :: Serializer a,
        deserializer :: Deserializer a
      }

voidDeserializer :: Maybe (Deserializer Void)
voidDeserializer = Nothing

voidSerializer :: Maybe (Serializer Void)
voidSerializer = Nothing
