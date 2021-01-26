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

-- import Control.Exception (throw)
-- import Data.Typeable
-- import HStream.Error
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
-- data EV = forall a. Typeable a => EV a
--
-- mkEV :: Typeable a => a -> EV
-- mkEV = EV
--
-- newtype ESerializer = ESerializer
--   { runESer :: EV -> BL.ByteString
--   }
--
-- mkESerializer :: Typeable a => Serializer a -> ESerializer
-- mkESerializer ser = ESerializer $ \ev ->
--   case cast ev of
--     Just v -> runSer ser v
--     Nothing -> throw $ TypeCastError "mkESerializer: type cast error"
--
-- newtype EDeserializer = EDeserializer {runEDeser :: BL.ByteString -> EV}
--
-- mkEDeserializer :: Typeable a => Deserializer a -> EDeserializer
-- mkEDeserializer deser = EDeserializer $ \s -> EV $ runDeser deser s
