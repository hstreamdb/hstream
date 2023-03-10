{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}

module HStream.Processing.Processor.ChangeLog where

import Data.Aeson
import qualified RIO.ByteString.Lazy      as BL
import           HStream.Processing.Stream.TimeWindows
import           HStream.Processing.Type
import GHC.Generics
import Data.Text.Lazy.Encoding (decodeUtf8, encodeUtf8)

class ChangeLogger h where
  logChangelog :: h -> BL.ByteString -> IO ()

data StateStoreChangelog k v ser
  = CLKSPut k v -- HS.table: K/V; HG.aggregate: K/V; HTW.aggregate: K/V
  | CLSSPut (TimeWindowKey k) v -- HSW.aggregate: TK/V
  | CLSSRemove (TimeWindowKey k) -- HSW.aggregate: TK
  | CLTKSPut (TimestampedKey ser) ser -- HS.join: TSK BS/BS
  deriving (Generic)

deriving instance (FromJSON k, FromJSON v, FromJSON ser) => FromJSON (StateStoreChangelog k v ser)
deriving instance (ToJSON k, ToJSON v, ToJSON ser) => ToJSON (StateStoreChangelog k v ser)

instance FromJSON BL.ByteString where
  parseJSON v = let pText = parseJSON v in encodeUtf8 <$> pText

instance ToJSON BL.ByteString where
  toJSON cb = toJSON (decodeUtf8 cb)
