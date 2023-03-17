{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE StandaloneDeriving #-}

module HStream.Processing.Processor.Snapshot where

import           Data.Aeson
import           Data.ByteString                       as BS
import           Data.Int                              (Int64)
import           Data.Map.Strict                       (Map)
import           Data.Text                             (Text)
import           GHC.Generics
import           HStream.Processing.Stream.TimeWindows
import           HStream.Processing.Type

data StateStoreSnapshotKey = StateStoreSnapshotKey
  { snapshotQueryId   :: Text
  , snapshotStoreName :: Text
  }
  deriving (Show, Generic, ToJSON)

data StateStoreSnapshotValue i k v ser
  = SnapshotKS  i (Map k v)
  | SnapshotSS  i (Map Timestamp (Map k (Map Timestamp v)))
  | SnapshotTKS i (Map Int64 (Map ser ser))
  deriving (Generic)

getSnapshotValueTail :: StateStoreSnapshotValue i k v ser -> i
getSnapshotValueTail (SnapshotKS  i _) = i
getSnapshotValueTail (SnapshotSS  i _) = i
getSnapshotValueTail (SnapshotTKS i _) = i

deriving instance (FromJSON i, FromJSON k, FromJSON v, FromJSON ser, Ord k, Ord ser, FromJSONKey k, FromJSONKey ser) =>
  FromJSON (StateStoreSnapshotValue i k v ser)
deriving instance (ToJSON i, ToJSON k, ToJSON v, ToJSON ser, ToJSONKey k, ToJSONKey ser) =>
  ToJSON (StateStoreSnapshotValue i k v ser)

class Snapshotter h where
  snapshot :: h -> BS.ByteString -> BS.ByteString -> IO ()
