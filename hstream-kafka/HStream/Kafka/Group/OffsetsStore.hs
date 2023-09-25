module HStream.Kafka.Group.OffsetsStore
  ( OffsetStorage(..)
  , OffsetStore(..)
  , mkCkpOffsetStorage
  )
where

import           Control.Monad   (unless)
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Text       as T
import           Data.Word       (Word64)
import qualified HStream.Logger  as Log
import qualified HStream.Store   as S
import           HStream.Utils   (textToCBytes)

class OffsetStorage s where
  commitOffsets :: s -> T.Text -> Map Word64 Word64 -> IO ()

data OffsetStore = Ckp CkpOffsetStorage

instance OffsetStorage OffsetStore where
  commitOffsets (Ckp s) = commitOffsets s

--------------------------------------------------------------------------------

data CkpOffsetStorage = CkpOffsetStorage
  { ckpStore     :: S.LDCheckpointStore
  , ckpStoreName :: T.Text
  , ckpStoreId   :: Word64
    -- ^ __consumer_offsets logID
  }

mkCkpOffsetStorage :: S.LDClient -> T.Text -> IO CkpOffsetStorage
mkCkpOffsetStorage client ckpStoreName = do
  let cbGroupName = textToCBytes ckpStoreName
  -- FIXME: need to get log attributes from somewhere
  S.initOffsetCheckpointDir client S.def
  ckpStoreId <- S.allocOffsetCheckpointId client cbGroupName
  ckpStore <- S.newRSMBasedCheckpointStore client ckpStoreId 5000
  Log.info $ "mkCkpOffsetStorage with name: " <> Log.build ckpStoreName <> ", storeId: " <> Log.build ckpStoreId
  return CkpOffsetStorage{..}

instance OffsetStorage CkpOffsetStorage where
  commitOffsets CkpOffsetStorage{..} offsetsKey offsets = do
    unless (Map.null offsets) $ do
      S.ckpStoreUpdateMultiLSN ckpStore (textToCBytes offsetsKey) offsets
