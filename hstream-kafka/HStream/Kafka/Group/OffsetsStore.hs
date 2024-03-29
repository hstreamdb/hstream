module HStream.Kafka.Group.OffsetsStore
  ( OffsetStorage(..)
  , mkCkpOffsetStorage
  )
where

import           Control.Monad   (unless)
import           Data.Bifunctor  (bimap)
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Text       as T
import           Data.Word       (Word64)
import qualified HStream.Logger  as Log
import qualified HStream.Store   as S
import           HStream.Utils   (textToCBytes)

type LogID = Word64
type LSN = Word64

class OffsetStorage s where
  commitOffsets :: s -> T.Text -> Map LogID LSN -> IO ()
  loadOffsets :: s -> T.Text -> IO (Map LogID LSN)

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
      logAttrs = S.def{S.logReplicationFactor = S.defAttr1 1}
  -- FIXME: need to get log attributes from somewhere
  S.initOffsetCheckpointDir client logAttrs
  ckpStoreId <- S.allocOffsetCheckpointId client cbGroupName
  ckpStore <- S.newRSMBasedCheckpointStore client ckpStoreId 5000
  Log.info $ "mkCkpOffsetStorage with name: " <> Log.build ckpStoreName <> ", storeId: " <> Log.build ckpStoreId
  return CkpOffsetStorage{..}

instance OffsetStorage CkpOffsetStorage where
  commitOffsets CkpOffsetStorage{..} offsetsKey offsets = do
    unless (Map.null offsets) $ do
      S.ckpStoreUpdateMultiLSN ckpStore (textToCBytes offsetsKey) offsets
  loadOffsets CkpOffsetStorage{..} offsetKey = do
    checkpoints <- S.ckpStoreGetAllCheckpoints' ckpStore (textToCBytes offsetKey)
    return . Map.fromList $ map (bimap fromIntegral fromIntegral) checkpoints
