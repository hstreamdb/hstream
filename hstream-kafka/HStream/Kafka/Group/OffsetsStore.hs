module HStream.Kafka.Group.OffsetsStore
  ( OffsetStorage(..)
  , mkCkpOffsetStorage
  , deleteCkpOffsetStorage
  ) where

import           Control.Monad      (unless)
import           Data.Map.Strict    (Map)
import qualified Data.Map.Strict    as Map
import qualified Data.Text          as T
import           Data.Word          (Word64)

import           HStream.Base.Timer (CompactedWorker, startCompactedWorker,
                                     stopCompactedWorker,
                                     triggerCompactedWorker)
import qualified HStream.Logger     as Log
import qualified HStream.Store      as S
import           HStream.Utils      (textToCBytes)

type LogID = Word64
type LSN = Word64

class OffsetStorage s where
  commitOffsets :: s -> T.Text -> Map LogID LSN -> IO ()
  loadOffsets :: s -> T.Text -> IO (Map LogID LSN)

--------------------------------------------------------------------------------

data CkpOffsetStorage = CkpOffsetStorage
  { ckpStore      :: !S.LDCheckpointStore
  , ckpStoreName  :: !T.Text
  , ckpStoreId    :: !Word64
    -- ^ __consumer_offsets logID
  , trimCkpWorker :: !CompactedWorker
  }

mkCkpOffsetStorage :: S.LDClient -> T.Text -> Int -> IO CkpOffsetStorage
mkCkpOffsetStorage client ckpStoreName replica = do
  let cbGroupName = textToCBytes ckpStoreName
      logAttrs = S.def{S.logReplicationFactor = S.defAttr1 replica}
  -- FIXME: need to get log attributes from somewhere
  S.initOffsetCheckpointDir client logAttrs
  ckpStoreId <- S.allocOffsetCheckpointId client cbGroupName
  ckpStore <- S.newRSMBasedCheckpointStore client ckpStoreId 5000
  Log.info $ "mkCkpOffsetStorage with name: " <> Log.build ckpStoreName
          <> ", storeId: " <> Log.build ckpStoreId
  trimCkpWorker <- startCompactedWorker (60 * 1000000){- 60s -} $ do
    Log.debug $ "Compacting checkpoint store of " <> Log.build ckpStoreName
    S.trimLastBefore 1 client ckpStoreId
  return CkpOffsetStorage{..}

-- FIXME: there may other resources(in memory or...) need to be released
deleteCkpOffsetStorage :: S.LDClient -> CkpOffsetStorage -> IO ()
deleteCkpOffsetStorage ldclient CkpOffsetStorage{..} = do
  stopCompactedWorker trimCkpWorker
  S.freeOffsetCheckpointId ldclient (textToCBytes ckpStoreName)

instance OffsetStorage CkpOffsetStorage where
  commitOffsets CkpOffsetStorage{..} offsetsKey offsets = do
    unless (Map.null offsets) $ do
      S.ckpStoreUpdateMultiLSN ckpStore (textToCBytes offsetsKey) offsets
      triggerCompactedWorker trimCkpWorker

  loadOffsets CkpOffsetStorage{..} offsetKey =
    Map.fromList <$> S.ckpStoreGetAllCheckpoints' ckpStore (textToCBytes offsetKey)
