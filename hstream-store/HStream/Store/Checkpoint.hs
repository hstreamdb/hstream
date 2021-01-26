module HStream.Store.Checkpoint
  ( CheckpointStore
  , newFileBasedCheckpointStore
  , getSequenceNumSync
  , updateSequenceNumSync
  , updateMultiSequenceNumSync
  ) where

import           Control.Monad           (void)
import           Data.Map.Strict         (Map)
import qualified Data.Map.Strict         as Map
import           Foreign.ForeignPtr      (newForeignPtr, withForeignPtr)
import           Z.Data.CBytes           (CBytes)
import qualified Z.Data.CBytes           as ZC
import qualified Z.Foreign               as Z

import           HStream.Internal.FFI    (CheckpointStore (..),
                                          SequenceNum (..), TopicID (..))
import qualified HStream.Internal.FFI    as FFI
import qualified HStream.Store.Exception as E

-------------------------------------------------------------------------------

-- | Create a file based checkpoint store.
--
-- Note: it's not safe to have multiple FileBasedVersionedConfigStore
-- objects created from the `root_path' accessing configs with the same
-- `key' concurrently. For the best practice, use only one
-- FileBasedVersionedConfigStore instance for one `root_path'.
newFileBasedCheckpointStore :: CBytes -> IO CheckpointStore
newFileBasedCheckpointStore root_path =
  ZC.withCBytesUnsafe root_path $ \path' -> do
    i <- FFI.c_new_file_based_checkpoint_store path'
    CheckpointStore <$> newForeignPtr FFI.c_free_checkpoint_store_fun i

getSequenceNumSync :: CheckpointStore -> CBytes -> TopicID -> IO SequenceNum
getSequenceNumSync (CheckpointStore store) customid (TopicID topicid) =
  ZC.withCBytes customid $ \customid' ->
  withForeignPtr store $ \store' -> do
    (ret_lsn, _) <- Z.withPrimSafe FFI.c_lsn_invalid $ \sn' ->
      E.throwStreamErrorIfNotOK $ FFI.c_checkpoint_store_get_lsn_sync_safe store' customid' topicid sn'
    return $ SequenceNum ret_lsn

updateSequenceNumSync :: CheckpointStore -> CBytes -> TopicID -> SequenceNum -> IO ()
updateSequenceNumSync (CheckpointStore store) customid (TopicID topicid) (SequenceNum sn) =
  ZC.withCBytes customid $ \customid' ->
  withForeignPtr store $ \store' -> do
    void $ E.throwStreamErrorIfNotOK $ FFI.c_checkpoint_store_update_lsn_sync_safe store' customid' topicid sn

updateMultiSequenceNumSync
  :: CheckpointStore
  -> CBytes
  -> Map TopicID SequenceNum
  -> IO ()
updateMultiSequenceNumSync (CheckpointStore store) customid sns =
  ZC.withCBytes customid $ \customid' ->
  withForeignPtr store $ \store' -> do
    let xs = Map.toList sns
    let ka = Z.primArrayFromList $ map (unTopicID . fst) xs
        va = Z.primArrayFromList $ map (unSequenceNum . snd) xs
    Z.withPrimArraySafe ka $ \ks' len ->
      Z.withPrimArraySafe va $ \vs' _len -> void $ E.throwStreamErrorIfNotOK $
        FFI.c_checkpoint_store_update_multi_lsn_sync_safe store' customid' ks' vs' (fromIntegral len)
