module HStream.Kafka.Server.Core.Store where

import           Control.Exception  (Exception, throwIO)
import           Control.Monad      (foldM, forM)
import           Data.Int           (Int32)
import           Data.IntMap.Strict (IntMap)
import qualified Data.IntMap.Strict as IntMap
import qualified Data.Map.Strict    as M
import           GHC.Stack          (HasCallStack)
import qualified HStream.Logger     as Log
import qualified HStream.Store      as S
import           Text.Printf        (printf)
import qualified Z.Data.Builder     as CB
import qualified Z.Data.CBytes      as CB
import qualified Z.Data.Parser      as CB

createTopicPartitions :: HasCallStack => S.LDClient -> S.StreamId -> Int32 -> IO [S.C_LogID]
createTopicPartitions client streamId partitions = do
  totalCnt <- getTotalPartitionCount client streamId
  forM [0..partitions-1] $ \i -> do
    S.createStreamPartition client streamId (Just (CB.pack . encode $ totalCnt + i)) M.empty
 where
  encode :: Int32 -> String
  encode = printf "%016d"

-- Get the total number of partitions of a topic
getTotalPartitionCount :: HasCallStack => S.LDClient -> S.StreamId -> IO Int32
getTotalPartitionCount client streamId = do
  fromIntegral . M.size <$> S.listStreamPartitions client streamId

newtype ParsePartitionIdError = ParsePartitionIdError String deriving Show
instance Exception ParsePartitionIdError

-- FIXME: find a better way to handle parse exception
listTopicPartitions :: HasCallStack => S.LDClient -> S.StreamId -> IO (IntMap S.C_LogID)
listTopicPartitions client streamId = do
  partitions <- S.listStreamPartitions client streamId
  foldM mapKey IntMap.empty (M.toList partitions)
 where
  mapKey acc (k, v) = do
    partitionId <- parsePartitionId k
    return $ IntMap.insert partitionId v acc
  parsePartitionId key = case CB.parse' CB.uint $ CB.build $ CB.toBuilder key of
    Right i -> return i
    Left e  -> do
      Log.fatal $ "parse partitionId error: " <> Log.build (show e)
      throwIO $ ParsePartitionIdError $ "Invalid partition id: " <> show key

