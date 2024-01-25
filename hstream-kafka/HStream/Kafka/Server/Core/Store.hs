module HStream.Kafka.Server.Core.Store where

import           Control.Exception (Exception, throw)
import           Control.Monad     (foldM, forM)
import           Data.Int          (Int32)
import qualified Data.Map.Strict   as M
import           GHC.Stack         (HasCallStack)
import qualified HStream.Store     as S
import           Text.Read         (readMaybe)
import qualified Z.Data.CBytes     as CB

createTopicPartitions :: S.LDClient -> S.StreamId -> Int32 -> IO [S.C_LogID]
createTopicPartitions client streamId partitions = do
  totalCnt <- getTotalPartitionCount client streamId
  forM [0..partitions-1] $ \i -> do
    S.createStreamPartition client streamId (Just (CB.pack . show $ totalCnt + i)) M.empty

-- Get the total number of partitions of a topic
getTotalPartitionCount :: S.LDClient -> S.StreamId -> IO Int32
getTotalPartitionCount client streamId = do
  fromIntegral . M.size <$> S.listStreamPartitions client streamId

newtype ParsePartitionIdError = ParsePartitionIdError String deriving Show
instance Exception ParsePartitionIdError

type PartitionId = Int32

-- FIXME: find a better way to handle parse exception
listTopicPartitions :: HasCallStack => S.LDClient -> S.StreamId -> IO (M.Map PartitionId S.C_LogID)
listTopicPartitions client streamId = do
  partitions <- S.listStreamPartitions client streamId
  foldM mapKey M.empty (M.toList partitions)
 where
  mapKey acc (k, v) = return $ M.insert (parsePartitionId k) v acc
  parsePartitionId key = case readMaybe (CB.unpack key) of
    Just i  -> i
    Nothing -> throw $ ParsePartitionIdError $ "Invalid partition id: " <> show key
