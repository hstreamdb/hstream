module HStream.Server.KafkaHandler.Offset
 ( handleOffsetCommitV0
 )
where

import           Control.Concurrent               (withMVar)
import qualified Data.HashMap.Strict              as HM
import qualified Data.Vector                      as V
import           HStream.Server.Types             (ServerContext (..))
import           Kafka.Group.GroupMetadataManager (storeOffsets)
import qualified Kafka.Protocol                   as K
import qualified Kafka.Protocol.Service           as K

--------------------
-- 8: OffsetCommit
--------------------
handleOffsetCommitV0
  :: ServerContext -> K.RequestContext -> K.OffsetCommitRequestV0 -> IO K.OffsetCommitResponseV0
handleOffsetCommitV0 ServerContext{..} _ K.OffsetCommitRequestV0{..} = do
  case K.unKaArray topics of
    Nothing      -> undefined
    Just topics' -> do
      mgr <- withMVar scGroupMetadataManagers $ return . HM.lookup groupId
      case mgr of
        Nothing       -> undefined
        Just groupMgr -> do
          response <- V.forM topics' $ \K.OffsetCommitRequestTopicV0{..} -> do
            res <- storeOffsets groupMgr name partitions
            return $ K.OffsetCommitResponseTopicV0 {partitions = res, name = name}
          return . K.OffsetCommitResponseV0 $ K.KaArray {unKaArray = Just response}
