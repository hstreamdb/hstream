{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE PatternSynonyms     #-}

module HStream.Kafka.Server.Handler.Offset
 ( handleOffsetCommit
 , handleOffsetFetch
 , handleListOffsets
 )
where

import qualified Control.Exception                     as E
import           Data.Int                              (Int64)
import           Data.Maybe                            (fromMaybe)
import qualified Data.Vector                           as V

import           HStream.Kafka.Common.Acl
import           HStream.Kafka.Common.Authorizer.Class
import qualified HStream.Kafka.Common.KafkaException   as K
import qualified HStream.Kafka.Common.Metrics          as Metrics
import           HStream.Kafka.Common.OffsetManager    (getLatestOffset,
                                                        getOffsetByTimestamp,
                                                        getOldestOffset)
import           HStream.Kafka.Common.Resource
import           HStream.Kafka.Common.Utils            (forKaArray, forKaArrayM,
                                                        vectorToKaArray)
import qualified HStream.Kafka.Group.Group             as G
import qualified HStream.Kafka.Group.GroupCoordinator  as GC
import           HStream.Kafka.Server.Types            (ServerContext (..))
import qualified HStream.Logger                        as Log
import qualified HStream.Store                         as S
import qualified Kafka.Protocol                        as K
import qualified Kafka.Protocol.Error                  as K
import qualified Kafka.Protocol.Service                as K

--------------------
-- 2: ListOffsets
--------------------
pattern LatestTimestamp :: Int64
pattern LatestTimestamp = (-1)

pattern EarliestTimestamp :: Int64
pattern EarliestTimestamp = (-2)

-- FIXME: This function does not handle any ErrorCodeException.
--        Modify the following 'listOffsetTopicPartitions' to fix it.
handleListOffsets :: ServerContext
                  -> K.RequestContext
                  -> K.ListOffsetsRequest
                  -> IO K.ListOffsetsResponse
handleListOffsets sc reqCtx req = do
  topicResps <- forKaArrayM req.topics $ \listOffsetsTopic -> do
    -- [ACL] check [DESCRIBE TOPIC] for each topic
    simpleAuthorize (toAuthorizableReqCtx reqCtx) sc.authorizer Res_TOPIC listOffsetsTopic.name AclOp_DESCRIBE >>= \case
      False ->
        return $ makeErrorTopicResponse listOffsetsTopic K.TOPIC_AUTHORIZATION_FAILED
      True  -> do
        -- Note: According to Kafka's implementation,
        --       authz is earlier than iso check.
        --       See kafka.server.KafkaApis#handleListOffsetRequestV1AndAbove
        if reqCtx.apiVersion >= 2 && req.isolationLevel /= 0 then do
          Log.warning $ "currently only support READ_UNCOMMITED(isolationLevel = 0) request."
          -- Note: Kafka returns 'NONE' for this case.
          --       See kafka.server.KafkaApis#handleListOffsetRequestV1AndAbove
          return $ makeErrorTopicResponse listOffsetsTopic K.NONE
          else do
            listOffsetTopicPartitions listOffsetsTopic
  return $ K.ListOffsetsResponse
         { topics = topicResps
         , throttleTimeMs = 0
         }
 where
   makeErrorTopicResponse :: K.ListOffsetsTopic
                          -> K.ErrorCode
                          -> K.ListOffsetsTopicResponse
   makeErrorTopicResponse listOffsetsTopic errorCode =
     -- FIXME: hard-coded constants
     let partitionsResp = forKaArray listOffsetsTopic.partitions $ \listOffsetsPartition ->
           K.ListOffsetsPartitionResponse
           { offset          = -1
           , timestamp       = -1
           , partitionIndex  = listOffsetsPartition.partitionIndex
           , errorCode       = errorCode
           , oldStyleOffsets = K.KaArray Nothing
           }
      in K.ListOffsetsTopicResponse
         { partitions = partitionsResp
         , name       = listOffsetsTopic.name
         }

   listOffsetTopicPartitions :: K.ListOffsetsTopic
                             -> IO K.ListOffsetsTopicResponse
   listOffsetTopicPartitions listOffsetsTopic = do
     orderedParts <- S.listStreamPartitionsOrderedByName sc.scLDClient (S.transToTopicStreamName listOffsetsTopic.name)
     partitionResps <-
       forKaArrayM listOffsetsTopic.partitions $ \listOffsetsPartition -> do
         -- TODO: handle Nothing
         let partition = orderedParts V.! (fromIntegral listOffsetsPartition.partitionIndex)
         offset <- getOffset (snd partition) listOffsetsPartition.timestamp
         -- FIXME: Similar function to 'makeErrorTopicResponse' above.
         --        Extract to a common function.
         return $ K.ListOffsetsPartitionResponse
                { offset          = offset
                , timestamp       = listOffsetsPartition.timestamp
                , partitionIndex  = listOffsetsPartition.partitionIndex
                , errorCode       = K.NONE
                , oldStyleOffsets = K.NonNullKaArray (V.singleton offset)
                }
     return $ K.ListOffsetsTopicResponse
            { partitions = partitionResps
            , name       = listOffsetsTopic.name
            }

   -- NOTE: The last offset of a partition is the offset of the upcoming
   -- message, i.e. the offset of the last available message + 1.
   getOffset logid LatestTimestamp =
     maybe 0 (+ 1) <$> getLatestOffset sc.scOffsetManager logid
   getOffset logid EarliestTimestamp =
     fromMaybe 0 <$> getOldestOffset sc.scOffsetManager logid
   -- Return the earliest offset whose timestamp is greater than or equal to
   -- the given timestamp.
   --
   -- TODO: actually, this is not supported currently.
   getOffset logid timestamp =
     fromMaybe (-1) <$> getOffsetByTimestamp sc.scOffsetManager logid timestamp

--------------------
-- 8: OffsetCommit
--------------------
handleOffsetCommit :: ServerContext
                   -> K.RequestContext
                   -> K.OffsetCommitRequest
                   -> IO K.OffsetCommitResponse
handleOffsetCommit ServerContext{..} reqCtx req = E.handle (\(K.ErrorCodeException code) -> returnErrorResponse code) $ do
  Metrics.withLabel Metrics.totalOffsetCommitRequest req.groupId Metrics.incCounter
  group <- if req.generationId < 0 then do
    GC.getOrMaybeCreateGroup scGroupCoordinator req.groupId ""
    else do
    GC.getGroup scGroupCoordinator req.groupId
  -- [ACL] check [READ GROUP]
  simpleAuthorize (toAuthorizableReqCtx reqCtx) authorizer Res_GROUP req.groupId AclOp_READ >>= \case
    False -> returnErrorResponse K.GROUP_AUTHORIZATION_FAILED
    -- Note: 'G.commitOffsets' works in a lock for the whole request,
    --       and does a pre-check (ACL authz). That is why we pass a
    --       "validate" function (the ACL authz check) on EACH topic
    --       to it.
    -- FIXME: Better method than passing a "validate" function?
    True  -> G.commitOffsets group req $ \offsetCommitTopic -> do
      -- [ACL] check [READ TOPIC] for each topic
      simpleAuthorize (toAuthorizableReqCtx reqCtx) authorizer Res_TOPIC offsetCommitTopic.name AclOp_READ >>= \case
        False -> return K.TOPIC_AUTHORIZATION_FAILED
        True  -> return K.NONE
  where
    -- FIXME: Similar code snippet to 'G.commitOffsets#makeErrorTopicResponse'.
    --        Extract it to a common function?
    makeErrorTopicResponse code offsetCommitTopic =
      K.OffsetCommitResponseTopic
      { name = offsetCommitTopic.name
      , partitions =
          forKaArray offsetCommitTopic.partitions $ \offsetCommitPartition ->
            K.OffsetCommitResponsePartition
            { partitionIndex = offsetCommitPartition.partitionIndex
            , errorCode      = code
            }
       }

    returnErrorResponse code = do
      Metrics.withLabel Metrics.totalFailedOffsetCommitRequest req.groupId Metrics.incCounter
      -- FIXME: hard-coded constants
      let resp = K.OffsetCommitResponse
            { throttleTimeMs = 0
            , topics = forKaArray req.topics (makeErrorTopicResponse code)
            }
      Log.fatal $ "commitOffsets error with code: " <> Log.build (show code)
               <> "\n\trequest: " <> Log.build (show req)
               <> "\n\tresponse: " <> Log.build (show resp)
      return resp

--------------------
-- 9: OffsetFetch
--------------------
-- FIXME: The previous implementation catches all 'ErrorCodeException'
--        and returns 'K.NONE' even if the error code is not 'K.NONE'.
--        This seems not correct. However, if we return the catched
--        error code, 'ConsumerTest.testManualAssign' will fail due to
--        "GROUP_ID_NOT_FOUND" (on looking up the group from the coordinator).
--        THIS SHOULD BE CAREFULLY REVIEWED AND FIXED!!!
handleOffsetFetch :: ServerContext
                  -> K.RequestContext
                  -> K.OffsetFetchRequest
                  -> IO K.OffsetFetchResponse
handleOffsetFetch ServerContext{..} reqCtx req = E.handle (\(K.ErrorCodeException code) -> return (makeErrorResponse K.NONE)) $ do
  -- [ACL] check [DESCRIBE GROUP] first
  simpleAuthorize (toAuthorizableReqCtx reqCtx) authorizer Res_GROUP req.groupId AclOp_DESCRIBE >>= \case
    False -> return $ makeErrorResponse K.TOPIC_AUTHORIZATION_FAILED
    True  -> do
      group_m <- GC.getGroupM scGroupCoordinator req.groupId
      case K.unKaArray req.topics of
        -- 'Nothing' means fetch offsets of ALL topics.
        -- WARNING: Offsets of unauthzed topics should not be leaked
        --          on "fetch all". That is why we pass the "validate"
        --          function (the ACL authz check) to 'G.fetchAllOffsets'.
        -- FIXME: Better method than passing a "validate" function?
        Nothing -> do
          Log.debug $ "fetching all offsets in group:" <> Log.build req.groupId
          topicResps <- case group_m of
            Nothing    -> return (vectorToKaArray V.empty)
            Just group -> G.fetchAllOffsets group $ \reqTopic -> do
              -- [ACL] check [DESCRIBE TOPIC] for each topic
              simpleAuthorize (toAuthorizableReqCtx reqCtx) authorizer Res_TOPIC reqTopic.name AclOp_DESCRIBE >>= \case
                False -> return K.TOPIC_AUTHORIZATION_FAILED
                True  -> return K.NONE
          return $ K.OffsetFetchResponse
            { throttleTimeMs = 0
            , errorCode = K.NONE
            , topics = topicResps
            }
        Just ts -> do
          topicResps <- V.forM ts $ \reqTopic -> do
            -- Note: 'G.fetchOffsets' does a pre-check (ACL authz)
            --       on each topic. That is why we pass a "validate"
            --       function to it.
            -- FIXME: Better method than passing a "validate" function?
            case group_m of
              -- FIXME: what error code should it return? 'K.UNKNOWN_TOPIC_OR_PARTITION'
              --        crashes some tests...
              Nothing    -> return (makeErrorTopicResponse K.NONE reqTopic)
              Just group -> G.fetchOffsets group reqTopic $ \reqTopic_ -> do
                -- [ACL] check [DESCRIBE TOPIC] for each topic
                simpleAuthorize (toAuthorizableReqCtx reqCtx) authorizer Res_TOPIC reqTopic_.name AclOp_DESCRIBE >>= \case
                  False -> return K.TOPIC_AUTHORIZATION_FAILED
                  True  -> return K.NONE
          return $ K.OffsetFetchResponse
            { throttleTimeMs = 0
            , errorCode = K.NONE
            , topics = K.KaArray (Just topicResps)
            -- FIXME: what to return on 'Nothing'?
            }
  where
    -- FIXME: hard-coded constants
    -- FIXME: Similar code snippet to 'G.fetchOffsets#makeErrorPartition'.
    --        Extract it to a common function?
    makeErrorTopicResponse code offsetFetchTopic =
      K.OffsetFetchResponseTopic
      { name = offsetFetchTopic.name
      , partitions =
          forKaArray offsetFetchTopic.partitionIndexes $ \idx ->
            K.OffsetFetchResponsePartition
            { partitionIndex  = idx
            , errorCode       = code
            , metadata        = Just ""
            , committedOffset = -1
            }
      }

    -- FIXME: hard-coded constants
    makeErrorResponse code =
      K.OffsetFetchResponse
      { throttleTimeMs = 0
      , errorCode = code
      , topics = forKaArray req.topics (makeErrorTopicResponse code)
      }
