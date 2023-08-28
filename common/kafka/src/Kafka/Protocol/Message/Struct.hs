{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE TypeFamilies          #-}

-------------------------------------------------------------------------------
-- TODO: Generate by kafka message json schema

module Kafka.Protocol.Message.Struct where

import           Data.ByteString         (ByteString)
import           Data.Int
import           Data.Text               (Text)
import           GHC.Generics

import           Kafka.Protocol.Encoding
import           Kafka.Protocol.Error
import           Kafka.Protocol.Service

-------------------------------------------------------------------------------

data ApiVersionV0 = ApiVersionV0
  { apiKey     :: {-# UNPACK #-} !ApiKey
  , minVersion :: {-# UNPACK #-} !Int16
  , maxVersion :: {-# UNPACK #-} !Int16
  } deriving (Show, Generic)
instance Serializable ApiVersionV0

type ApiVersionV1 = ApiVersionV0

type ApiVersionV2 = ApiVersionV0

data CreatableReplicaAssignmentV0 = CreatableReplicaAssignmentV0
  { partitionIndex :: {-# UNPACK #-} !Int32
  , brokerIds      :: !(KaArray Int32)
  } deriving (Show, Generic)
instance Serializable CreatableReplicaAssignmentV0

data CreateableTopicConfigV0 = CreateableTopicConfigV0
  { name  :: !Text
  , value :: !NullableString
  } deriving (Show, Generic)
instance Serializable CreateableTopicConfigV0

data CreatableTopicV0 = CreatableTopicV0
  { name              :: !Text
  , numPartitions     :: {-# UNPACK #-} !Int32
  , replicationFactor :: {-# UNPACK #-} !Int16
  , assignments       :: !(KaArray CreatableReplicaAssignmentV0)
  , configs           :: !(KaArray CreateableTopicConfigV0)
  } deriving (Show, Generic)
instance Serializable CreatableTopicV0

data CreatableTopicResultV0 = CreatableTopicResultV0
  { name      :: !Text
  , errorCode :: {-# UNPACK #-} !ErrorCode
  } deriving (Show, Generic)
instance Serializable CreatableTopicResultV0

data DeletableTopicResultV0 = DeletableTopicResultV0
  { name      :: !Text
  , errorCode :: {-# UNPACK #-} !ErrorCode
  } deriving (Show, Generic)
instance Serializable DeletableTopicResultV0

data DescribedGroupMemberV0 = DescribedGroupMemberV0
  { memberId         :: !Text
  , clientId         :: !Text
  , clientHost       :: !Text
  , memberMetadata   :: !ByteString
  , memberAssignment :: !ByteString
  } deriving (Show, Generic)
instance Serializable DescribedGroupMemberV0

data DescribedGroupV0 = DescribedGroupV0
  { errorCode    :: {-# UNPACK #-} !ErrorCode
  , groupId      :: !Text
  , groupState   :: !Text
  , protocolType :: !Text
  , protocolData :: !Text
  , members      :: !(KaArray DescribedGroupMemberV0)
  } deriving (Show, Generic)
instance Serializable DescribedGroupV0

data FetchPartitionV0 = FetchPartitionV0
  { partition         :: {-# UNPACK #-} !Int32
  , fetchOffset       :: {-# UNPACK #-} !Int64
  , partitionMaxBytes :: {-# UNPACK #-} !Int32
  } deriving (Show, Generic)
instance Serializable FetchPartitionV0

data FetchTopicV0 = FetchTopicV0
  { topic      :: !Text
  , partitions :: !(KaArray FetchPartitionV0)
  } deriving (Show, Generic)
instance Serializable FetchTopicV0

data PartitionDataV0 = PartitionDataV0
  { partitionIndex :: {-# UNPACK #-} !Int32
  , errorCode      :: {-# UNPACK #-} !ErrorCode
  , highWatermark  :: {-# UNPACK #-} !Int64
  , recordBytes    :: !NullableBytes
  } deriving (Show, Generic)
instance Serializable PartitionDataV0

data FetchableTopicResponseV0 = FetchableTopicResponseV0
  { topic      :: !Text
  , partitions :: !(KaArray PartitionDataV0)
  } deriving (Show, Generic)
instance Serializable FetchableTopicResponseV0

data JoinGroupRequestProtocolV0 = JoinGroupRequestProtocolV0
  { name     :: !Text
  , metadata :: !ByteString
  } deriving (Show, Generic)
instance Serializable JoinGroupRequestProtocolV0

data JoinGroupResponseMemberV0 = JoinGroupResponseMemberV0
  { memberId :: !Text
  , metadata :: !ByteString
  } deriving (Show, Generic)
instance Serializable JoinGroupResponseMemberV0

data ListedGroupV0 = ListedGroupV0
  { groupId      :: !Text
  , protocolType :: !Text
  } deriving (Show, Generic)
instance Serializable ListedGroupV0

data ListOffsetsPartitionV0 = ListOffsetsPartitionV0
  { partitionIndex :: {-# UNPACK #-} !Int32
  , timestamp      :: {-# UNPACK #-} !Int64
  , maxNumOffsets  :: {-# UNPACK #-} !Int32
  } deriving (Show, Generic)
instance Serializable ListOffsetsPartitionV0

data ListOffsetsTopicV0 = ListOffsetsTopicV0
  { name       :: !Text
  , partitions :: !(KaArray ListOffsetsPartitionV0)
  } deriving (Show, Generic)
instance Serializable ListOffsetsTopicV0

data ListOffsetsPartitionResponseV0 = ListOffsetsPartitionResponseV0
  { partitionIndex  :: {-# UNPACK #-} !Int32
  , errorCode       :: {-# UNPACK #-} !ErrorCode
  , oldStyleOffsets :: !(KaArray Int64)
  } deriving (Show, Generic)
instance Serializable ListOffsetsPartitionResponseV0

data ListOffsetsTopicResponseV0 = ListOffsetsTopicResponseV0
  { name       :: !Text
  , partitions :: !(KaArray ListOffsetsPartitionResponseV0)
  } deriving (Show, Generic)
instance Serializable ListOffsetsTopicResponseV0

newtype MetadataRequestTopicV0 = MetadataRequestTopicV0
  { name :: Text
  } deriving (Show, Generic)
instance Serializable MetadataRequestTopicV0

type MetadataRequestTopicV1 = MetadataRequestTopicV0

data MetadataResponseBrokerV0 = MetadataResponseBrokerV0
  { nodeId :: {-# UNPACK #-} !Int32
  , host   :: !Text
  , port   :: {-# UNPACK #-} !Int32
  } deriving (Show, Generic)
instance Serializable MetadataResponseBrokerV0

data MetadataResponsePartitionV0 = MetadataResponsePartitionV0
  { errorCode      :: {-# UNPACK #-} !ErrorCode
  , partitionIndex :: {-# UNPACK #-} !Int32
  , leaderId       :: {-# UNPACK #-} !Int32
  , replicaNodes   :: !(KaArray Int32)
  , isrNodes       :: !(KaArray Int32)
  } deriving (Show, Generic)
instance Serializable MetadataResponsePartitionV0

data MetadataResponseTopicV0 = MetadataResponseTopicV0
  { errorCode  :: {-# UNPACK #-} !ErrorCode
  , name       :: !Text
  , partitions :: !(KaArray MetadataResponsePartitionV0)
  } deriving (Show, Generic)
instance Serializable MetadataResponseTopicV0

data MetadataResponseBrokerV1 = MetadataResponseBrokerV1
  { nodeId :: {-# UNPACK #-} !Int32
  , host   :: !Text
  , port   :: {-# UNPACK #-} !Int32
  , rack   :: !NullableString
  } deriving (Show, Generic)
instance Serializable MetadataResponseBrokerV1

type MetadataResponsePartitionV1 = MetadataResponsePartitionV0

data MetadataResponseTopicV1 = MetadataResponseTopicV1
  { errorCode  :: {-# UNPACK #-} !ErrorCode
  , name       :: !Text
  , isInternal :: Bool
  , partitions :: !(KaArray MetadataResponsePartitionV0)
  } deriving (Show, Generic)
instance Serializable MetadataResponseTopicV1

data OffsetCommitRequestPartitionV0 = OffsetCommitRequestPartitionV0
  { partitionIndex    :: {-# UNPACK #-} !Int32
  , committedOffset   :: {-# UNPACK #-} !Int64
  , committedMetadata :: !NullableString
  } deriving (Show, Generic)
instance Serializable OffsetCommitRequestPartitionV0

data OffsetCommitRequestTopicV0 = OffsetCommitRequestTopicV0
  { name       :: !Text
  , partitions :: !(KaArray OffsetCommitRequestPartitionV0)
  } deriving (Show, Generic)
instance Serializable OffsetCommitRequestTopicV0

data OffsetCommitResponsePartitionV0 = OffsetCommitResponsePartitionV0
  { partitionIndex :: {-# UNPACK #-} !Int32
  , errorCode      :: {-# UNPACK #-} !ErrorCode
  } deriving (Show, Generic)
instance Serializable OffsetCommitResponsePartitionV0

data OffsetCommitResponseTopicV0 = OffsetCommitResponseTopicV0
  { name       :: !Text
  , partitions :: !(KaArray OffsetCommitResponsePartitionV0)
  } deriving (Show, Generic)
instance Serializable OffsetCommitResponseTopicV0

data OffsetFetchRequestTopicV0 = OffsetFetchRequestTopicV0
  { name             :: !Text
  , partitionIndexes :: !(KaArray Int32)
  } deriving (Show, Generic)
instance Serializable OffsetFetchRequestTopicV0

data OffsetFetchResponsePartitionV0 = OffsetFetchResponsePartitionV0
  { partitionIndex  :: {-# UNPACK #-} !Int32
  , committedOffset :: {-# UNPACK #-} !Int64
  , metadata        :: !NullableString
  , errorCode       :: {-# UNPACK #-} !ErrorCode
  } deriving (Show, Generic)
instance Serializable OffsetFetchResponsePartitionV0

data OffsetFetchResponseTopicV0 = OffsetFetchResponseTopicV0
  { name       :: !Text
  , partitions :: !(KaArray OffsetFetchResponsePartitionV0)
  } deriving (Show, Generic)
instance Serializable OffsetFetchResponseTopicV0

data PartitionProduceDataV0 = PartitionProduceDataV0
  { index       :: {-# UNPACK #-} !Int32
  , recordBytes :: !NullableBytes
  } deriving (Show, Generic)
instance Serializable PartitionProduceDataV0

data TopicProduceDataV0 = TopicProduceDataV0
  { name          :: !Text
  , partitionData :: !(KaArray PartitionProduceDataV0)
  } deriving (Show, Generic)
instance Serializable TopicProduceDataV0

data PartitionProduceResponseV0 = PartitionProduceResponseV0
  { index      :: {-# UNPACK #-} !Int32
  , errorCode  :: {-# UNPACK #-} !ErrorCode
  , baseOffset :: {-# UNPACK #-} !Int64
  } deriving (Show, Generic)
instance Serializable PartitionProduceResponseV0

data TopicProduceResponseV0 = TopicProduceResponseV0
  { name               :: !Text
  , partitionResponses :: !(KaArray PartitionProduceResponseV0)
  } deriving (Show, Generic)
instance Serializable TopicProduceResponseV0

data SyncGroupRequestAssignmentV0 = SyncGroupRequestAssignmentV0
  { memberId   :: !Text
  , assignment :: !ByteString
  } deriving (Show, Generic)
instance Serializable SyncGroupRequestAssignmentV0

-------------------------------------------------------------------------------

data ApiVersionsRequestV0 = ApiVersionsRequestV0
  deriving (Show, Generic)
instance Serializable ApiVersionsRequestV0

type ApiVersionsRequestV1 = ApiVersionsRequestV0

type ApiVersionsRequestV2 = ApiVersionsRequestV0

data ApiVersionsResponseV0 = ApiVersionsResponseV0
  { errorCode :: {-# UNPACK #-} !ErrorCode
  , apiKeys   :: !(KaArray ApiVersionV0)
  } deriving (Show, Generic)
instance Serializable ApiVersionsResponseV0

data ApiVersionsResponseV1 = ApiVersionsResponseV1
  { errorCode      :: {-# UNPACK #-} !ErrorCode
  , apiKeys        :: !(KaArray ApiVersionV0)
  , throttleTimeMs :: {-# UNPACK #-} !Int32
  } deriving (Show, Generic)
instance Serializable ApiVersionsResponseV1

type ApiVersionsResponseV2 = ApiVersionsResponseV1

data CreateTopicsRequestV0 = CreateTopicsRequestV0
  { topics    :: !(KaArray CreatableTopicV0)
  , timeoutMs :: {-# UNPACK #-} !Int32
  } deriving (Show, Generic)
instance Serializable CreateTopicsRequestV0

newtype CreateTopicsResponseV0 = CreateTopicsResponseV0
  { topics :: (KaArray CreatableTopicResultV0)
  } deriving (Show, Generic)
instance Serializable CreateTopicsResponseV0

data DeleteTopicsRequestV0 = DeleteTopicsRequestV0
  { topicNames :: !(KaArray Text)
  , timeoutMs  :: {-# UNPACK #-} !Int32
  } deriving (Show, Generic)
instance Serializable DeleteTopicsRequestV0

newtype DeleteTopicsResponseV0 = DeleteTopicsResponseV0
  { responses :: (KaArray DeletableTopicResultV0)
  } deriving (Show, Generic)
instance Serializable DeleteTopicsResponseV0

newtype DescribeGroupsRequestV0 = DescribeGroupsRequestV0
  { groups :: (KaArray Text)
  } deriving (Show, Generic)
instance Serializable DescribeGroupsRequestV0

newtype DescribeGroupsResponseV0 = DescribeGroupsResponseV0
  { groups :: (KaArray DescribedGroupV0)
  } deriving (Show, Generic)
instance Serializable DescribeGroupsResponseV0

data FetchRequestV0 = FetchRequestV0
  { replicaId :: {-# UNPACK #-} !Int32
  , maxWaitMs :: {-# UNPACK #-} !Int32
  , minBytes  :: {-# UNPACK #-} !Int32
  , topics    :: !(KaArray FetchTopicV0)
  } deriving (Show, Generic)
instance Serializable FetchRequestV0

newtype FetchResponseV0 = FetchResponseV0
  { responses :: (KaArray FetchableTopicResponseV0)
  } deriving (Show, Generic)
instance Serializable FetchResponseV0

newtype FindCoordinatorRequestV0 = FindCoordinatorRequestV0
  { key :: Text
  } deriving (Show, Generic)
instance Serializable FindCoordinatorRequestV0

data FindCoordinatorResponseV0 = FindCoordinatorResponseV0
  { errorCode :: {-# UNPACK #-} !ErrorCode
  , nodeId    :: {-# UNPACK #-} !Int32
  , host      :: !Text
  , port      :: {-# UNPACK #-} !Int32
  } deriving (Show, Generic)
instance Serializable FindCoordinatorResponseV0

data HeartbeatRequestV0 = HeartbeatRequestV0
  { groupId      :: !Text
  , generationId :: {-# UNPACK #-} !Int32
  , memberId     :: !Text
  } deriving (Show, Generic)
instance Serializable HeartbeatRequestV0

newtype HeartbeatResponseV0 = HeartbeatResponseV0
  { errorCode :: ErrorCode
  } deriving (Show, Generic)
instance Serializable HeartbeatResponseV0

data JoinGroupRequestV0 = JoinGroupRequestV0
  { groupId          :: !Text
  , sessionTimeoutMs :: {-# UNPACK #-} !Int32
  , memberId         :: !Text
  , protocolType     :: !Text
  , protocols        :: !(KaArray JoinGroupRequestProtocolV0)
  } deriving (Show, Generic)
instance Serializable JoinGroupRequestV0

data JoinGroupResponseV0 = JoinGroupResponseV0
  { errorCode    :: {-# UNPACK #-} !ErrorCode
  , generationId :: {-# UNPACK #-} !Int32
  , protocolName :: !Text
  , leader       :: !Text
  , memberId     :: !Text
  , members      :: !(KaArray JoinGroupResponseMemberV0)
  } deriving (Show, Generic)
instance Serializable JoinGroupResponseV0

data LeaveGroupRequestV0 = LeaveGroupRequestV0
  { groupId  :: !Text
  , memberId :: !Text
  } deriving (Show, Generic)
instance Serializable LeaveGroupRequestV0

newtype LeaveGroupResponseV0 = LeaveGroupResponseV0
  { errorCode :: ErrorCode
  } deriving (Show, Generic)
instance Serializable LeaveGroupResponseV0

data ListGroupsRequestV0 = ListGroupsRequestV0
  deriving (Show, Generic)
instance Serializable ListGroupsRequestV0

data ListGroupsResponseV0 = ListGroupsResponseV0
  { errorCode :: {-# UNPACK #-} !ErrorCode
  , groups    :: !(KaArray ListedGroupV0)
  } deriving (Show, Generic)
instance Serializable ListGroupsResponseV0

data ListOffsetsRequestV0 = ListOffsetsRequestV0
  { replicaId :: {-# UNPACK #-} !Int32
  , topics    :: !(KaArray ListOffsetsTopicV0)
  } deriving (Show, Generic)
instance Serializable ListOffsetsRequestV0

newtype ListOffsetsResponseV0 = ListOffsetsResponseV0
  { topics :: (KaArray ListOffsetsTopicResponseV0)
  } deriving (Show, Generic)
instance Serializable ListOffsetsResponseV0

newtype MetadataRequestV0 = MetadataRequestV0
  { topics :: (KaArray MetadataRequestTopicV0)
  } deriving (Show, Generic)
instance Serializable MetadataRequestV0

type MetadataRequestV1 = MetadataRequestV0

data MetadataResponseV0 = MetadataResponseV0
  { brokers :: !(KaArray MetadataResponseBrokerV0)
  , topics  :: !(KaArray MetadataResponseTopicV0)
  } deriving (Show, Generic)
instance Serializable MetadataResponseV0

data MetadataResponseV1 = MetadataResponseV1
  { brokers      :: !(KaArray MetadataResponseBrokerV1)
  , controllerId :: {-# UNPACK #-} !Int32
  , topics       :: !(KaArray MetadataResponseTopicV1)
  } deriving (Show, Generic)
instance Serializable MetadataResponseV1

data OffsetCommitRequestV0 = OffsetCommitRequestV0
  { groupId :: !Text
  , topics  :: !(KaArray OffsetCommitRequestTopicV0)
  } deriving (Show, Generic)
instance Serializable OffsetCommitRequestV0

newtype OffsetCommitResponseV0 = OffsetCommitResponseV0
  { topics :: (KaArray OffsetCommitResponseTopicV0)
  } deriving (Show, Generic)
instance Serializable OffsetCommitResponseV0

data OffsetFetchRequestV0 = OffsetFetchRequestV0
  { groupId :: !Text
  , topics  :: !(KaArray OffsetFetchRequestTopicV0)
  } deriving (Show, Generic)
instance Serializable OffsetFetchRequestV0

newtype OffsetFetchResponseV0 = OffsetFetchResponseV0
  { topics :: (KaArray OffsetFetchResponseTopicV0)
  } deriving (Show, Generic)
instance Serializable OffsetFetchResponseV0

data ProduceRequestV0 = ProduceRequestV0
  { acks      :: {-# UNPACK #-} !Int16
  , timeoutMs :: {-# UNPACK #-} !Int32
  , topicData :: !(KaArray TopicProduceDataV0)
  } deriving (Show, Generic)
instance Serializable ProduceRequestV0

newtype ProduceResponseV0 = ProduceResponseV0
  { responses :: (KaArray TopicProduceResponseV0)
  } deriving (Show, Generic)
instance Serializable ProduceResponseV0

data SyncGroupRequestV0 = SyncGroupRequestV0
  { groupId      :: !Text
  , generationId :: {-# UNPACK #-} !Int32
  , memberId     :: !Text
  , assignments  :: !(KaArray SyncGroupRequestAssignmentV0)
  } deriving (Show, Generic)
instance Serializable SyncGroupRequestV0

data SyncGroupResponseV0 = SyncGroupResponseV0
  { errorCode  :: {-# UNPACK #-} !ErrorCode
  , assignment :: !ByteString
  } deriving (Show, Generic)
instance Serializable SyncGroupResponseV0

-------------------------------------------------------------------------------

data HStreamKafkaV0

instance Service HStreamKafkaV0 where
  type ServiceName HStreamKafkaV0 = "HStreamKafkaV0"
  type ServiceMethods HStreamKafkaV0 =
    '[ "produce"
     , "fetch"
     , "listOffsets"
     , "metadata"
     , "offsetCommit"
     , "offsetFetch"
     , "findCoordinator"
     , "joinGroup"
     , "heartbeat"
     , "leaveGroup"
     , "syncGroup"
     , "describeGroups"
     , "listGroups"
     , "apiVersions"
     , "createTopics"
     , "deleteTopics"
     ]

instance HasMethodImpl HStreamKafkaV0 "produce" where
  type MethodName HStreamKafkaV0 "produce" = "produce"
  type MethodKey HStreamKafkaV0 "produce" = 0
  type MethodVersion HStreamKafkaV0 "produce" = 0
  type MethodInput HStreamKafkaV0 "produce" = ProduceRequestV0
  type MethodOutput HStreamKafkaV0 "produce" = ProduceResponseV0

instance HasMethodImpl HStreamKafkaV0 "fetch" where
  type MethodName HStreamKafkaV0 "fetch" = "fetch"
  type MethodKey HStreamKafkaV0 "fetch" = 1
  type MethodVersion HStreamKafkaV0 "fetch" = 0
  type MethodInput HStreamKafkaV0 "fetch" = FetchRequestV0
  type MethodOutput HStreamKafkaV0 "fetch" = FetchResponseV0

instance HasMethodImpl HStreamKafkaV0 "listOffsets" where
  type MethodName HStreamKafkaV0 "listOffsets" = "listOffsets"
  type MethodKey HStreamKafkaV0 "listOffsets" = 2
  type MethodVersion HStreamKafkaV0 "listOffsets" = 0
  type MethodInput HStreamKafkaV0 "listOffsets" = ListOffsetsRequestV0
  type MethodOutput HStreamKafkaV0 "listOffsets" = ListOffsetsResponseV0

instance HasMethodImpl HStreamKafkaV0 "metadata" where
  type MethodName HStreamKafkaV0 "metadata" = "metadata"
  type MethodKey HStreamKafkaV0 "metadata" = 3
  type MethodVersion HStreamKafkaV0 "metadata" = 0
  type MethodInput HStreamKafkaV0 "metadata" = MetadataRequestV0
  type MethodOutput HStreamKafkaV0 "metadata" = MetadataResponseV0

instance HasMethodImpl HStreamKafkaV0 "offsetCommit" where
  type MethodName HStreamKafkaV0 "offsetCommit" = "offsetCommit"
  type MethodKey HStreamKafkaV0 "offsetCommit" = 8
  type MethodVersion HStreamKafkaV0 "offsetCommit" = 0
  type MethodInput HStreamKafkaV0 "offsetCommit" = OffsetCommitRequestV0
  type MethodOutput HStreamKafkaV0 "offsetCommit" = OffsetCommitResponseV0

instance HasMethodImpl HStreamKafkaV0 "offsetFetch" where
  type MethodName HStreamKafkaV0 "offsetFetch" = "offsetFetch"
  type MethodKey HStreamKafkaV0 "offsetFetch" = 9
  type MethodVersion HStreamKafkaV0 "offsetFetch" = 0
  type MethodInput HStreamKafkaV0 "offsetFetch" = OffsetFetchRequestV0
  type MethodOutput HStreamKafkaV0 "offsetFetch" = OffsetFetchResponseV0

instance HasMethodImpl HStreamKafkaV0 "findCoordinator" where
  type MethodName HStreamKafkaV0 "findCoordinator" = "findCoordinator"
  type MethodKey HStreamKafkaV0 "findCoordinator" = 10
  type MethodVersion HStreamKafkaV0 "findCoordinator" = 0
  type MethodInput HStreamKafkaV0 "findCoordinator" = FindCoordinatorRequestV0
  type MethodOutput HStreamKafkaV0 "findCoordinator" = FindCoordinatorResponseV0

instance HasMethodImpl HStreamKafkaV0 "joinGroup" where
  type MethodName HStreamKafkaV0 "joinGroup" = "joinGroup"
  type MethodKey HStreamKafkaV0 "joinGroup" = 11
  type MethodVersion HStreamKafkaV0 "joinGroup" = 0
  type MethodInput HStreamKafkaV0 "joinGroup" = JoinGroupRequestV0
  type MethodOutput HStreamKafkaV0 "joinGroup" = JoinGroupResponseV0

instance HasMethodImpl HStreamKafkaV0 "heartbeat" where
  type MethodName HStreamKafkaV0 "heartbeat" = "heartbeat"
  type MethodKey HStreamKafkaV0 "heartbeat" = 12
  type MethodVersion HStreamKafkaV0 "heartbeat" = 0
  type MethodInput HStreamKafkaV0 "heartbeat" = HeartbeatRequestV0
  type MethodOutput HStreamKafkaV0 "heartbeat" = HeartbeatResponseV0

instance HasMethodImpl HStreamKafkaV0 "leaveGroup" where
  type MethodName HStreamKafkaV0 "leaveGroup" = "leaveGroup"
  type MethodKey HStreamKafkaV0 "leaveGroup" = 13
  type MethodVersion HStreamKafkaV0 "leaveGroup" = 0
  type MethodInput HStreamKafkaV0 "leaveGroup" = LeaveGroupRequestV0
  type MethodOutput HStreamKafkaV0 "leaveGroup" = LeaveGroupResponseV0

instance HasMethodImpl HStreamKafkaV0 "syncGroup" where
  type MethodName HStreamKafkaV0 "syncGroup" = "syncGroup"
  type MethodKey HStreamKafkaV0 "syncGroup" = 14
  type MethodVersion HStreamKafkaV0 "syncGroup" = 0
  type MethodInput HStreamKafkaV0 "syncGroup" = SyncGroupRequestV0
  type MethodOutput HStreamKafkaV0 "syncGroup" = SyncGroupResponseV0

instance HasMethodImpl HStreamKafkaV0 "describeGroups" where
  type MethodName HStreamKafkaV0 "describeGroups" = "describeGroups"
  type MethodKey HStreamKafkaV0 "describeGroups" = 15
  type MethodVersion HStreamKafkaV0 "describeGroups" = 0
  type MethodInput HStreamKafkaV0 "describeGroups" = DescribeGroupsRequestV0
  type MethodOutput HStreamKafkaV0 "describeGroups" = DescribeGroupsResponseV0

instance HasMethodImpl HStreamKafkaV0 "listGroups" where
  type MethodName HStreamKafkaV0 "listGroups" = "listGroups"
  type MethodKey HStreamKafkaV0 "listGroups" = 16
  type MethodVersion HStreamKafkaV0 "listGroups" = 0
  type MethodInput HStreamKafkaV0 "listGroups" = ListGroupsRequestV0
  type MethodOutput HStreamKafkaV0 "listGroups" = ListGroupsResponseV0

instance HasMethodImpl HStreamKafkaV0 "apiVersions" where
  type MethodName HStreamKafkaV0 "apiVersions" = "apiVersions"
  type MethodKey HStreamKafkaV0 "apiVersions" = 18
  type MethodVersion HStreamKafkaV0 "apiVersions" = 0
  type MethodInput HStreamKafkaV0 "apiVersions" = ApiVersionsRequestV0
  type MethodOutput HStreamKafkaV0 "apiVersions" = ApiVersionsResponseV0

instance HasMethodImpl HStreamKafkaV0 "createTopics" where
  type MethodName HStreamKafkaV0 "createTopics" = "createTopics"
  type MethodKey HStreamKafkaV0 "createTopics" = 19
  type MethodVersion HStreamKafkaV0 "createTopics" = 0
  type MethodInput HStreamKafkaV0 "createTopics" = CreateTopicsRequestV0
  type MethodOutput HStreamKafkaV0 "createTopics" = CreateTopicsResponseV0

instance HasMethodImpl HStreamKafkaV0 "deleteTopics" where
  type MethodName HStreamKafkaV0 "deleteTopics" = "deleteTopics"
  type MethodKey HStreamKafkaV0 "deleteTopics" = 20
  type MethodVersion HStreamKafkaV0 "deleteTopics" = 0
  type MethodInput HStreamKafkaV0 "deleteTopics" = DeleteTopicsRequestV0
  type MethodOutput HStreamKafkaV0 "deleteTopics" = DeleteTopicsResponseV0

data HStreamKafkaV1

instance Service HStreamKafkaV1 where
  type ServiceName HStreamKafkaV1 = "HStreamKafkaV1"
  type ServiceMethods HStreamKafkaV1 =
    '[ "metadata"
     , "apiVersions"
     ]

instance HasMethodImpl HStreamKafkaV1 "metadata" where
  type MethodName HStreamKafkaV1 "metadata" = "metadata"
  type MethodKey HStreamKafkaV1 "metadata" = 3
  type MethodVersion HStreamKafkaV1 "metadata" = 1
  type MethodInput HStreamKafkaV1 "metadata" = MetadataRequestV1
  type MethodOutput HStreamKafkaV1 "metadata" = MetadataResponseV1

instance HasMethodImpl HStreamKafkaV1 "apiVersions" where
  type MethodName HStreamKafkaV1 "apiVersions" = "apiVersions"
  type MethodKey HStreamKafkaV1 "apiVersions" = 18
  type MethodVersion HStreamKafkaV1 "apiVersions" = 1
  type MethodInput HStreamKafkaV1 "apiVersions" = ApiVersionsRequestV1
  type MethodOutput HStreamKafkaV1 "apiVersions" = ApiVersionsResponseV1

data HStreamKafkaV2

instance Service HStreamKafkaV2 where
  type ServiceName HStreamKafkaV2 = "HStreamKafkaV2"
  type ServiceMethods HStreamKafkaV2 =
    '[ "apiVersions"
     ]

instance HasMethodImpl HStreamKafkaV2 "apiVersions" where
  type MethodName HStreamKafkaV2 "apiVersions" = "apiVersions"
  type MethodKey HStreamKafkaV2 "apiVersions" = 18
  type MethodVersion HStreamKafkaV2 "apiVersions" = 2
  type MethodInput HStreamKafkaV2 "apiVersions" = ApiVersionsRequestV2
  type MethodOutput HStreamKafkaV2 "apiVersions" = ApiVersionsResponseV2

-------------------------------------------------------------------------------

newtype ApiKey = ApiKey Int16
  deriving newtype (Num, Integral, Real, Enum, Ord, Eq, Bounded, Serializable)

instance Show ApiKey where
  show (ApiKey 0)  = "Produce(0)"
  show (ApiKey 1)  = "Fetch(1)"
  show (ApiKey 2)  = "ListOffsets(2)"
  show (ApiKey 3)  = "Metadata(3)"
  show (ApiKey 8)  = "OffsetCommit(8)"
  show (ApiKey 9)  = "OffsetFetch(9)"
  show (ApiKey 10) = "FindCoordinator(10)"
  show (ApiKey 11) = "JoinGroup(11)"
  show (ApiKey 12) = "Heartbeat(12)"
  show (ApiKey 13) = "LeaveGroup(13)"
  show (ApiKey 14) = "SyncGroup(14)"
  show (ApiKey 15) = "DescribeGroups(15)"
  show (ApiKey 16) = "ListGroups(16)"
  show (ApiKey 18) = "ApiVersions(18)"
  show (ApiKey 19) = "CreateTopics(19)"
  show (ApiKey 20) = "DeleteTopics(20)"
  show (ApiKey n)  = "Unknown " <> show n

supportedApiVersions :: [ApiVersionV0]
supportedApiVersions =
  [ ApiVersionV0 (ApiKey 0) 0 0
  , ApiVersionV0 (ApiKey 1) 0 0
  , ApiVersionV0 (ApiKey 2) 0 0
  , ApiVersionV0 (ApiKey 3) 0 1
  , ApiVersionV0 (ApiKey 8) 0 0
  , ApiVersionV0 (ApiKey 9) 0 0
  , ApiVersionV0 (ApiKey 10) 0 0
  , ApiVersionV0 (ApiKey 11) 0 0
  , ApiVersionV0 (ApiKey 12) 0 0
  , ApiVersionV0 (ApiKey 13) 0 0
  , ApiVersionV0 (ApiKey 14) 0 0
  , ApiVersionV0 (ApiKey 15) 0 0
  , ApiVersionV0 (ApiKey 16) 0 0
  , ApiVersionV0 (ApiKey 18) 0 2
  , ApiVersionV0 (ApiKey 19) 0 0
  , ApiVersionV0 (ApiKey 20) 0 0
  ]
