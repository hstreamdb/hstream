{-# LANGUAGE DuplicateRecordFields #-}

-------------------------------------------------------------------------------
-- TODO: Generate by kafka message json schema

module Kafka.Protocol.Message.Struct where

import           Data.ByteString         (ByteString)
import           Data.Int
import           Data.Text               (Text)
import           GHC.Generics

import           Kafka.Protocol.Encoding
import           Kafka.Protocol.Error

-------------------------------------------------------------------------------

data ApiVersion = ApiVersion
  { apiKey     :: {-# UNPACK #-} !Int16
  , minVersion :: {-# UNPACK #-} !Int16
  , maxVersion :: {-# UNPACK #-} !Int16
  } deriving (Show, Generic)
instance Serializable ApiVersion

data CreatableReplicaAssignment = CreatableReplicaAssignment
  { partitionIndex :: {-# UNPACK #-} !Int32
  , brokerIds      :: !(KaArray Int32)
  } deriving (Show, Generic)
instance Serializable CreatableReplicaAssignment

data CreateableTopicConfig = CreateableTopicConfig
  { name  :: !Text
  , value :: !NullableString
  } deriving (Show, Generic)
instance Serializable CreateableTopicConfig

data CreatableTopic = CreatableTopic
  { name              :: !Text
  , numPartitions     :: {-# UNPACK #-} !Int32
  , replicationFactor :: {-# UNPACK #-} !Int16
  , assignments       :: !(KaArray CreatableReplicaAssignment)
  , configs           :: !(KaArray CreateableTopicConfig)
  } deriving (Show, Generic)
instance Serializable CreatableTopic

data CreatableTopicResult = CreatableTopicResult
  { name      :: !Text
  , errorCode :: {-# UNPACK #-} !ErrorCode
  } deriving (Show, Generic)
instance Serializable CreatableTopicResult

data DeletableTopicResult = DeletableTopicResult
  { name      :: !Text
  , errorCode :: {-# UNPACK #-} !ErrorCode
  } deriving (Show, Generic)
instance Serializable DeletableTopicResult

data DescribedGroupMember = DescribedGroupMember
  { memberId         :: !Text
  , clientId         :: !Text
  , clientHost       :: !Text
  , memberMetadata   :: !ByteString
  , memberAssignment :: !ByteString
  } deriving (Show, Generic)
instance Serializable DescribedGroupMember

data DescribedGroup = DescribedGroup
  { errorCode    :: {-# UNPACK #-} !ErrorCode
  , groupId      :: !Text
  , groupState   :: !Text
  , protocolType :: !Text
  , protocolData :: !Text
  , members      :: !(KaArray DescribedGroupMember)
  } deriving (Show, Generic)
instance Serializable DescribedGroup

data FetchPartition = FetchPartition
  { partition         :: {-# UNPACK #-} !Int32
  , fetchOffset       :: {-# UNPACK #-} !Int64
  , partitionMaxBytes :: {-# UNPACK #-} !Int32
  } deriving (Show, Generic)
instance Serializable FetchPartition

data FetchTopic = FetchTopic
  { topic      :: !Text
  , partitions :: !(KaArray FetchPartition)
  } deriving (Show, Generic)
instance Serializable FetchTopic

data PartitionData = PartitionData
  { partitionIndex :: {-# UNPACK #-} !Int32
  , errorCode      :: {-# UNPACK #-} !ErrorCode
  , highWatermark  :: {-# UNPACK #-} !Int64
  , recordBytes    :: !NullableBytes
  } deriving (Show, Generic)
instance Serializable PartitionData

data FetchableTopicResponse = FetchableTopicResponse
  { topic      :: !Text
  , partitions :: !(KaArray PartitionData)
  } deriving (Show, Generic)
instance Serializable FetchableTopicResponse

data JoinGroupRequestProtocol = JoinGroupRequestProtocol
  { name     :: !Text
  , metadata :: !ByteString
  } deriving (Show, Generic)
instance Serializable JoinGroupRequestProtocol

data JoinGroupResponseMember = JoinGroupResponseMember
  { memberId :: !Text
  , metadata :: !ByteString
  } deriving (Show, Generic)
instance Serializable JoinGroupResponseMember

data ListedGroup = ListedGroup
  { groupId      :: !Text
  , protocolType :: !Text
  } deriving (Show, Generic)
instance Serializable ListedGroup

data ListOffsetsPartition = ListOffsetsPartition
  { partitionIndex :: {-# UNPACK #-} !Int32
  , timestamp      :: {-# UNPACK #-} !Int64
  , maxNumOffsets  :: {-# UNPACK #-} !Int32
  } deriving (Show, Generic)
instance Serializable ListOffsetsPartition

data ListOffsetsTopic = ListOffsetsTopic
  { name       :: !Text
  , partitions :: !(KaArray ListOffsetsPartition)
  } deriving (Show, Generic)
instance Serializable ListOffsetsTopic

data ListOffsetsPartitionResponse = ListOffsetsPartitionResponse
  { partitionIndex  :: {-# UNPACK #-} !Int32
  , errorCode       :: {-# UNPACK #-} !ErrorCode
  , oldStyleOffsets :: !(KaArray Int64)
  } deriving (Show, Generic)
instance Serializable ListOffsetsPartitionResponse

data ListOffsetsTopicResponse = ListOffsetsTopicResponse
  { name       :: !Text
  , partitions :: !(KaArray ListOffsetsPartitionResponse)
  } deriving (Show, Generic)
instance Serializable ListOffsetsTopicResponse

newtype MetadataRequestTopic = MetadataRequestTopic
  { name :: Text
  } deriving (Show, Generic)
instance Serializable MetadataRequestTopic

data MetadataResponseBroker = MetadataResponseBroker
  { nodeId :: {-# UNPACK #-} !Int32
  , host   :: !Text
  , port   :: {-# UNPACK #-} !Int32
  } deriving (Show, Generic)
instance Serializable MetadataResponseBroker

data MetadataResponsePartition = MetadataResponsePartition
  { errorCode      :: {-# UNPACK #-} !ErrorCode
  , partitionIndex :: {-# UNPACK #-} !Int32
  , leaderId       :: {-# UNPACK #-} !Int32
  , replicaNodes   :: !(KaArray Int32)
  , isrNodes       :: !(KaArray Int32)
  } deriving (Show, Generic)
instance Serializable MetadataResponsePartition

data MetadataResponseTopic = MetadataResponseTopic
  { errorCode  :: {-# UNPACK #-} !ErrorCode
  , name       :: !Text
  , partitions :: !(KaArray MetadataResponsePartition)
  } deriving (Show, Generic)
instance Serializable MetadataResponseTopic

data OffsetCommitRequestPartition = OffsetCommitRequestPartition
  { partitionIndex    :: {-# UNPACK #-} !Int32
  , committedOffset   :: {-# UNPACK #-} !Int64
  , committedMetadata :: !NullableString
  } deriving (Show, Generic)
instance Serializable OffsetCommitRequestPartition

data OffsetCommitRequestTopic = OffsetCommitRequestTopic
  { name       :: !Text
  , partitions :: !(KaArray OffsetCommitRequestPartition)
  } deriving (Show, Generic)
instance Serializable OffsetCommitRequestTopic

data OffsetCommitResponsePartition = OffsetCommitResponsePartition
  { partitionIndex :: {-# UNPACK #-} !Int32
  , errorCode      :: {-# UNPACK #-} !ErrorCode
  } deriving (Show, Generic)
instance Serializable OffsetCommitResponsePartition

data OffsetCommitResponseTopic = OffsetCommitResponseTopic
  { name       :: !Text
  , partitions :: !(KaArray OffsetCommitResponsePartition)
  } deriving (Show, Generic)
instance Serializable OffsetCommitResponseTopic

data OffsetFetchRequestTopic = OffsetFetchRequestTopic
  { name             :: !Text
  , partitionIndexes :: !(KaArray Int32)
  } deriving (Show, Generic)
instance Serializable OffsetFetchRequestTopic

data OffsetFetchResponsePartition = OffsetFetchResponsePartition
  { partitionIndex  :: {-# UNPACK #-} !Int32
  , committedOffset :: {-# UNPACK #-} !Int64
  , metadata        :: !NullableString
  , errorCode       :: {-# UNPACK #-} !ErrorCode
  } deriving (Show, Generic)
instance Serializable OffsetFetchResponsePartition

data OffsetFetchResponseTopic = OffsetFetchResponseTopic
  { name       :: !Text
  , partitions :: !(KaArray OffsetFetchResponsePartition)
  } deriving (Show, Generic)
instance Serializable OffsetFetchResponseTopic

data PartitionProduceData = PartitionProduceData
  { index       :: {-# UNPACK #-} !Int32
  , recordBytes :: !NullableBytes
  } deriving (Show, Generic)
instance Serializable PartitionProduceData

data TopicProduceData = TopicProduceData
  { name          :: !Text
  , partitionData :: !(KaArray PartitionProduceData)
  } deriving (Show, Generic)
instance Serializable TopicProduceData

data PartitionProduceResponse = PartitionProduceResponse
  { index      :: {-# UNPACK #-} !Int32
  , errorCode  :: {-# UNPACK #-} !ErrorCode
  , baseOffset :: {-# UNPACK #-} !Int64
  } deriving (Show, Generic)
instance Serializable PartitionProduceResponse

data TopicProduceResponse = TopicProduceResponse
  { name               :: !Text
  , partitionResponses :: !(KaArray PartitionProduceResponse)
  } deriving (Show, Generic)
instance Serializable TopicProduceResponse

data SyncGroupRequestAssignment = SyncGroupRequestAssignment
  { memberId   :: !Text
  , assignment :: !ByteString
  } deriving (Show, Generic)
instance Serializable SyncGroupRequestAssignment

-------------------------------------------------------------------------------

data ApiVersionsRequest = ApiVersionsRequest
  deriving (Show, Generic)
instance Serializable ApiVersionsRequest

data ApiVersionsResponse = ApiVersionsResponse
  { errorCode :: {-# UNPACK #-} !ErrorCode
  , apiKeys   :: !(KaArray ApiVersion)
  } deriving (Show, Generic)
instance Serializable ApiVersionsResponse

data CreateTopicsRequest = CreateTopicsRequest
  { topics    :: !(KaArray CreatableTopic)
  , timeoutMs :: {-# UNPACK #-} !Int32
  } deriving (Show, Generic)
instance Serializable CreateTopicsRequest

newtype CreateTopicsResponse = CreateTopicsResponse
  { topics :: (KaArray CreatableTopicResult)
  } deriving (Show, Generic)
instance Serializable CreateTopicsResponse

data DeleteTopicsRequest = DeleteTopicsRequest
  { topicNames :: !(KaArray Text)
  , timeoutMs  :: {-# UNPACK #-} !Int32
  } deriving (Show, Generic)
instance Serializable DeleteTopicsRequest

newtype DeleteTopicsResponse = DeleteTopicsResponse
  { responses :: (KaArray DeletableTopicResult)
  } deriving (Show, Generic)
instance Serializable DeleteTopicsResponse

newtype DescribeGroupsRequest = DescribeGroupsRequest
  { groups :: (KaArray Text)
  } deriving (Show, Generic)
instance Serializable DescribeGroupsRequest

newtype DescribeGroupsResponse = DescribeGroupsResponse
  { groups :: (KaArray DescribedGroup)
  } deriving (Show, Generic)
instance Serializable DescribeGroupsResponse

data FetchRequest = FetchRequest
  { replicaId :: {-# UNPACK #-} !Int32
  , maxWaitMs :: {-# UNPACK #-} !Int32
  , minBytes  :: {-# UNPACK #-} !Int32
  , topics    :: !(KaArray FetchTopic)
  } deriving (Show, Generic)
instance Serializable FetchRequest

newtype FetchResponse = FetchResponse
  { responses :: (KaArray FetchableTopicResponse)
  } deriving (Show, Generic)
instance Serializable FetchResponse

newtype FindCoordinatorRequest = FindCoordinatorRequest
  { key :: Text
  } deriving (Show, Generic)
instance Serializable FindCoordinatorRequest

data FindCoordinatorResponse = FindCoordinatorResponse
  { errorCode :: {-# UNPACK #-} !ErrorCode
  , nodeId    :: {-# UNPACK #-} !Int32
  , host      :: !Text
  , port      :: {-# UNPACK #-} !Int32
  } deriving (Show, Generic)
instance Serializable FindCoordinatorResponse

data HeartbeatRequest = HeartbeatRequest
  { groupId      :: !Text
  , generationId :: {-# UNPACK #-} !Int32
  , memberId     :: !Text
  } deriving (Show, Generic)
instance Serializable HeartbeatRequest

newtype HeartbeatResponse = HeartbeatResponse
  { errorCode :: ErrorCode
  } deriving (Show, Generic)
instance Serializable HeartbeatResponse

data JoinGroupRequest = JoinGroupRequest
  { groupId          :: !Text
  , sessionTimeoutMs :: {-# UNPACK #-} !Int32
  , memberId         :: !Text
  , protocolType     :: !Text
  , protocols        :: !(KaArray JoinGroupRequestProtocol)
  } deriving (Show, Generic)
instance Serializable JoinGroupRequest

data JoinGroupResponse = JoinGroupResponse
  { errorCode    :: {-# UNPACK #-} !ErrorCode
  , generationId :: {-# UNPACK #-} !Int32
  , protocolName :: !Text
  , leader       :: !Text
  , memberId     :: !Text
  , members      :: !(KaArray JoinGroupResponseMember)
  } deriving (Show, Generic)
instance Serializable JoinGroupResponse

data LeaveGroupRequest = LeaveGroupRequest
  { groupId  :: !Text
  , memberId :: !Text
  } deriving (Show, Generic)
instance Serializable LeaveGroupRequest

newtype LeaveGroupResponse = LeaveGroupResponse
  { errorCode :: ErrorCode
  } deriving (Show, Generic)
instance Serializable LeaveGroupResponse

data ListGroupsRequest = ListGroupsRequest
  deriving (Show, Generic)
instance Serializable ListGroupsRequest

data ListGroupsResponse = ListGroupsResponse
  { errorCode :: {-# UNPACK #-} !ErrorCode
  , groups    :: !(KaArray ListedGroup)
  } deriving (Show, Generic)
instance Serializable ListGroupsResponse

data ListOffsetsRequest = ListOffsetsRequest
  { replicaId :: {-# UNPACK #-} !Int32
  , topics    :: !(KaArray ListOffsetsTopic)
  } deriving (Show, Generic)
instance Serializable ListOffsetsRequest

newtype ListOffsetsResponse = ListOffsetsResponse
  { topics :: (KaArray ListOffsetsTopicResponse)
  } deriving (Show, Generic)
instance Serializable ListOffsetsResponse

newtype MetadataRequest = MetadataRequest
  { topics :: (KaArray MetadataRequestTopic)
  } deriving (Show, Generic)
instance Serializable MetadataRequest

data MetadataResponse = MetadataResponse
  { brokers :: !(KaArray MetadataResponseBroker)
  , topics  :: !(KaArray MetadataResponseTopic)
  } deriving (Show, Generic)
instance Serializable MetadataResponse

data OffsetCommitRequest = OffsetCommitRequest
  { groupId :: !Text
  , topics  :: !(KaArray OffsetCommitRequestTopic)
  } deriving (Show, Generic)
instance Serializable OffsetCommitRequest

newtype OffsetCommitResponse = OffsetCommitResponse
  { topics :: (KaArray OffsetCommitResponseTopic)
  } deriving (Show, Generic)
instance Serializable OffsetCommitResponse

data OffsetFetchRequest = OffsetFetchRequest
  { groupId :: !Text
  , topics  :: !(KaArray OffsetFetchRequestTopic)
  } deriving (Show, Generic)
instance Serializable OffsetFetchRequest

newtype OffsetFetchResponse = OffsetFetchResponse
  { topics :: (KaArray OffsetFetchResponseTopic)
  } deriving (Show, Generic)
instance Serializable OffsetFetchResponse

data ProduceRequest = ProduceRequest
  { acks      :: {-# UNPACK #-} !Int16
  , timeoutMs :: {-# UNPACK #-} !Int32
  , topicData :: !(KaArray TopicProduceData)
  } deriving (Show, Generic)
instance Serializable ProduceRequest

newtype ProduceResponse = ProduceResponse
  { responses :: (KaArray TopicProduceResponse)
  } deriving (Show, Generic)
instance Serializable ProduceResponse

data SyncGroupRequest = SyncGroupRequest
  { groupId      :: !Text
  , generationId :: {-# UNPACK #-} !Int32
  , memberId     :: !Text
  , assignments  :: !(KaArray SyncGroupRequestAssignment)
  } deriving (Show, Generic)
instance Serializable SyncGroupRequest

data SyncGroupResponse = SyncGroupResponse
  { errorCode  :: {-# UNPACK #-} !ErrorCode
  , assignment :: !ByteString
  } deriving (Show, Generic)
instance Serializable SyncGroupResponse
