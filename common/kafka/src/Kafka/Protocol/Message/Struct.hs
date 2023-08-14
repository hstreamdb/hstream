{-# LANGUAGE DuplicateRecordFields #-}

module Kafka.Protocol.Message.Struct where

import           Data.ByteString (ByteString)
import           Data.Int
import           Data.Text       (Text)
import           Data.Vector     (Vector)

-- TODO
data Records = Records
  deriving (Show)

-------------------------------------------------------------------------------
-- TODO: Generate by kafka message json schema

data ApiVersion = ApiVersion
  { apiKey     :: {-# UNPACK #-} !Int16
  , minVersion :: {-# UNPACK #-} !Int16
  , maxVersion :: {-# UNPACK #-} !Int16
  } deriving (Show)

data CreatableReplicaAssignment = CreatableReplicaAssignment
  { partitionIndex :: {-# UNPACK #-} !Int32
  , brokerIds      :: Maybe (Vector Int32)
  } deriving (Show)

data CreateableTopicConfig = CreateableTopicConfig
  { name  :: !Text
  , value :: !Text
  } deriving (Show)

data CreatableTopic = CreatableTopic
  { name              :: !Text
  , numPartitions     :: {-# UNPACK #-} !Int32
  , replicationFactor :: {-# UNPACK #-} !Int16
  , assignments       :: Maybe (Vector CreatableReplicaAssignment)
  , configs           :: Maybe (Vector CreateableTopicConfig)
  } deriving (Show)

data CreatableTopicResult = CreatableTopicResult
  { name      :: !Text
  , errorCode :: {-# UNPACK #-} !Int16
  } deriving (Show)

data DeletableTopicResult = DeletableTopicResult
  { name      :: !Text
  , errorCode :: {-# UNPACK #-} !Int16
  } deriving (Show)

data DescribedGroupMember = DescribedGroupMember
  { memberId         :: !Text
  , clientId         :: !Text
  , clientHost       :: !Text
  , memberMetadata   :: !ByteString
  , memberAssignment :: !ByteString
  } deriving (Show)

data DescribedGroup = DescribedGroup
  { errorCode    :: {-# UNPACK #-} !Int16
  , groupId      :: !Text
  , groupState   :: !Text
  , protocolType :: !Text
  , protocolData :: !Text
  , members      :: Maybe (Vector DescribedGroupMember)
  } deriving (Show)

data FetchPartition = FetchPartition
  { partition         :: {-# UNPACK #-} !Int32
  , fetchOffset       :: {-# UNPACK #-} !Int64
  , partitionMaxBytes :: {-# UNPACK #-} !Int32
  } deriving (Show)

data FetchTopic = FetchTopic
  { topic      :: !Text
  , partitions :: Maybe (Vector FetchPartition)
  } deriving (Show)

data PartitionData = PartitionData
  { partitionIndex :: {-# UNPACK #-} !Int32
  , errorCode      :: {-# UNPACK #-} !Int16
  , highWatermark  :: {-# UNPACK #-} !Int64
  , records        :: !Records
  } deriving (Show)

data FetchableTopicResponse = FetchableTopicResponse
  { topic      :: !Text
  , partitions :: Maybe (Vector PartitionData)
  } deriving (Show)

data JoinGroupRequestProtocol = JoinGroupRequestProtocol
  { name     :: !Text
  , metadata :: !ByteString
  } deriving (Show)

data JoinGroupResponseMember = JoinGroupResponseMember
  { memberId :: !Text
  , metadata :: !ByteString
  } deriving (Show)

data ListedGroup = ListedGroup
  { groupId      :: !Text
  , protocolType :: !Text
  } deriving (Show)

data ListOffsetsPartition = ListOffsetsPartition
  { partitionIndex :: {-# UNPACK #-} !Int32
  , timestamp      :: {-# UNPACK #-} !Int64
  , maxNumOffsets  :: {-# UNPACK #-} !Int32
  } deriving (Show)

data ListOffsetsTopic = ListOffsetsTopic
  { name       :: !Text
  , partitions :: Maybe (Vector ListOffsetsPartition)
  } deriving (Show)

data ListOffsetsPartitionResponse = ListOffsetsPartitionResponse
  { partitionIndex  :: {-# UNPACK #-} !Int32
  , errorCode       :: {-# UNPACK #-} !Int16
  , oldStyleOffsets :: Maybe (Vector Int64)
  } deriving (Show)

data ListOffsetsTopicResponse = ListOffsetsTopicResponse
  { name       :: !Text
  , partitions :: Maybe (Vector ListOffsetsPartitionResponse)
  } deriving (Show)

newtype MetadataRequestTopic = MetadataRequestTopic
  { name :: Text
  } deriving (Show)

data MetadataResponseBroker = MetadataResponseBroker
  { nodeId :: {-# UNPACK #-} !Int32
  , host   :: !Text
  , port   :: {-# UNPACK #-} !Int32
  } deriving (Show)

data MetadataResponsePartition = MetadataResponsePartition
  { errorCode      :: {-# UNPACK #-} !Int16
  , partitionIndex :: {-# UNPACK #-} !Int32
  , leaderId       :: {-# UNPACK #-} !Int32
  , replicaNodes   :: Maybe (Vector Int32)
  , isrNodes       :: Maybe (Vector Int32)
  } deriving (Show)

data MetadataResponseTopic = MetadataResponseTopic
  { errorCode  :: {-# UNPACK #-} !Int16
  , name       :: !Text
  , partitions :: Maybe (Vector MetadataResponsePartition)
  } deriving (Show)

data OffsetCommitRequestPartition = OffsetCommitRequestPartition
  { partitionIndex    :: {-# UNPACK #-} !Int32
  , committedOffset   :: {-# UNPACK #-} !Int64
  , committedMetadata :: !Text
  } deriving (Show)

data OffsetCommitRequestTopic = OffsetCommitRequestTopic
  { name       :: !Text
  , partitions :: Maybe (Vector OffsetCommitRequestPartition)
  } deriving (Show)

data OffsetCommitResponsePartition = OffsetCommitResponsePartition
  { partitionIndex :: {-# UNPACK #-} !Int32
  , errorCode      :: {-# UNPACK #-} !Int16
  } deriving (Show)

data OffsetCommitResponseTopic = OffsetCommitResponseTopic
  { name       :: !Text
  , partitions :: Maybe (Vector OffsetCommitResponsePartition)
  } deriving (Show)

data OffsetFetchRequestTopic = OffsetFetchRequestTopic
  { name             :: !Text
  , partitionIndexes :: Maybe (Vector Int32)
  } deriving (Show)

data OffsetFetchResponsePartition = OffsetFetchResponsePartition
  { partitionIndex  :: {-# UNPACK #-} !Int32
  , committedOffset :: {-# UNPACK #-} !Int64
  , metadata        :: !Text
  , errorCode       :: {-# UNPACK #-} !Int16
  } deriving (Show)

data OffsetFetchResponseTopic = OffsetFetchResponseTopic
  { name       :: !Text
  , partitions :: Maybe (Vector OffsetFetchResponsePartition)
  } deriving (Show)

data PartitionProduceData = PartitionProduceData
  { index   :: {-# UNPACK #-} !Int32
  , records :: !Records
  } deriving (Show)

data TopicProduceData = TopicProduceData
  { name          :: !Text
  , partitionData :: Maybe (Vector PartitionProduceData)
  } deriving (Show)

data PartitionProduceResponse = PartitionProduceResponse
  { index      :: {-# UNPACK #-} !Int32
  , errorCode  :: {-# UNPACK #-} !Int16
  , baseOffset :: {-# UNPACK #-} !Int64
  } deriving (Show)

data TopicProduceResponse = TopicProduceResponse
  { name               :: !Text
  , partitionResponses :: Maybe (Vector PartitionProduceResponse)
  } deriving (Show)

data SyncGroupRequestAssignment = SyncGroupRequestAssignment
  { memberId   :: !Text
  , assignment :: !ByteString
  } deriving (Show)

-------------------------------------------------------------------------------

data ApiVersionsRequest = ApiVersionsRequest
  deriving (Show)

data ApiVersionsResponse = ApiVersionsResponse
  { errorCode :: {-# UNPACK #-} !Int16
  , apiKeys   :: Maybe (Vector ApiVersion)
  } deriving (Show)

data CreateTopicsRequest = CreateTopicsRequest
  { topics    :: Maybe (Vector CreatableTopic)
  , timeoutMs :: {-# UNPACK #-} !Int32
  } deriving (Show)

newtype CreateTopicsResponse = CreateTopicsResponse
  { topics :: Maybe (Vector CreatableTopicResult)
  } deriving (Show)

data DeleteTopicsRequest = DeleteTopicsRequest
  { topicNames :: Maybe (Vector Text)
  , timeoutMs  :: {-# UNPACK #-} !Int32
  } deriving (Show)

newtype DeleteTopicsResponse = DeleteTopicsResponse
  { responses :: Maybe (Vector DeletableTopicResult)
  } deriving (Show)

newtype DescribeGroupsRequest = DescribeGroupsRequest
  { groups :: Maybe (Vector Text)
  } deriving (Show)

newtype DescribeGroupsResponse = DescribeGroupsResponse
  { groups :: Maybe (Vector DescribedGroup)
  } deriving (Show)

data FetchRequest = FetchRequest
  { replicaId :: {-# UNPACK #-} !Int32
  , maxWaitMs :: {-# UNPACK #-} !Int32
  , minBytes  :: {-# UNPACK #-} !Int32
  , topics    :: Maybe (Vector FetchTopic)
  } deriving (Show)

newtype FetchResponse = FetchResponse
  { responses :: Maybe (Vector FetchableTopicResponse)
  } deriving (Show)

newtype FindCoordinatorRequest = FindCoordinatorRequest
  { key :: Text
  } deriving (Show)

data FindCoordinatorResponse = FindCoordinatorResponse
  { errorCode :: {-# UNPACK #-} !Int16
  , nodeId    :: {-# UNPACK #-} !Int32
  , host      :: !Text
  , port      :: {-# UNPACK #-} !Int32
  } deriving (Show)

data HeartbeatRequest = HeartbeatRequest
  { groupId      :: !Text
  , generationId :: {-# UNPACK #-} !Int32
  , memberId     :: !Text
  } deriving (Show)

newtype HeartbeatResponse = HeartbeatResponse
  { errorCode :: Int16
  } deriving (Show)

data JoinGroupRequest = JoinGroupRequest
  { groupId          :: !Text
  , sessionTimeoutMs :: {-# UNPACK #-} !Int32
  , memberId         :: !Text
  , protocolType     :: !Text
  , protocols        :: Maybe (Vector JoinGroupRequestProtocol)
  } deriving (Show)

data JoinGroupResponse = JoinGroupResponse
  { errorCode    :: {-# UNPACK #-} !Int16
  , generationId :: {-# UNPACK #-} !Int32
  , protocolName :: !Text
  , leader       :: !Text
  , memberId     :: !Text
  , members      :: Maybe (Vector JoinGroupResponseMember)
  } deriving (Show)

data LeaveGroupRequest = LeaveGroupRequest
  { groupId  :: !Text
  , memberId :: !Text
  } deriving (Show)

newtype LeaveGroupResponse = LeaveGroupResponse
  { errorCode :: Int16
  } deriving (Show)

data ListGroupsRequest = ListGroupsRequest
  deriving (Show)

data ListGroupsResponse = ListGroupsResponse
  { errorCode :: {-# UNPACK #-} !Int16
  , groups    :: Maybe (Vector ListedGroup)
  } deriving (Show)

data ListOffsetsRequest = ListOffsetsRequest
  { replicaId :: {-# UNPACK #-} !Int32
  , topics    :: Maybe (Vector ListOffsetsTopic)
  } deriving (Show)

newtype ListOffsetsResponse = ListOffsetsResponse
  { topics :: Maybe (Vector ListOffsetsTopicResponse)
  } deriving (Show)

newtype MetadataRequest = MetadataRequest
  { topics :: Maybe (Vector MetadataRequestTopic)
  } deriving (Show)

data MetadataResponse = MetadataResponse
  { brokers :: Maybe (Vector MetadataResponseBroker)
  , topics  :: Maybe (Vector MetadataResponseTopic)
  } deriving (Show)

data OffsetCommitRequest = OffsetCommitRequest
  { groupId :: !Text
  , topics  :: Maybe (Vector OffsetCommitRequestTopic)
  } deriving (Show)

newtype OffsetCommitResponse = OffsetCommitResponse
  { topics :: Maybe (Vector OffsetCommitResponseTopic)
  } deriving (Show)

data OffsetFetchRequest = OffsetFetchRequest
  { groupId :: !Text
  , topics  :: Maybe (Vector OffsetFetchRequestTopic)
  } deriving (Show)

newtype OffsetFetchResponse = OffsetFetchResponse
  { topics :: Maybe (Vector OffsetFetchResponseTopic)
  } deriving (Show)

data ProduceRequest = ProduceRequest
  { acks      :: {-# UNPACK #-} !Int16
  , timeoutMs :: {-# UNPACK #-} !Int32
  , topicData :: Maybe (Vector TopicProduceData)
  } deriving (Show)

newtype ProduceResponse = ProduceResponse
  { responses :: Maybe (Vector TopicProduceResponse)
  } deriving (Show)

data SyncGroupRequest = SyncGroupRequest
  { groupId      :: !Text
  , generationId :: {-# UNPACK #-} !Int32
  , memberId     :: !Text
  , assignments  :: Maybe (Vector SyncGroupRequestAssignment)
  } deriving (Show)

data SyncGroupResponse = SyncGroupResponse
  { errorCode  :: {-# UNPACK #-} !Int16
  , assignment :: !ByteString
  } deriving (Show)
