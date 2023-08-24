{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE TypeFamilies          #-}

-------------------------------------------------------------------------------
-- TODO: Generate by kafka message json schema

module Kafka.Protocol.Message.StructV0 where

import           Data.ByteString               (ByteString)
import           Data.Int
import           Data.Text                     (Text)
import           GHC.Generics

import           Kafka.Protocol.Encoding
import           Kafka.Protocol.Error
import           Kafka.Protocol.Message.Common
import           Kafka.Protocol.Service

-------------------------------------------------------------------------------

data ApiVersion = ApiVersion
  { apiKey     :: {-# UNPACK #-} !ApiKey
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

-------------------------------------------------------------------------------

supportedApiVersions :: [ApiVersion]
supportedApiVersions =
  [ ApiVersion (ApiKey 0) 0 0
  , ApiVersion (ApiKey 1) 0 0
  , ApiVersion (ApiKey 2) 0 0
  , ApiVersion (ApiKey 3) 0 0
  , ApiVersion (ApiKey 8) 0 0
  , ApiVersion (ApiKey 9) 0 0
  , ApiVersion (ApiKey 10) 0 0
  , ApiVersion (ApiKey 11) 0 0
  , ApiVersion (ApiKey 12) 0 0
  , ApiVersion (ApiKey 13) 0 0
  , ApiVersion (ApiKey 14) 0 0
  , ApiVersion (ApiKey 15) 0 0
  , ApiVersion (ApiKey 16) 0 0
  , ApiVersion (ApiKey 18) 0 0
  , ApiVersion (ApiKey 19) 0 0
  , ApiVersion (ApiKey 20) 0 0
  ]

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
  type MethodInput HStreamKafkaV0 "produce" = ProduceRequest
  type MethodOutput HStreamKafkaV0 "produce" = ProduceResponse

instance HasMethodImpl HStreamKafkaV0 "fetch" where
  type MethodName HStreamKafkaV0 "fetch" = "fetch"
  type MethodKey HStreamKafkaV0 "fetch" = 1
  type MethodVersion HStreamKafkaV0 "fetch" = 0
  type MethodInput HStreamKafkaV0 "fetch" = FetchRequest
  type MethodOutput HStreamKafkaV0 "fetch" = FetchResponse

instance HasMethodImpl HStreamKafkaV0 "listOffsets" where
  type MethodName HStreamKafkaV0 "listOffsets" = "listOffsets"
  type MethodKey HStreamKafkaV0 "listOffsets" = 2
  type MethodVersion HStreamKafkaV0 "listOffsets" = 0
  type MethodInput HStreamKafkaV0 "listOffsets" = ListOffsetsRequest
  type MethodOutput HStreamKafkaV0 "listOffsets" = ListOffsetsResponse

instance HasMethodImpl HStreamKafkaV0 "metadata" where
  type MethodName HStreamKafkaV0 "metadata" = "metadata"
  type MethodKey HStreamKafkaV0 "metadata" = 3
  type MethodVersion HStreamKafkaV0 "metadata" = 0
  type MethodInput HStreamKafkaV0 "metadata" = MetadataRequest
  type MethodOutput HStreamKafkaV0 "metadata" = MetadataResponse

instance HasMethodImpl HStreamKafkaV0 "offsetCommit" where
  type MethodName HStreamKafkaV0 "offsetCommit" = "offsetCommit"
  type MethodKey HStreamKafkaV0 "offsetCommit" = 8
  type MethodVersion HStreamKafkaV0 "offsetCommit" = 0
  type MethodInput HStreamKafkaV0 "offsetCommit" = OffsetCommitRequest
  type MethodOutput HStreamKafkaV0 "offsetCommit" = OffsetCommitResponse

instance HasMethodImpl HStreamKafkaV0 "offsetFetch" where
  type MethodName HStreamKafkaV0 "offsetFetch" = "offsetFetch"
  type MethodKey HStreamKafkaV0 "offsetFetch" = 9
  type MethodVersion HStreamKafkaV0 "offsetFetch" = 0
  type MethodInput HStreamKafkaV0 "offsetFetch" = OffsetFetchRequest
  type MethodOutput HStreamKafkaV0 "offsetFetch" = OffsetFetchResponse

instance HasMethodImpl HStreamKafkaV0 "findCoordinator" where
  type MethodName HStreamKafkaV0 "findCoordinator" = "findCoordinator"
  type MethodKey HStreamKafkaV0 "findCoordinator" = 10
  type MethodVersion HStreamKafkaV0 "findCoordinator" = 0
  type MethodInput HStreamKafkaV0 "findCoordinator" = FindCoordinatorRequest
  type MethodOutput HStreamKafkaV0 "findCoordinator" = FindCoordinatorResponse

instance HasMethodImpl HStreamKafkaV0 "joinGroup" where
  type MethodName HStreamKafkaV0 "joinGroup" = "joinGroup"
  type MethodKey HStreamKafkaV0 "joinGroup" = 11
  type MethodVersion HStreamKafkaV0 "joinGroup" = 0
  type MethodInput HStreamKafkaV0 "joinGroup" = JoinGroupRequest
  type MethodOutput HStreamKafkaV0 "joinGroup" = JoinGroupResponse

instance HasMethodImpl HStreamKafkaV0 "heartbeat" where
  type MethodName HStreamKafkaV0 "heartbeat" = "heartbeat"
  type MethodKey HStreamKafkaV0 "heartbeat" = 12
  type MethodVersion HStreamKafkaV0 "heartbeat" = 0
  type MethodInput HStreamKafkaV0 "heartbeat" = HeartbeatRequest
  type MethodOutput HStreamKafkaV0 "heartbeat" = HeartbeatResponse

instance HasMethodImpl HStreamKafkaV0 "leaveGroup" where
  type MethodName HStreamKafkaV0 "leaveGroup" = "leaveGroup"
  type MethodKey HStreamKafkaV0 "leaveGroup" = 13
  type MethodVersion HStreamKafkaV0 "leaveGroup" = 0
  type MethodInput HStreamKafkaV0 "leaveGroup" = LeaveGroupRequest
  type MethodOutput HStreamKafkaV0 "leaveGroup" = LeaveGroupResponse

instance HasMethodImpl HStreamKafkaV0 "syncGroup" where
  type MethodName HStreamKafkaV0 "syncGroup" = "syncGroup"
  type MethodKey HStreamKafkaV0 "syncGroup" = 14
  type MethodVersion HStreamKafkaV0 "syncGroup" = 0
  type MethodInput HStreamKafkaV0 "syncGroup" = SyncGroupRequest
  type MethodOutput HStreamKafkaV0 "syncGroup" = SyncGroupResponse

instance HasMethodImpl HStreamKafkaV0 "describeGroups" where
  type MethodName HStreamKafkaV0 "describeGroups" = "describeGroups"
  type MethodKey HStreamKafkaV0 "describeGroups" = 15
  type MethodVersion HStreamKafkaV0 "describeGroups" = 0
  type MethodInput HStreamKafkaV0 "describeGroups" = DescribeGroupsRequest
  type MethodOutput HStreamKafkaV0 "describeGroups" = DescribeGroupsResponse

instance HasMethodImpl HStreamKafkaV0 "listGroups" where
  type MethodName HStreamKafkaV0 "listGroups" = "listGroups"
  type MethodKey HStreamKafkaV0 "listGroups" = 16
  type MethodVersion HStreamKafkaV0 "listGroups" = 0
  type MethodInput HStreamKafkaV0 "listGroups" = ListGroupsRequest
  type MethodOutput HStreamKafkaV0 "listGroups" = ListGroupsResponse

instance HasMethodImpl HStreamKafkaV0 "apiVersions" where
  type MethodName HStreamKafkaV0 "apiVersions" = "apiVersions"
  type MethodKey HStreamKafkaV0 "apiVersions" = 18
  type MethodVersion HStreamKafkaV0 "apiVersions" = 0
  type MethodInput HStreamKafkaV0 "apiVersions" = ApiVersionsRequest
  type MethodOutput HStreamKafkaV0 "apiVersions" = ApiVersionsResponse

instance HasMethodImpl HStreamKafkaV0 "createTopics" where
  type MethodName HStreamKafkaV0 "createTopics" = "createTopics"
  type MethodKey HStreamKafkaV0 "createTopics" = 19
  type MethodVersion HStreamKafkaV0 "createTopics" = 0
  type MethodInput HStreamKafkaV0 "createTopics" = CreateTopicsRequest
  type MethodOutput HStreamKafkaV0 "createTopics" = CreateTopicsResponse

instance HasMethodImpl HStreamKafkaV0 "deleteTopics" where
  type MethodName HStreamKafkaV0 "deleteTopics" = "deleteTopics"
  type MethodKey HStreamKafkaV0 "deleteTopics" = 20
  type MethodVersion HStreamKafkaV0 "deleteTopics" = 0
  type MethodInput HStreamKafkaV0 "deleteTopics" = DeleteTopicsRequest
  type MethodOutput HStreamKafkaV0 "deleteTopics" = DeleteTopicsResponse
