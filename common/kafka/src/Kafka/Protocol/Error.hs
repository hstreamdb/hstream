{-# LANGUAGE PatternSynonyms #-}

-- Ref: https://kafka.apache.org/protocol.html#protocol_error_codes

module Kafka.Protocol.Error
  ( ErrorCode
  , doesErrorRetriable
  , pattern UNKNOWN_SERVER_ERROR
  , pattern NONE
  , pattern OFFSET_OUT_OF_RANGE
  , pattern CORRUPT_MESSAGE
  , pattern UNKNOWN_TOPIC_OR_PARTITION
  , pattern INVALID_FETCH_SIZE
  , pattern LEADER_NOT_AVAILABLE
  , pattern NOT_LEADER_OR_FOLLOWER
  , pattern REQUEST_TIMED_OUT
  , pattern BROKER_NOT_AVAILABLE
  , pattern REPLICA_NOT_AVAILABLE
  , pattern MESSAGE_TOO_LARGE
  , pattern STALE_CONTROLLER_EPOCH
  , pattern OFFSET_METADATA_TOO_LARGE
  , pattern NETWORK_EXCEPTION
  , pattern COORDINATOR_LOAD_IN_PROGRESS
  , pattern COORDINATOR_NOT_AVAILABLE
  , pattern NOT_COORDINATOR
  , pattern INVALID_TOPIC_EXCEPTION
  , pattern RECORD_LIST_TOO_LARGE
  , pattern NOT_ENOUGH_REPLICAS
  , pattern NOT_ENOUGH_REPLICAS_AFTER_APPEND
  , pattern INVALID_REQUIRED_ACKS
  , pattern ILLEGAL_GENERATION
  , pattern INCONSISTENT_GROUP_PROTOCOL
  , pattern INVALID_GROUP_ID
  , pattern UNKNOWN_MEMBER_ID
  , pattern INVALID_SESSION_TIMEOUT
  , pattern REBALANCE_IN_PROGRESS
  , pattern INVALID_COMMIT_OFFSET_SIZE
  , pattern TOPIC_AUTHORIZATION_FAILED
  , pattern GROUP_AUTHORIZATION_FAILED
  , pattern CLUSTER_AUTHORIZATION_FAILED
  , pattern INVALID_TIMESTAMP
  , pattern UNSUPPORTED_SASL_MECHANISM
  , pattern ILLEGAL_SASL_STATE
  , pattern UNSUPPORTED_VERSION
  , pattern TOPIC_ALREADY_EXISTS
  , pattern INVALID_PARTITIONS
  , pattern INVALID_REPLICATION_FACTOR
  , pattern INVALID_REPLICA_ASSIGNMENT
  , pattern INVALID_CONFIG
  , pattern NOT_CONTROLLER
  , pattern INVALID_REQUEST
  , pattern UNSUPPORTED_FOR_MESSAGE_FORMAT
  , pattern POLICY_VIOLATION
  , pattern OUT_OF_ORDER_SEQUENCE_NUMBER
  , pattern DUPLICATE_SEQUENCE_NUMBER
  , pattern INVALID_PRODUCER_EPOCH
  , pattern INVALID_TXN_STATE
  , pattern INVALID_PRODUCER_ID_MAPPING
  , pattern INVALID_TRANSACTION_TIMEOUT
  , pattern CONCURRENT_TRANSACTIONS
  , pattern TRANSACTION_COORDINATOR_FENCED
  , pattern TRANSACTIONAL_ID_AUTHORIZATION_FAILED
  , pattern SECURITY_DISABLED
  , pattern OPERATION_NOT_ATTEMPTED
  , pattern KAFKA_STORAGE_ERROR
  , pattern LOG_DIR_NOT_FOUND
  , pattern SASL_AUTHENTICATION_FAILED
  , pattern UNKNOWN_PRODUCER_ID
  , pattern REASSIGNMENT_IN_PROGRESS
  , pattern DELEGATION_TOKEN_AUTH_DISABLED
  , pattern DELEGATION_TOKEN_NOT_FOUND
  , pattern DELEGATION_TOKEN_OWNER_MISMATCH
  , pattern DELEGATION_TOKEN_REQUEST_NOT_ALLOWED
  , pattern DELEGATION_TOKEN_AUTHORIZATION_FAILED
  , pattern DELEGATION_TOKEN_EXPIRED
  , pattern INVALID_PRINCIPAL_TYPE
  , pattern NON_EMPTY_GROUP
  , pattern GROUP_ID_NOT_FOUND
  , pattern FETCH_SESSION_ID_NOT_FOUND
  , pattern INVALID_FETCH_SESSION_EPOCH
  , pattern LISTENER_NOT_FOUND
  , pattern TOPIC_DELETION_DISABLED
  , pattern FENCED_LEADER_EPOCH
  , pattern UNKNOWN_LEADER_EPOCH
  , pattern UNSUPPORTED_COMPRESSION_TYPE
  , pattern STALE_BROKER_EPOCH
  , pattern OFFSET_NOT_AVAILABLE
  , pattern MEMBER_ID_REQUIRED
  , pattern PREFERRED_LEADER_NOT_AVAILABLE
  , pattern GROUP_MAX_SIZE_REACHED
  , pattern FENCED_INSTANCE_ID
  , pattern ELIGIBLE_LEADERS_NOT_AVAILABLE
  , pattern ELECTION_NOT_NEEDED
  , pattern NO_REASSIGNMENT_IN_PROGRESS
  , pattern GROUP_SUBSCRIBED_TO_TOPIC
  , pattern INVALID_RECORD
  , pattern UNSTABLE_OFFSET_COMMIT
  , pattern THROTTLING_QUOTA_EXCEEDED
  , pattern PRODUCER_FENCED
  , pattern RESOURCE_NOT_FOUND
  , pattern DUPLICATE_RESOURCE
  , pattern UNACCEPTABLE_CREDENTIAL
  , pattern INCONSISTENT_VOTER_SET
  , pattern INVALID_UPDATE_VERSION
  , pattern FEATURE_UPDATE_FAILED
  , pattern PRINCIPAL_DESERIALIZATION_FAILURE
  , pattern SNAPSHOT_NOT_FOUND
  , pattern POSITION_OUT_OF_RANGE
  , pattern UNKNOWN_TOPIC_ID
  , pattern DUPLICATE_BROKER_REGISTRATION
  , pattern BROKER_ID_NOT_REGISTERED
  , pattern INCONSISTENT_TOPIC_ID
  , pattern INCONSISTENT_CLUSTER_ID
  , pattern TRANSACTIONAL_ID_NOT_FOUND
  , pattern FETCH_SESSION_TOPIC_ID_ERROR
  , pattern INELIGIBLE_REPLICA
  , pattern NEW_LEADER_ELECTED
  , pattern OFFSET_MOVED_TO_TIERED_STORAGE
  , pattern FENCED_MEMBER_EPOCH
  , pattern UNRELEASED_INSTANCE_ID
  , pattern UNSUPPORTED_ASSIGNOR
  ) where

import           Data.Int                (Int16)

import           Kafka.Protocol.Encoding (Serializable)

-------------------------------------------------------------------------------

newtype ErrorCode = ErrorCode Int16
  deriving (Show)
  deriving newtype (Eq, Num, Serializable)

{-# COMPLETE
      UNKNOWN_SERVER_ERROR
    , NONE
    , OFFSET_OUT_OF_RANGE
    , CORRUPT_MESSAGE
    , UNKNOWN_TOPIC_OR_PARTITION
    , INVALID_FETCH_SIZE
    , LEADER_NOT_AVAILABLE
    , NOT_LEADER_OR_FOLLOWER
    , REQUEST_TIMED_OUT
    , BROKER_NOT_AVAILABLE
    , REPLICA_NOT_AVAILABLE
    , MESSAGE_TOO_LARGE
    , STALE_CONTROLLER_EPOCH
    , OFFSET_METADATA_TOO_LARGE
    , NETWORK_EXCEPTION
    , COORDINATOR_LOAD_IN_PROGRESS
    , COORDINATOR_NOT_AVAILABLE
    , NOT_COORDINATOR
    , INVALID_TOPIC_EXCEPTION
    , RECORD_LIST_TOO_LARGE
    , NOT_ENOUGH_REPLICAS
    , NOT_ENOUGH_REPLICAS_AFTER_APPEND
    , INVALID_REQUIRED_ACKS
    , ILLEGAL_GENERATION
    , INCONSISTENT_GROUP_PROTOCOL
    , INVALID_GROUP_ID
    , UNKNOWN_MEMBER_ID
    , INVALID_SESSION_TIMEOUT
    , REBALANCE_IN_PROGRESS
    , INVALID_COMMIT_OFFSET_SIZE
    , TOPIC_AUTHORIZATION_FAILED
    , GROUP_AUTHORIZATION_FAILED
    , CLUSTER_AUTHORIZATION_FAILED
    , INVALID_TIMESTAMP
    , UNSUPPORTED_SASL_MECHANISM
    , ILLEGAL_SASL_STATE
    , UNSUPPORTED_VERSION
    , TOPIC_ALREADY_EXISTS
    , INVALID_PARTITIONS
    , INVALID_REPLICATION_FACTOR
    , INVALID_REPLICA_ASSIGNMENT
    , INVALID_CONFIG
    , NOT_CONTROLLER
    , INVALID_REQUEST
    , UNSUPPORTED_FOR_MESSAGE_FORMAT
    , POLICY_VIOLATION
    , OUT_OF_ORDER_SEQUENCE_NUMBER
    , DUPLICATE_SEQUENCE_NUMBER
    , INVALID_PRODUCER_EPOCH
    , INVALID_TXN_STATE
    , INVALID_PRODUCER_ID_MAPPING
    , INVALID_TRANSACTION_TIMEOUT
    , CONCURRENT_TRANSACTIONS
    , TRANSACTION_COORDINATOR_FENCED
    , TRANSACTIONAL_ID_AUTHORIZATION_FAILED
    , SECURITY_DISABLED
    , OPERATION_NOT_ATTEMPTED
    , KAFKA_STORAGE_ERROR
    , LOG_DIR_NOT_FOUND
    , SASL_AUTHENTICATION_FAILED
    , UNKNOWN_PRODUCER_ID
    , REASSIGNMENT_IN_PROGRESS
    , DELEGATION_TOKEN_AUTH_DISABLED
    , DELEGATION_TOKEN_NOT_FOUND
    , DELEGATION_TOKEN_OWNER_MISMATCH
    , DELEGATION_TOKEN_REQUEST_NOT_ALLOWED
    , DELEGATION_TOKEN_AUTHORIZATION_FAILED
    , DELEGATION_TOKEN_EXPIRED
    , INVALID_PRINCIPAL_TYPE
    , NON_EMPTY_GROUP
    , GROUP_ID_NOT_FOUND
    , FETCH_SESSION_ID_NOT_FOUND
    , INVALID_FETCH_SESSION_EPOCH
    , LISTENER_NOT_FOUND
    , TOPIC_DELETION_DISABLED
    , FENCED_LEADER_EPOCH
    , UNKNOWN_LEADER_EPOCH
    , UNSUPPORTED_COMPRESSION_TYPE
    , STALE_BROKER_EPOCH
    , OFFSET_NOT_AVAILABLE
    , MEMBER_ID_REQUIRED
    , PREFERRED_LEADER_NOT_AVAILABLE
    , GROUP_MAX_SIZE_REACHED
    , FENCED_INSTANCE_ID
    , ELIGIBLE_LEADERS_NOT_AVAILABLE
    , ELECTION_NOT_NEEDED
    , NO_REASSIGNMENT_IN_PROGRESS
    , GROUP_SUBSCRIBED_TO_TOPIC
    , INVALID_RECORD
    , UNSTABLE_OFFSET_COMMIT
    , THROTTLING_QUOTA_EXCEEDED
    , PRODUCER_FENCED
    , RESOURCE_NOT_FOUND
    , DUPLICATE_RESOURCE
    , UNACCEPTABLE_CREDENTIAL
    , INCONSISTENT_VOTER_SET
    , INVALID_UPDATE_VERSION
    , FEATURE_UPDATE_FAILED
    , PRINCIPAL_DESERIALIZATION_FAILURE
    , SNAPSHOT_NOT_FOUND
    , POSITION_OUT_OF_RANGE
    , UNKNOWN_TOPIC_ID
    , DUPLICATE_BROKER_REGISTRATION
    , BROKER_ID_NOT_REGISTERED
    , INCONSISTENT_TOPIC_ID
    , INCONSISTENT_CLUSTER_ID
    , TRANSACTIONAL_ID_NOT_FOUND
    , FETCH_SESSION_TOPIC_ID_ERROR
    , INELIGIBLE_REPLICA
    , NEW_LEADER_ELECTED
    , OFFSET_MOVED_TO_TIERED_STORAGE
    , FENCED_MEMBER_EPOCH
    , UNRELEASED_INSTANCE_ID
    , UNSUPPORTED_ASSIGNOR
  #-}

-- | The server experienced an unexpected error when processing the request.
pattern UNKNOWN_SERVER_ERROR :: ErrorCode
pattern UNKNOWN_SERVER_ERROR = (-1)

pattern NONE :: ErrorCode
pattern NONE = 0

-- | The requested offset is not within the range of offsets maintained by the
-- server.
pattern OFFSET_OUT_OF_RANGE :: ErrorCode
pattern OFFSET_OUT_OF_RANGE = 1

-- | This message has failed its CRC checksum, exceeds the valid size, has a
-- null key for a compacted topic, or is otherwise corrupt.
pattern CORRUPT_MESSAGE :: ErrorCode
pattern CORRUPT_MESSAGE = 2

-- | This server does not host this topic-partition.
pattern UNKNOWN_TOPIC_OR_PARTITION :: ErrorCode
pattern UNKNOWN_TOPIC_OR_PARTITION = 3

-- | The requested fetch size is invalid.
pattern INVALID_FETCH_SIZE :: ErrorCode
pattern INVALID_FETCH_SIZE = 4

-- | There is no leader for this topic-partition as we are in the middle of a
-- leadership election.
pattern LEADER_NOT_AVAILABLE :: ErrorCode
pattern LEADER_NOT_AVAILABLE = 5

-- | For requests intended only for the leader, this error indicates that the
-- broker is not the current leader. For requests intended for any replica,
-- this error indicates that the broker is not a replica of the topic
-- partition.
pattern NOT_LEADER_OR_FOLLOWER :: ErrorCode
pattern NOT_LEADER_OR_FOLLOWER = 6

-- | The request timed out.
pattern REQUEST_TIMED_OUT :: ErrorCode
pattern REQUEST_TIMED_OUT = 7

-- | The broker is not available.
pattern BROKER_NOT_AVAILABLE :: ErrorCode
pattern BROKER_NOT_AVAILABLE = 8

-- | The replica is not available for the requested topic-partition.
-- Produce/Fetch requests and other requests intended only for the leader or
-- follower return NOT_LEADER_OR_FOLLOWER if the broker is not a replica of the
-- topic-partition.
pattern REPLICA_NOT_AVAILABLE :: ErrorCode
pattern REPLICA_NOT_AVAILABLE = 9

-- | The request included a message larger than the max message size the server
-- will accept.
pattern MESSAGE_TOO_LARGE :: ErrorCode
pattern MESSAGE_TOO_LARGE = 10

-- | The controller moved to another broker.
pattern STALE_CONTROLLER_EPOCH :: ErrorCode
pattern STALE_CONTROLLER_EPOCH = 11

-- | The metadata field of the offset request was too large.
pattern OFFSET_METADATA_TOO_LARGE :: ErrorCode
pattern OFFSET_METADATA_TOO_LARGE = 12

-- | The server disconnected before a response was received.
pattern NETWORK_EXCEPTION :: ErrorCode
pattern NETWORK_EXCEPTION = 13

-- | The coordinator is loading and hence can't process requests.
pattern COORDINATOR_LOAD_IN_PROGRESS :: ErrorCode
pattern COORDINATOR_LOAD_IN_PROGRESS = 14

-- | The coordinator is not available.
pattern COORDINATOR_NOT_AVAILABLE :: ErrorCode
pattern COORDINATOR_NOT_AVAILABLE = 15

-- | This is not the correct coordinator.
pattern NOT_COORDINATOR :: ErrorCode
pattern NOT_COORDINATOR = 16

-- | The request attempted to perform an operation on an invalid topic.
pattern INVALID_TOPIC_EXCEPTION :: ErrorCode
pattern INVALID_TOPIC_EXCEPTION = 17

-- | The request included message batch larger than the configured segment size
-- on the server.
pattern RECORD_LIST_TOO_LARGE :: ErrorCode
pattern RECORD_LIST_TOO_LARGE = 18

-- | Messages are rejected since there are fewer in-sync replicas than
-- required.
pattern NOT_ENOUGH_REPLICAS :: ErrorCode
pattern NOT_ENOUGH_REPLICAS = 19

-- | Messages are written to the log, but to fewer in-sync replicas than
-- required.
pattern NOT_ENOUGH_REPLICAS_AFTER_APPEND :: ErrorCode
pattern NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20

-- | Produce request specified an invalid value for required acks.
pattern INVALID_REQUIRED_ACKS :: ErrorCode
pattern INVALID_REQUIRED_ACKS = 21

-- | Specified group generation id is not valid.
pattern ILLEGAL_GENERATION :: ErrorCode
pattern ILLEGAL_GENERATION = 22

-- | The group member's supported protocols are incompatible with those of
-- existing members or first group member tried to join with empty protocol
-- type or empty protocol list.
pattern INCONSISTENT_GROUP_PROTOCOL :: ErrorCode
pattern INCONSISTENT_GROUP_PROTOCOL = 23

-- | The configured groupId is invalid.
pattern INVALID_GROUP_ID :: ErrorCode
pattern INVALID_GROUP_ID = 24

-- | The coordinator is not aware of this member.
pattern UNKNOWN_MEMBER_ID :: ErrorCode
pattern UNKNOWN_MEMBER_ID = 25

-- | The session timeout is not within the range allowed by the broker (as
-- configured by group.min.session.timeout.ms and
-- group.max.session.timeout.ms).
pattern INVALID_SESSION_TIMEOUT :: ErrorCode
pattern INVALID_SESSION_TIMEOUT = 26

-- | The group is rebalancing, so a rejoin is needed.
pattern REBALANCE_IN_PROGRESS :: ErrorCode
pattern REBALANCE_IN_PROGRESS = 27

-- | The committing offset data size is not valid.
pattern INVALID_COMMIT_OFFSET_SIZE :: ErrorCode
pattern INVALID_COMMIT_OFFSET_SIZE = 28

-- | Topic authorization failed.
pattern TOPIC_AUTHORIZATION_FAILED :: ErrorCode
pattern TOPIC_AUTHORIZATION_FAILED = 29

-- | Group authorization failed.
pattern GROUP_AUTHORIZATION_FAILED :: ErrorCode
pattern GROUP_AUTHORIZATION_FAILED = 30

-- | Cluster authorization failed.
pattern CLUSTER_AUTHORIZATION_FAILED :: ErrorCode
pattern CLUSTER_AUTHORIZATION_FAILED = 31

-- | The timestamp of the message is out of acceptable range.
pattern INVALID_TIMESTAMP :: ErrorCode
pattern INVALID_TIMESTAMP = 32

-- | The broker does not support the requested SASL mechanism.
pattern UNSUPPORTED_SASL_MECHANISM :: ErrorCode
pattern UNSUPPORTED_SASL_MECHANISM = 33

-- | Request is not valid given the current SASL state.
pattern ILLEGAL_SASL_STATE :: ErrorCode
pattern ILLEGAL_SASL_STATE = 34

-- | The version of API is not supported.
pattern UNSUPPORTED_VERSION :: ErrorCode
pattern UNSUPPORTED_VERSION = 35

-- | Topic with this name already exists.
pattern TOPIC_ALREADY_EXISTS :: ErrorCode
pattern TOPIC_ALREADY_EXISTS = 36

-- | Number of partitions is below 1.
pattern INVALID_PARTITIONS :: ErrorCode
pattern INVALID_PARTITIONS = 37

-- | Replication factor is below 1 or larger than the number of available
-- brokers.
pattern INVALID_REPLICATION_FACTOR :: ErrorCode
pattern INVALID_REPLICATION_FACTOR = 38

-- | Replica assignment is invalid.
pattern INVALID_REPLICA_ASSIGNMENT :: ErrorCode
pattern INVALID_REPLICA_ASSIGNMENT = 39

-- | Configuration is invalid.
pattern INVALID_CONFIG :: ErrorCode
pattern INVALID_CONFIG = 40

-- | This is not the correct controller for this cluster.
pattern NOT_CONTROLLER :: ErrorCode
pattern NOT_CONTROLLER = 41

-- | This most likely occurs because of a request being malformed by the client
-- library or the message was sent to an incompatible broker. See the broker
-- logs for more details.
pattern INVALID_REQUEST :: ErrorCode
pattern INVALID_REQUEST = 42

-- | The message format version on the broker does not support the request.
pattern UNSUPPORTED_FOR_MESSAGE_FORMAT :: ErrorCode
pattern UNSUPPORTED_FOR_MESSAGE_FORMAT = 43

-- | Request parameters do not satisfy the configured policy.
pattern POLICY_VIOLATION :: ErrorCode
pattern POLICY_VIOLATION = 44

-- | The broker received an out of order sequence number.
pattern OUT_OF_ORDER_SEQUENCE_NUMBER :: ErrorCode
pattern OUT_OF_ORDER_SEQUENCE_NUMBER = 45

-- | The broker received a duplicate sequence number.
pattern DUPLICATE_SEQUENCE_NUMBER :: ErrorCode
pattern DUPLICATE_SEQUENCE_NUMBER = 46

-- | Producer attempted to produce with an old epoch.
pattern INVALID_PRODUCER_EPOCH :: ErrorCode
pattern INVALID_PRODUCER_EPOCH = 47

-- | The producer attempted a transactional operation in an invalid state.
pattern INVALID_TXN_STATE :: ErrorCode
pattern INVALID_TXN_STATE = 48

-- | The producer attempted to use a producer id which is not currently
-- assigned to its transactional id.
pattern INVALID_PRODUCER_ID_MAPPING :: ErrorCode
pattern INVALID_PRODUCER_ID_MAPPING = 49

-- | The transaction timeout is larger than the maximum value allowed by the
-- broker (as configured by transaction.max.timeout.ms).
pattern INVALID_TRANSACTION_TIMEOUT :: ErrorCode
pattern INVALID_TRANSACTION_TIMEOUT = 50

-- | The producer attempted to update a transaction while another concurrent
-- operation on the same transaction was ongoing.
pattern CONCURRENT_TRANSACTIONS :: ErrorCode
pattern CONCURRENT_TRANSACTIONS = 51

-- | Indicates that the transaction coordinator sending a WriteTxnMarker is no
-- longer the current coordinator for a given producer.
pattern TRANSACTION_COORDINATOR_FENCED :: ErrorCode
pattern TRANSACTION_COORDINATOR_FENCED = 52

-- | Transactional Id authorization failed.
pattern TRANSACTIONAL_ID_AUTHORIZATION_FAILED :: ErrorCode
pattern TRANSACTIONAL_ID_AUTHORIZATION_FAILED = 53

-- | Security features are disabled.
pattern SECURITY_DISABLED :: ErrorCode
pattern SECURITY_DISABLED = 54

-- | The broker did not attempt to execute this operation. This may happen for
-- batched RPCs where some operations in the batch failed, causing the broker
-- to respond without trying the rest.
pattern OPERATION_NOT_ATTEMPTED :: ErrorCode
pattern OPERATION_NOT_ATTEMPTED = 55

-- | Disk error when trying to access log file on the disk.
pattern KAFKA_STORAGE_ERROR :: ErrorCode
pattern KAFKA_STORAGE_ERROR = 56

-- | The user-specified log directory is not found in the broker config.
pattern LOG_DIR_NOT_FOUND :: ErrorCode
pattern LOG_DIR_NOT_FOUND = 57

-- | SASL Authentication failed.
pattern SASL_AUTHENTICATION_FAILED :: ErrorCode
pattern SASL_AUTHENTICATION_FAILED = 58

-- | This exception is raised by the broker if it could not locate the producer
-- metadata associated with the producerId in question. This could happen if,
-- for instance, the producer's records were deleted because their retention
-- time had elapsed. Once the last records of the producerId are removed, the
-- producer's metadata is removed from the broker, and future appends by the
-- producer will return this exception.
pattern UNKNOWN_PRODUCER_ID :: ErrorCode
pattern UNKNOWN_PRODUCER_ID = 59

-- | A partition reassignment is in progress.
pattern REASSIGNMENT_IN_PROGRESS :: ErrorCode
pattern REASSIGNMENT_IN_PROGRESS = 60

-- | Delegation Token feature is not enabled.
pattern DELEGATION_TOKEN_AUTH_DISABLED :: ErrorCode
pattern DELEGATION_TOKEN_AUTH_DISABLED = 61

-- | Delegation Token is not found on server.
pattern DELEGATION_TOKEN_NOT_FOUND :: ErrorCode
pattern DELEGATION_TOKEN_NOT_FOUND = 62

-- | Specified Principal is not valid Owner/Renewer.
pattern DELEGATION_TOKEN_OWNER_MISMATCH :: ErrorCode
pattern DELEGATION_TOKEN_OWNER_MISMATCH = 63

-- | Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels
-- and on delegation token authenticated channels.
pattern DELEGATION_TOKEN_REQUEST_NOT_ALLOWED :: ErrorCode
pattern DELEGATION_TOKEN_REQUEST_NOT_ALLOWED = 64

-- | Delegation Token authorization failed.
pattern DELEGATION_TOKEN_AUTHORIZATION_FAILED :: ErrorCode
pattern DELEGATION_TOKEN_AUTHORIZATION_FAILED = 65

-- | Delegation Token is expired.
pattern DELEGATION_TOKEN_EXPIRED :: ErrorCode
pattern DELEGATION_TOKEN_EXPIRED = 66

-- | Supplied principalType is not supported.
pattern INVALID_PRINCIPAL_TYPE :: ErrorCode
pattern INVALID_PRINCIPAL_TYPE = 67

-- | The group is not empty.
pattern NON_EMPTY_GROUP :: ErrorCode
pattern NON_EMPTY_GROUP = 68

-- | The group id does not exist.
pattern GROUP_ID_NOT_FOUND :: ErrorCode
pattern GROUP_ID_NOT_FOUND = 69

-- | The fetch session ID was not found.
pattern FETCH_SESSION_ID_NOT_FOUND :: ErrorCode
pattern FETCH_SESSION_ID_NOT_FOUND = 70

-- | The fetch session epoch is invalid.
pattern INVALID_FETCH_SESSION_EPOCH :: ErrorCode
pattern INVALID_FETCH_SESSION_EPOCH = 71

-- | There is no listener on the leader broker that matches the listener on
-- which metadata request was processed.
pattern LISTENER_NOT_FOUND :: ErrorCode
pattern LISTENER_NOT_FOUND = 72

-- | Topic deletion is disabled.
pattern TOPIC_DELETION_DISABLED :: ErrorCode
pattern TOPIC_DELETION_DISABLED = 73

-- | The leader epoch in the request is older than the epoch on the broker.
pattern FENCED_LEADER_EPOCH :: ErrorCode
pattern FENCED_LEADER_EPOCH = 74

-- | The leader epoch in the request is newer than the epoch on the broker.
pattern UNKNOWN_LEADER_EPOCH :: ErrorCode
pattern UNKNOWN_LEADER_EPOCH = 75

-- | The requesting client does not support the compression type of given
-- partition.
pattern UNSUPPORTED_COMPRESSION_TYPE :: ErrorCode
pattern UNSUPPORTED_COMPRESSION_TYPE = 76

-- | Broker epoch has changed.
pattern STALE_BROKER_EPOCH :: ErrorCode
pattern STALE_BROKER_EPOCH = 77

-- | The leader high watermark has not caught up from a recent leader election
-- so the offsets cannot be guaranteed to be monotonically increasing.
pattern OFFSET_NOT_AVAILABLE :: ErrorCode
pattern OFFSET_NOT_AVAILABLE = 78

-- | The group member needs to have a valid member id before actually entering
-- a consumer group.
pattern MEMBER_ID_REQUIRED :: ErrorCode
pattern MEMBER_ID_REQUIRED = 79

-- | The preferred leader was not available.
pattern PREFERRED_LEADER_NOT_AVAILABLE :: ErrorCode
pattern PREFERRED_LEADER_NOT_AVAILABLE = 80

-- | The consumer group has reached its max size.
pattern GROUP_MAX_SIZE_REACHED :: ErrorCode
pattern GROUP_MAX_SIZE_REACHED = 81

-- | The broker rejected this static consumer since another consumer with the
-- same group.instance.id has registered with a different member.id.
pattern FENCED_INSTANCE_ID :: ErrorCode
pattern FENCED_INSTANCE_ID = 82

-- | Eligible topic partition leaders are not available.
pattern ELIGIBLE_LEADERS_NOT_AVAILABLE :: ErrorCode
pattern ELIGIBLE_LEADERS_NOT_AVAILABLE = 83

-- | Leader election not needed for topic partition.
pattern ELECTION_NOT_NEEDED :: ErrorCode
pattern ELECTION_NOT_NEEDED = 84

-- | No partition reassignment is in progress.
pattern NO_REASSIGNMENT_IN_PROGRESS :: ErrorCode
pattern NO_REASSIGNMENT_IN_PROGRESS = 85

-- | Deleting offsets of a topic is forbidden while the consumer group is
-- actively subscribed to it.
pattern GROUP_SUBSCRIBED_TO_TOPIC :: ErrorCode
pattern GROUP_SUBSCRIBED_TO_TOPIC = 86

-- | This record has failed the validation on broker and hence will be
-- rejected.
pattern INVALID_RECORD :: ErrorCode
pattern INVALID_RECORD = 87

-- | There are unstable offsets that need to be cleared.
pattern UNSTABLE_OFFSET_COMMIT :: ErrorCode
pattern UNSTABLE_OFFSET_COMMIT = 88

-- | The throttling quota has been exceeded.
pattern THROTTLING_QUOTA_EXCEEDED :: ErrorCode
pattern THROTTLING_QUOTA_EXCEEDED = 89

-- | There is a newer producer with the same transactionalId which fences the
-- current one.
pattern PRODUCER_FENCED :: ErrorCode
pattern PRODUCER_FENCED = 90

-- | A request illegally referred to a resource that does not exist.
pattern RESOURCE_NOT_FOUND :: ErrorCode
pattern RESOURCE_NOT_FOUND = 91

-- | A request illegally referred to the same resource twice.
pattern DUPLICATE_RESOURCE :: ErrorCode
pattern DUPLICATE_RESOURCE = 92

-- | Requested credential would not meet criteria for acceptability.
pattern UNACCEPTABLE_CREDENTIAL :: ErrorCode
pattern UNACCEPTABLE_CREDENTIAL = 93

-- | Indicates that the either the sender or recipient of a voter-only request
-- is not one of the expected voters
pattern INCONSISTENT_VOTER_SET :: ErrorCode
pattern INCONSISTENT_VOTER_SET = 94

-- | The given update version was invalid.
pattern INVALID_UPDATE_VERSION :: ErrorCode
pattern INVALID_UPDATE_VERSION = 95

-- | Unable to update finalized features due to an unexpected server error.
pattern FEATURE_UPDATE_FAILED :: ErrorCode
pattern FEATURE_UPDATE_FAILED = 96

-- | Request principal deserialization failed during forwarding. This indicates
-- an internal error on the broker cluster security setup.
pattern PRINCIPAL_DESERIALIZATION_FAILURE :: ErrorCode
pattern PRINCIPAL_DESERIALIZATION_FAILURE = 97

-- | Requested snapshot was not found
pattern SNAPSHOT_NOT_FOUND :: ErrorCode
pattern SNAPSHOT_NOT_FOUND = 98

-- | Requested position is not greater than or equal to zero, and less than the
-- size of the snapshot.
pattern POSITION_OUT_OF_RANGE :: ErrorCode
pattern POSITION_OUT_OF_RANGE = 99

-- | This server does not host this topic ID.
pattern UNKNOWN_TOPIC_ID :: ErrorCode
pattern UNKNOWN_TOPIC_ID = 100

-- | This broker ID is already in use.
pattern DUPLICATE_BROKER_REGISTRATION :: ErrorCode
pattern DUPLICATE_BROKER_REGISTRATION = 101

-- | The given broker ID was not registered.
pattern BROKER_ID_NOT_REGISTERED :: ErrorCode
pattern BROKER_ID_NOT_REGISTERED = 102

-- | The log's topic ID did not match the topic ID in the request
pattern INCONSISTENT_TOPIC_ID :: ErrorCode
pattern INCONSISTENT_TOPIC_ID = 103

-- | The clusterId in the request does not match that found on the server
pattern INCONSISTENT_CLUSTER_ID :: ErrorCode
pattern INCONSISTENT_CLUSTER_ID = 104

-- | The transactionalId could not be found
pattern TRANSACTIONAL_ID_NOT_FOUND :: ErrorCode
pattern TRANSACTIONAL_ID_NOT_FOUND = 105

-- | The fetch session encountered inconsistent topic ID usage
pattern FETCH_SESSION_TOPIC_ID_ERROR :: ErrorCode
pattern FETCH_SESSION_TOPIC_ID_ERROR = 106

-- | The new ISR contains at least one ineligible replica.
pattern INELIGIBLE_REPLICA :: ErrorCode
pattern INELIGIBLE_REPLICA = 107

-- | The AlterPartition request successfully updated the partition state but
-- the leader has changed.
pattern NEW_LEADER_ELECTED :: ErrorCode
pattern NEW_LEADER_ELECTED = 108

-- | The requested offset is moved to tiered storage.
pattern OFFSET_MOVED_TO_TIERED_STORAGE :: ErrorCode
pattern OFFSET_MOVED_TO_TIERED_STORAGE = 109

-- | The member epoch is fenced by the group coordinator. The member must
-- abandon all its partitions and rejoin.
pattern FENCED_MEMBER_EPOCH :: ErrorCode
pattern FENCED_MEMBER_EPOCH = 110

-- | The instance ID is still used by another member in the consumer group.
-- That member must leave first.
pattern UNRELEASED_INSTANCE_ID :: ErrorCode
pattern UNRELEASED_INSTANCE_ID = 111

-- | The assignor or its version range is not supported by the consumer group.
pattern UNSUPPORTED_ASSIGNOR :: ErrorCode
pattern UNSUPPORTED_ASSIGNOR = 112

doesErrorRetriable :: ErrorCode -> Bool
doesErrorRetriable UNKNOWN_SERVER_ERROR                  = False
doesErrorRetriable NONE                                  = False
doesErrorRetriable OFFSET_OUT_OF_RANGE                   = False
doesErrorRetriable CORRUPT_MESSAGE                       = True
doesErrorRetriable UNKNOWN_TOPIC_OR_PARTITION            = True
doesErrorRetriable INVALID_FETCH_SIZE                    = False
doesErrorRetriable LEADER_NOT_AVAILABLE                  = True
doesErrorRetriable NOT_LEADER_OR_FOLLOWER                = True
doesErrorRetriable REQUEST_TIMED_OUT                     = True
doesErrorRetriable BROKER_NOT_AVAILABLE                  = False
doesErrorRetriable REPLICA_NOT_AVAILABLE                 = True
doesErrorRetriable MESSAGE_TOO_LARGE                     = False
doesErrorRetriable STALE_CONTROLLER_EPOCH                = False
doesErrorRetriable OFFSET_METADATA_TOO_LARGE             = False
doesErrorRetriable NETWORK_EXCEPTION                     = True
doesErrorRetriable COORDINATOR_LOAD_IN_PROGRESS          = True
doesErrorRetriable COORDINATOR_NOT_AVAILABLE             = True
doesErrorRetriable NOT_COORDINATOR                       = True
doesErrorRetriable INVALID_TOPIC_EXCEPTION               = False
doesErrorRetriable RECORD_LIST_TOO_LARGE                 = False
doesErrorRetriable NOT_ENOUGH_REPLICAS                   = True
doesErrorRetriable NOT_ENOUGH_REPLICAS_AFTER_APPEND      = True
doesErrorRetriable INVALID_REQUIRED_ACKS                 = False
doesErrorRetriable ILLEGAL_GENERATION                    = False
doesErrorRetriable INCONSISTENT_GROUP_PROTOCOL           = False
doesErrorRetriable INVALID_GROUP_ID                      = False
doesErrorRetriable UNKNOWN_MEMBER_ID                     = False
doesErrorRetriable INVALID_SESSION_TIMEOUT               = False
doesErrorRetriable REBALANCE_IN_PROGRESS                 = False
doesErrorRetriable INVALID_COMMIT_OFFSET_SIZE            = False
doesErrorRetriable TOPIC_AUTHORIZATION_FAILED            = False
doesErrorRetriable GROUP_AUTHORIZATION_FAILED            = False
doesErrorRetriable CLUSTER_AUTHORIZATION_FAILED          = False
doesErrorRetriable INVALID_TIMESTAMP                     = False
doesErrorRetriable UNSUPPORTED_SASL_MECHANISM            = False
doesErrorRetriable ILLEGAL_SASL_STATE                    = False
doesErrorRetriable UNSUPPORTED_VERSION                   = False
doesErrorRetriable TOPIC_ALREADY_EXISTS                  = False
doesErrorRetriable INVALID_PARTITIONS                    = False
doesErrorRetriable INVALID_REPLICATION_FACTOR            = False
doesErrorRetriable INVALID_REPLICA_ASSIGNMENT            = False
doesErrorRetriable INVALID_CONFIG                        = False
doesErrorRetriable NOT_CONTROLLER                        = True
doesErrorRetriable INVALID_REQUEST                       = False
doesErrorRetriable UNSUPPORTED_FOR_MESSAGE_FORMAT        = False
doesErrorRetriable POLICY_VIOLATION                      = False
doesErrorRetriable OUT_OF_ORDER_SEQUENCE_NUMBER          = False
doesErrorRetriable DUPLICATE_SEQUENCE_NUMBER             = False
doesErrorRetriable INVALID_PRODUCER_EPOCH                = False
doesErrorRetriable INVALID_TXN_STATE                     = False
doesErrorRetriable INVALID_PRODUCER_ID_MAPPING           = False
doesErrorRetriable INVALID_TRANSACTION_TIMEOUT           = False
doesErrorRetriable CONCURRENT_TRANSACTIONS               = True
doesErrorRetriable TRANSACTION_COORDINATOR_FENCED        = False
doesErrorRetriable TRANSACTIONAL_ID_AUTHORIZATION_FAILED = False
doesErrorRetriable SECURITY_DISABLED                     = False
doesErrorRetriable OPERATION_NOT_ATTEMPTED               = False
doesErrorRetriable KAFKA_STORAGE_ERROR                   = True
doesErrorRetriable LOG_DIR_NOT_FOUND                     = False
doesErrorRetriable SASL_AUTHENTICATION_FAILED            = False
doesErrorRetriable UNKNOWN_PRODUCER_ID                   = False
doesErrorRetriable REASSIGNMENT_IN_PROGRESS              = False
doesErrorRetriable DELEGATION_TOKEN_AUTH_DISABLED        = False
doesErrorRetriable DELEGATION_TOKEN_NOT_FOUND            = False
doesErrorRetriable DELEGATION_TOKEN_OWNER_MISMATCH       = False
doesErrorRetriable DELEGATION_TOKEN_REQUEST_NOT_ALLOWED  = False
doesErrorRetriable DELEGATION_TOKEN_AUTHORIZATION_FAILED = False
doesErrorRetriable DELEGATION_TOKEN_EXPIRED              = False
doesErrorRetriable INVALID_PRINCIPAL_TYPE                = False
doesErrorRetriable NON_EMPTY_GROUP                       = False
doesErrorRetriable GROUP_ID_NOT_FOUND                    = False
doesErrorRetriable FETCH_SESSION_ID_NOT_FOUND            = True
doesErrorRetriable INVALID_FETCH_SESSION_EPOCH           = True
doesErrorRetriable LISTENER_NOT_FOUND                    = True
doesErrorRetriable TOPIC_DELETION_DISABLED               = False
doesErrorRetriable FENCED_LEADER_EPOCH                   = True
doesErrorRetriable UNKNOWN_LEADER_EPOCH                  = True
doesErrorRetriable UNSUPPORTED_COMPRESSION_TYPE          = False
doesErrorRetriable STALE_BROKER_EPOCH                    = False
doesErrorRetriable OFFSET_NOT_AVAILABLE                  = True
doesErrorRetriable MEMBER_ID_REQUIRED                    = False
doesErrorRetriable PREFERRED_LEADER_NOT_AVAILABLE        = True
doesErrorRetriable GROUP_MAX_SIZE_REACHED                = False
doesErrorRetriable FENCED_INSTANCE_ID                    = False
doesErrorRetriable ELIGIBLE_LEADERS_NOT_AVAILABLE        = True
doesErrorRetriable ELECTION_NOT_NEEDED                   = True
doesErrorRetriable NO_REASSIGNMENT_IN_PROGRESS           = False
doesErrorRetriable GROUP_SUBSCRIBED_TO_TOPIC             = False
doesErrorRetriable INVALID_RECORD                        = False
doesErrorRetriable UNSTABLE_OFFSET_COMMIT                = True
doesErrorRetriable THROTTLING_QUOTA_EXCEEDED             = True
doesErrorRetriable PRODUCER_FENCED                       = False
doesErrorRetriable RESOURCE_NOT_FOUND                    = False
doesErrorRetriable DUPLICATE_RESOURCE                    = False
doesErrorRetriable UNACCEPTABLE_CREDENTIAL               = False
doesErrorRetriable INCONSISTENT_VOTER_SET                = False
doesErrorRetriable INVALID_UPDATE_VERSION                = False
doesErrorRetriable FEATURE_UPDATE_FAILED                 = False
doesErrorRetriable PRINCIPAL_DESERIALIZATION_FAILURE     = False
doesErrorRetriable SNAPSHOT_NOT_FOUND                    = False
doesErrorRetriable POSITION_OUT_OF_RANGE                 = False
doesErrorRetriable UNKNOWN_TOPIC_ID                      = True
doesErrorRetriable DUPLICATE_BROKER_REGISTRATION         = False
doesErrorRetriable BROKER_ID_NOT_REGISTERED              = False
doesErrorRetriable INCONSISTENT_TOPIC_ID                 = True
doesErrorRetriable INCONSISTENT_CLUSTER_ID               = False
doesErrorRetriable TRANSACTIONAL_ID_NOT_FOUND            = False
doesErrorRetriable FETCH_SESSION_TOPIC_ID_ERROR          = True
doesErrorRetriable INELIGIBLE_REPLICA                    = False
doesErrorRetriable NEW_LEADER_ELECTED                    = False
doesErrorRetriable OFFSET_MOVED_TO_TIERED_STORAGE        = False
doesErrorRetriable FENCED_MEMBER_EPOCH                   = False
doesErrorRetriable UNRELEASED_INSTANCE_ID                = False
doesErrorRetriable UNSUPPORTED_ASSIGNOR                  = False
{-# INLINE doesErrorRetriable #-}
