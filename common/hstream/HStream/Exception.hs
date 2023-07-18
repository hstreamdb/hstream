{-# LANGUAGE CPP             #-}
{-# LANGUAGE GADTs           #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE RankNTypes      #-}
{-# OPTIONS_GHC -pgmPcpphs -optP--cpp #-}

module HStream.Exception
  ( SomeHStreamException
    -- Root of Server Exception
  , SomeHServerException (..)

    -- * Exception Info
  , ExInfo (..)
  , displayExInfo

    -- * Exception: SomeCancelled
    --
    -- $cancelled
  , SomeCancelled
  , StreamReadError (StreamReadError)
  , StreamReadClose (StreamReadClose)
  , StreamWriteError (StreamWriteError)

    -- * Exception: SomeUnknown
    --
    -- $unknown
  , SomeUnknown
  , UnknownPushQueryStatus (UnknownPushQueryStatus)

    -- * Exception: SomeInvalidArgument
    --
    -- $invalidArgument
  , SomeInvalidArgument
  , InvalidReplicaFactor (InvalidReplicaFactor)
  , InvalidObjectIdentifier (InvalidObjectIdentifier)
  , invalidIdentifier
  , InvalidShardCount (InvalidShardCount)
  , EmptyBatchedRecord (EmptyBatchedRecord)
  , InvalidRecordSize (InvalidRecordSize)
  , InvalidResourceType (InvalidResourceType)
  , InvalidShardOffset (InvalidShardOffset)
  , InvalidSubscriptionOffset (InvalidSubscriptionOffset)
  , DecodeHStreamRecordErr (DecodeHStreamRecordErr)
  , NoRecordHeader (NoRecordHeader)
  , UnknownCompressionType (UnknownCompressionType)
  , InvalidStatsType (InvalidStatsType)
  , InvalidStatsInterval (InvalidStatsInterval)
  , InvalidSqlStatement (InvalidSqlStatement)
  , InvalidConnectorType (InvalidConnectorType)
  , InvalidQuerySql (InvalidQuerySql)
  , SQLNotSupportedByParseSQL(SQLNotSupportedByParseSQL)
  , ConflictShardReaderOffset (ConflictShardReaderOffset)
  , TooManyShardCount (TooManyShardCount)
  , InvalidTrimPoint (InvalidTrimPoint)

    -- * Exception: SomeDeadlineExceeded
    --
    -- $deadlineExceeded
  , SomeDeadlineExceeded

    -- * Exception: SomeNotFound
    --
    -- $notFound
  , SomeNotFound
  , NodesNotFound (NodesNotFound)
  , StreamNotFound (StreamNotFound)
  , SubscriptionNotFound (SubscriptionNotFound)
  , ConnectorNotFound (ConnectorNotFound)
  , ViewNotFound (ViewNotFound)
  , ShardNotFound (ShardNotFound)
  , QueryNotFound (QueryNotFound)
  , RQLiteTableNotFound (RQLiteTableNotFound)
  , RQLiteRowNotFound (RQLiteRowNotFound)
  , LocalMetaStoreTableNotFound (LocalMetaStoreTableNotFound)
  , LocalMetaStoreObjectNotFound (LocalMetaStoreObjectNotFound)

    -- * Exception: SomeAlreadyExists
    --
    -- $alreadyExists
  , SomeAlreadyExists
  , StreamExists (StreamExists)
  , SubscriptionExists (SubscriptionExists)
  , ConsumerExists (ConsumerExists)
  , ShardReaderExists (ShardReaderExists)
  , RQLiteTableAlreadyExists (RQLiteTableAlreadyExists)
  , RQLiteRowAlreadyExists (RQLiteRowAlreadyExists)
  , LocalMetaStoreTableAlreadyExists (LocalMetaStoreTableAlreadyExists)
  , LocalMetaStoreObjectAlreadyExists (LocalMetaStoreObjectAlreadyExists)
  , ConnectorExists (ConnectorExists)
  , QueryExists (QueryExists)
  , ViewExists (ViewExists)

    -- * Exception: SomePermissionDenied
    --
    -- $permissionDenied
  , SomePermissionDenied

    -- * Exception: SomeResourceExhausted
    --
    -- $resourceExhausted
  , SomeResourceExhausted
  , NoMoreSlots (NoMoreSlots)

    -- * Exception: SomeFailedPrecondition
    --
    -- $failedPrecondition
  , SomeFailedPrecondition
  , FoundSubscription (FoundSubscription)
  , EmptyShardReader (EmptyShardReader)
  , NonExistentStream (NonExistentStream)
  , WrongServer (WrongServer)
  , WrongExecutionPlan (WrongExecutionPlan)
  , FoundActiveConsumers (FoundActiveConsumers)
  , ShardCanNotSplit (ShardCanNotSplit)
  , ShardCanNotMerge (ShardCanNotMerge)
  , RQLiteRowBadVersion (RQLiteRowBadVersion)
  , LocalMetaStoreObjectBadVersion (LocalMetaStoreObjectBadVersion)
  , QueryNotTerminated (QueryNotTerminated)
  , FoundAssociatedView (FoundAssociatedView)
  , QueryAlreadyTerminated(QueryAlreadyTerminated)
  , QueryNotAborted(QueryNotAborted)
  , ConnectorCheckFailed(ConnectorCheckFailed)
  , ConnectorInvalidStatus(ConnectorInvalidStatus)

    -- * Exception: SomeAborted
    --
    -- $aborted
  , SomeAborted
  , SubscriptionIsDeleting (SubscriptionIsDeleting)
  , SubscriptionInvalidError (SubscriptionInvalidError)
  , ConsumerInvalidError (ConsumerInvalidError)
  , QueryNotRunning (QueryNotRunning)

    -- * Exception: SomeOutOfRange
    --
    -- $outOfRange
  , SomeOutOfRange

    -- * Exception: SomeUnimplemented
    --
    -- $unimplemented
  , SomeUnimplemented
  , ExecPlanUnimplemented (ExecPlanUnimplemented)
  , ConnectorUnimplemented (ConnectorUnimplemented)

    -- * Exception: SomeInternal
    --
    -- $internal
  , SomeInternal
  , UnexpectedError (UnexpectedError)
  , WrongOffset (WrongOffset)
  , ZstdCompresstionErr (ZstdCompresstionErr)
  , RQLiteNetworkErr (RQLiteNetworkErr)
  , RQLiteDecodeErr (RQLiteDecodeErr)
  , RQLiteUnspecifiedErr (RQLiteUnspecifiedErr)
  , LocalMetaStoreInternalErr (LocalMetaStoreInternalErr)
  , SlotAllocDecodeError (SlotAllocDecodeError)
  , DiscardedMethod (DiscardedMethod)
  , PushQuerySendError (PushQuerySendError)
  , ConnectorProcessError (ConnectorProcessError)
  , SomeStoreInternal (SomeStoreInternal)
  , SomeServerInternal (SomeServerInternal)

    -- * Exception: SomeUnavailable
  , SomeUnavailable
  , ServerNotAvailable (ServerNotAvailable)
  , ResourceAllocationException (ResourceAllocationException)

    -- * Exception: SomeDataLoss
  , SomeDataLoss

    -- * Exception: SomeUnauthenticated
    --
    -- $unauthenticated
  , SomeUnauthenticated
  , RQLiteNoAuth (RQLiteNoAuth)

    -- * Handler
  , mkExceptionHandle
  , mkExceptionHandle'
    -- ** for grpc-haskell
  , MkResp
  , ExceptionHandle
  , setRespType
  , mkStatusDetails
  , defaultHServerExHandlers
  , zkExceptionHandlers
    -- ** for hs-grpc-server
  , mkGrpcStatus
  , mkStatusMsg
  , hServerExHandlers
  , zkExHandlers

    -- * Re-export
  , E.Handler
  ) where

import           Control.Exception             (Exception (..), SomeException)
import qualified Control.Exception             as E
import           Data.Aeson                    ((.=))
import qualified Data.Aeson                    as Aeson
import           Data.ByteString               (ByteString)
import qualified Data.ByteString.Char8         as BSC
import qualified Data.ByteString.Lazy.Char8    as BSLC
import           Data.Text                     (Text)
import qualified Data.Text                     as Text
import           Data.Typeable                 (Typeable, cast)
import           GHC.Stack                     (CallStack, HasCallStack,
                                                callStack, prettyCallStack)
import qualified HsGrpc.Server.Types           as HsGrpc
import           Network.GRPC.HighLevel.Client (StatusCode (..),
                                                StatusDetails (..))
import           Network.GRPC.HighLevel.Server (ServerResponse (..))
import           Proto3.Wire                   (fromProtoEnum)
import qualified ZooKeeper.Exception           as ZK

import qualified HStream.Logger                as Log
import qualified HStream.Server.HStreamApi     as API

-------------------------------------------------------------------------------

-- | The root exception type for all the exceptions in a hstream server
--
-- @
-- SomeHStreamException
--   |
--   +-> SomeHServerException
--   |
--   +-> SomeHStoreException (TODO)
-- @
--
-- TODO: Make the SomeHStoreException to be a sub exception of SomeHStreamException
data SomeHStreamException = forall e . Exception e => SomeHStreamException e

instance Show SomeHStreamException where
  show (SomeHStreamException e) = show e

instance Exception SomeHStreamException

-- The format of this function's name is important because of the macro.
--
-- Format: "a <> ExceptionName(SomeHStreamException) <> ToException"
aSomeHStreamExceptionToException :: Exception e => e -> E.SomeException
aSomeHStreamExceptionToException = E.toException . SomeHStreamException

-- Format: "a <> ExceptionName(SomeHStreamException) <> FromException"
aSomeHStreamExceptionFromException :: Exception e => E.SomeException -> Maybe e
aSomeHStreamExceptionFromException x = do
  SomeHStreamException a <- E.fromException x
  cast a

#define MAKE_SUB_EX(BASE_E, SUB_E) \
data SUB_E = forall e . Exception e => SUB_E e;                                \
instance Show SUB_E where                                                      \
{ show (SUB_E e) = show e };                                                   \
instance Exception SUB_E where                                                 \
{ toException = a##BASE_E##ToException;                                        \
  fromException = a##BASE_E##FromException;                                    \
  displayException (SUB_E e) = displayException e;                             \
};                                                                             \
a##SUB_E##ToException :: Exception e => e -> E.SomeException;                  \
a##SUB_E##ToException = E.toException . SUB_E;                                 \
a##SUB_E##FromException :: Exception e => E.SomeException -> Maybe e;          \
a##SUB_E##FromException x = do SUB_E a <- E.fromException x; cast a;

-------------------------------------------------------------------------------

MAKE_SUB_EX(SomeHStreamException, SomeHServerException)

-------------------------------------------------------------------------------

data ExInfo a = ExInfo
  { exErrCode   :: {-# UNPACK #-} !API.ErrorCode
  , exErrMsg    :: {-# UNPACK #-} !Text
  , exErrExtra  :: {-# UNPACK #-} !a
  , exCallStack :: {-# UNPACK #-} !CallStack    -- ^ lightweight partial call-stack
  }

instance Show a => Show (ExInfo a) where
  show ExInfo{..} = "{"
                 <> "error:" <> show exErrCode <> ","
                 <> "message:" <> Text.unpack exErrMsg <> ","
                 <> "extra:" <> show exErrExtra
                 <> "}\n"
                 <> prettyCallStack exCallStack

instance Aeson.ToJSON a => Aeson.ToJSON (ExInfo a) where
  toJSON ExInfo{..} = Aeson.object
    [ "error" .= fromProtoEnum exErrCode
    , "message" .= exErrMsg
    , "extra" .= Aeson.toJSON exErrExtra
    ]

displayExInfo :: Aeson.ToJSON a => (ExInfo a) -> String
displayExInfo = BSLC.unpack . Aeson.encode

#define MAKE_EX(ExCls, Ex, Ty) \
  newtype Ex = Ex##_ (ExInfo Ty) deriving (Show);                     \
  instance Exception Ex where                                         \
  { toException = a##ExCls##ToException;                              \
    fromException = a##ExCls##FromException;                          \
    displayException (Ex##_ info) = displayExInfo info;               \
  };                                                                  \
  {-# COMPLETE Ex #-};                                                \
  pattern Ex :: HasCallStack => API.ErrorCode -> Text -> Ty -> Ex;    \
  pattern Ex ec emsg extra <- Ex##_ (ExInfo ec emsg extra _) where    \
    Ex ec emsg extra = Ex##_ (ExInfo ec emsg extra callStack);

#define MAKE_EX_DEFMSG(ExCls, Ex, Ty) \
  newtype Ex = Ex##_ (ExInfo Ty) deriving (Show);                     \
  instance Exception Ex where                                         \
  { toException = a##ExCls##ToException;                              \
    fromException = a##ExCls##FromException;                          \
    displayException (Ex##_ info) = displayExInfo info;               \
  };                                                                  \
  {-# COMPLETE Ex #-};                                                \
  pattern Ex :: HasCallStack => API.ErrorCode -> Ty -> Ex;            \
  pattern Ex ec extra <- Ex##_ (ExInfo ec _ extra _) where            \
    Ex ec extra = Ex##_ (ExInfo ec #Ex extra callStack);

-- A generic version of MAKE_EX
#define MAKE_EX_G(ExCls, Ex) \
  newtype Ex a = Ex##_ (ExInfo a) deriving (Show);                             \
  instance (Typeable a, Show a, Aeson.ToJSON a) => Exception (Ex a) where      \
  { toException = a##ExCls##ToException;                                       \
    fromException = a##ExCls##FromException;                                   \
    displayException (Ex##_ info) = displayExInfo info;                        \
  };                                                                           \
  {-# COMPLETE Ex #-};                                                         \
  pattern Ex :: HasCallStack => API.ErrorCode -> Text -> a -> Ex a;            \
  pattern Ex ec emsg extra <- Ex##_ (ExInfo ec emsg extra _) where             \
    Ex ec emsg extra = Ex##_ (ExInfo ec emsg extra callStack);

#define MAKE_EX_G_DEFMSG(ExCls, Ex) \
  newtype Ex a = Ex##_ (ExInfo a) deriving (Show);                             \
  instance (Typeable a, Show a, Aeson.ToJSON a) => Exception (Ex a) where      \
  { toException = a##ExCls##ToException;                                       \
    fromException = a##ExCls##FromException;                                   \
    displayException (Ex##_ info) = displayExInfo info;                        \
  };                                                                           \
  {-# COMPLETE Ex #-};                                                         \
  pattern Ex :: HasCallStack => API.ErrorCode -> a -> Ex a;                    \
  pattern Ex ec extra <- Ex##_ (ExInfo ec _ extra _) where                     \
    Ex ec extra = Ex##_ (ExInfo ec #Ex extra callStack);

-- Constructor has 0 argument
#define MAKE_EX_0(ExCls, Ex, ECode, EMsg) \
  data Ex = Ex##_ (ExInfo Aeson.Value) deriving (Show);   \
  instance Exception Ex where                             \
  { toException = a##ExCls##ToException;                  \
    fromException = a##ExCls##FromException;              \
    displayException (Ex##_ info) = displayExInfo info;   \
  };                                                      \
  {-# COMPLETE Ex #-};                                    \
  pattern Ex :: HasCallStack => Ex;                       \
  pattern Ex <- Ex##_ _ where                             \
    Ex = Ex##_ (ExInfo ECode EMsg Aeson.Null callStack);

#define MAKE_EX_0_DEFMSG(ExCls, Ex, ECode) \
  MAKE_EX_0(ExCls, Ex, ECode, #Ex)

-- Constructor has 1 argument, which is exErrExtra
#define MAKE_EX_1(ExCls, Ex, Ty, ECode, EMsg) \
newtype Ex = Ex##_ (ExInfo Ty) deriving (Show);         \
instance Exception Ex where                             \
{ toException = a##ExCls##ToException;                  \
  fromException = a##ExCls##FromException;              \
  displayException (Ex##_ info) = displayExInfo info;   \
};                                                      \
{-# COMPLETE Ex #-};                                    \
pattern Ex :: HasCallStack => Ty -> Ex ;                \
pattern Ex extra <- Ex##_ (ExInfo _ _ extra _) where    \
  Ex extra = Ex##_ (ExInfo ECode EMsg extra callStack);

#define MAKE_EX_1_DEFMSG(ExCls, Ex, Ty, ECode) \
  MAKE_EX_1(ExCls, Ex, Ty, ECode, #Ex)

-------------------------------------------------------------------------------
-- Exception: SomeCancelled

-- $cancelled
--
-- The operation was cancelled, typically by the caller.
MAKE_SUB_EX(SomeHServerException, SomeCancelled)

MAKE_EX_1_DEFMSG(SomeCancelled, StreamReadError, String, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeCancelled, StreamReadClose, String, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeCancelled, StreamWriteError, String, API.ErrorCodeInternalError)

-------------------------------------------------------------------------------
-- Exception: SomeUnknown

-- $unknown
--
-- Unknown error. For example, this error may be returned when a Status value
-- received from another address space belongs to an error space that is not
-- known in this address space. Also errors raised by APIs that do not return
-- enough error information may be converted to this error.
MAKE_SUB_EX(SomeHServerException, SomeUnknown)

MAKE_EX_1_DEFMSG(SomeUnknown, UnknownPushQueryStatus, String, API.ErrorCodeInternalError)

-------------------------------------------------------------------------------
-- Exception: SomeInvalidArgument

-- $invalidArgument
--
-- The client specified an invalid argument. Note that 'InvalidArgument'
-- indicates arguments that are problematic regardless of the state of the
-- system.
MAKE_SUB_EX(SomeHServerException, SomeInvalidArgument)

MAKE_EX_1_DEFMSG(SomeInvalidArgument, InvalidReplicaFactor, String, API.ErrorCodeStreamInvalidReplicaFactor)
MAKE_EX_DEFMSG(SomeInvalidArgument, InvalidObjectIdentifier, String)
MAKE_EX_1_DEFMSG(SomeInvalidArgument, InvalidShardCount, String, API.ErrorCodeStreamInvalidShardCount)
MAKE_EX_0(SomeInvalidArgument, EmptyBatchedRecord, API.ErrorCodeStreamEmptyBatchedRecord,
    "BatchedRecord shouldn't be Nothing")
MAKE_EX_1(SomeInvalidArgument, InvalidRecordSize, Int, API.ErrorCodeStreamInvalidRecordSize,
    "Record size exceeds the maximum size limit")
MAKE_EX_1_DEFMSG(SomeInvalidArgument, InvalidResourceType, String, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeInvalidArgument, InvalidShardOffset, String, API.ErrorCodeStreamInvalidOffset)
MAKE_EX_1_DEFMSG(SomeInvalidArgument, InvalidTrimPoint, String, API.ErrorCodeStreamInvalidOffset)
MAKE_EX_0_DEFMSG(SomeInvalidArgument, InvalidSubscriptionOffset, API.ErrorCodeSubscriptionInvalidOffset)
MAKE_EX_1_DEFMSG(SomeInvalidArgument, DecodeHStreamRecordErr, String, API.ErrorCodeInternalError)
MAKE_EX_0(SomeInvalidArgument, NoRecordHeader, API.ErrorCodeInternalError,
    "HStreamRecord doesn't have a header.")
MAKE_EX_0_DEFMSG(SomeInvalidArgument, UnknownCompressionType, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeInvalidArgument, InvalidStatsType, String, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeInvalidArgument, InvalidStatsInterval, String, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeInvalidArgument, InvalidSqlStatement, String, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeInvalidArgument, InvalidConnectorType, Text, API.ErrorCodeConnectorInvalidType)
MAKE_EX_1_DEFMSG(SomeInvalidArgument, SQLNotSupportedByParseSQL, Text, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeInvalidArgument, InvalidQuerySql, String, API.ErrorCodeQueryInvalidSQL)
MAKE_EX_1_DEFMSG(SomeInvalidArgument, ConflictShardReaderOffset, String, API.ErrorCodeShardReaderConflictOffset)
MAKE_EX_1_DEFMSG(SomeInvalidArgument, TooManyShardCount, String, API.ErrorCodeShardReaderTooManyShards)

invalidIdentifier :: API.ResourceType -> String -> InvalidObjectIdentifier
invalidIdentifier API.ResourceTypeResStream       = InvalidObjectIdentifier API.ErrorCodeStreamInvalidObjectIdentifier
invalidIdentifier API.ResourceTypeResSubscription = InvalidObjectIdentifier API.ErrorCodeSubscriptionInvalidObjectIdentifier
invalidIdentifier API.ResourceTypeResShard        = InvalidObjectIdentifier API.ErrorCodeInternalError
invalidIdentifier API.ResourceTypeResShardReader  = InvalidObjectIdentifier API.ErrorCodeShardReaderInvalidObjectIdentifier
invalidIdentifier API.ResourceTypeResConnector    = InvalidObjectIdentifier API.ErrorCodeConnectorInvalidObjectIdentifier
invalidIdentifier API.ResourceTypeResQuery        = InvalidObjectIdentifier API.ErrorCodeQueryInvalidObjectIdentifier
invalidIdentifier API.ResourceTypeResView         = InvalidObjectIdentifier API.ErrorCodeViewInvalidObjectIdentifier

-------------------------------------------------------------------------------
-- Exception: SomeDeadlineExceeded

-- $deadlineExceeded
--
-- The deadline expired before the operation could complete. For operations
-- that change the state of the system, this error may be returned even if the
-- operation has completed successfully. For example, a successful response
-- from a server could have been delayed long.
MAKE_SUB_EX(SomeHServerException, SomeDeadlineExceeded)

-------------------------------------------------------------------------------
-- Exception: SomeNotFound

-- $notFound
--
-- Some requested entity (e.g., stream or subscription) was not found.
-- Note to server developers: if a request is denied for an entire class of
-- users, such as gradual feature rollout or undocumented allowlist, NotFound
-- may be used. If a request is denied for some users within a class of users,
-- such as user-based access control, PermissionDenied must be used.
MAKE_SUB_EX(SomeHServerException, SomeNotFound)

MAKE_EX_1_DEFMSG(SomeNotFound, NodesNotFound, Text, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeNotFound, StreamNotFound, Text, API.ErrorCodeStreamNotFound)
MAKE_EX_1_DEFMSG(SomeNotFound, SubscriptionNotFound, Text, API.ErrorCodeSubscriptionNotFound)
MAKE_EX_1_DEFMSG(SomeNotFound, ConnectorNotFound, Text, API.ErrorCodeConnectorNotFound)
MAKE_EX_1_DEFMSG(SomeNotFound, ViewNotFound, Text, API.ErrorCodeQueryNotFound)
MAKE_EX_1_DEFMSG(SomeNotFound, ShardNotFound, Text, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeNotFound, QueryNotFound, Text, API.ErrorCodeQueryNotFound)
MAKE_EX_1_DEFMSG(SomeNotFound, RQLiteTableNotFound, String, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeNotFound, RQLiteRowNotFound, String, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeNotFound, LocalMetaStoreTableNotFound, String, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeNotFound, LocalMetaStoreObjectNotFound, String, API.ErrorCodeInternalError)

-------------------------------------------------------------------------------
-- Exception: SomeAlreadyExists

-- $alreadyExists
--
-- The entity that a client attempted to create (e.g., stream) already exists.
MAKE_SUB_EX(SomeHServerException, SomeAlreadyExists)

MAKE_EX_1_DEFMSG(SomeAlreadyExists, StreamExists, Text, API.ErrorCodeStreamExists)
MAKE_EX_1_DEFMSG(SomeAlreadyExists, SubscriptionExists, Text, API.ErrorCodeSubscriptionExists)
MAKE_EX_1_DEFMSG(SomeAlreadyExists, QueryExists, Text, API.ErrorCodeQueryExists)
MAKE_EX_1_DEFMSG(SomeAlreadyExists, ViewExists, Text, API.ErrorCodeViewExists)
MAKE_EX_1_DEFMSG(SomeAlreadyExists, ConsumerExists, String, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeAlreadyExists, ShardReaderExists, Text, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeAlreadyExists, RQLiteTableAlreadyExists, String, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeAlreadyExists, RQLiteRowAlreadyExists, String, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeAlreadyExists, LocalMetaStoreTableAlreadyExists, String, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeAlreadyExists, LocalMetaStoreObjectAlreadyExists, String, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeAlreadyExists, ConnectorExists, Text, API.ErrorCodeConnectorExists)

-------------------------------------------------------------------------------
-- Exception: SomePermissionDenied

-- $permissionDenied
--
-- The caller does not have permission to execute the specified operation.
-- PERMISSION_DENIED must not be used for rejections caused by exhausting some
-- resource (use RESOURCE_EXHAUSTED instead for those errors).
-- PERMISSION_DENIED must not be used if the caller can not be identified
-- (use UNAUTHENTICATED instead for those errors). This error code does not
-- imply the request is valid or the requested entity exists or satisfies other
-- pre-conditions.
MAKE_SUB_EX(SomeHServerException, SomePermissionDenied)

-------------------------------------------------------------------------------
-- Exception: SomeResourceExhausted

-- $resourceExhausted
--
-- Some resource has been exhausted, perhaps a per-user quota, or perhaps the
-- entire file system is out of space.
MAKE_SUB_EX(SomeHServerException, SomeResourceExhausted)

MAKE_EX_0(SomeResourceExhausted, NoMoreSlots, API.ErrorCodeInternalError,
    "This slot group is full!")

-------------------------------------------------------------------------------
-- Exception: SomeFailedPrecondition

-- $failedPrecondition
--
-- The operation was rejected because the system is not in a state required for
-- the operation's execution. For example, the directory to be deleted is
-- non-empty, an rmdir operation is applied to a non-directory, etc.
--
-- Service implementors can use the following guidelines to decide between
-- 'FailedPrecondition', 'Aborted', and 'Unavailable':
--
-- (a) Use 'Unavailable' if the client can retry just the failing call.
-- (b) Use 'Aborted' if the client should retry at a higher level (e.g., when a
--     client-specified test-and-set fails, indicating the client should restart
--     a read-modify-write sequence).
-- (c) Use 'FailedPrecondition' if the client should not retry until the system
--     state has been explicitly fixed. E.g., if an "rmdir" fails because the
--     directory is non-empty, FailedPrecondition should be returned since the
--     client should not retry unless the files are deleted from the directory.
MAKE_SUB_EX(SomeHServerException, SomeFailedPrecondition)

MAKE_EX_0(SomeFailedPrecondition, FoundSubscription, API.ErrorCodeStreamFoundSubscription,
    "Stream still has subscription")
MAKE_EX_1_DEFMSG(SomeFailedPrecondition, EmptyShardReader, String, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeFailedPrecondition, NonExistentStream, Text, API.ErrorCodeSubscriptionCreationOnNonExistentStream)
MAKE_EX_1_DEFMSG(SomeFailedPrecondition, WrongServer, String, API.ErrorCodeWrongServer)
MAKE_EX_1_DEFMSG(SomeFailedPrecondition, WrongExecutionPlan, String, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeFailedPrecondition, FoundActiveConsumers, String, API.ErrorCodeSubscriptionFoundActiveConsumers)
MAKE_EX_1_DEFMSG(SomeFailedPrecondition, ShardCanNotSplit, String, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeFailedPrecondition, ShardCanNotMerge, String, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeFailedPrecondition, RQLiteRowBadVersion, String, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeFailedPrecondition, LocalMetaStoreObjectBadVersion, String, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeFailedPrecondition, QueryNotTerminated, Text, API.ErrorCodeQueryNotTerminated)
MAKE_EX_1_DEFMSG(SomeFailedPrecondition, QueryNotAborted, Text, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeFailedPrecondition, FoundAssociatedView, Text, API.ErrorCodeQueryFoundAssociatedView)
MAKE_EX_1_DEFMSG(SomeFailedPrecondition, QueryAlreadyTerminated, Text, API.ErrorCodeQueryAlreadyTerminated)
MAKE_EX_1_DEFMSG(SomeFailedPrecondition, ConnectorCheckFailed, Aeson.Value, API.ErrorCodeConnectorCheckFailed)
MAKE_EX_1_DEFMSG(SomeFailedPrecondition, ConnectorInvalidStatus, Text, API.ErrorCodeInternalError)

-------------------------------------------------------------------------------
-- Exception: SomeAborted

-- $aborted
--
-- The operation was aborted, typically due to a concurrency issue such as a
-- sequencer check failure or transaction abort. See the guidelines above for
-- deciding between 'FailedPrecondition', 'Aborted', and 'Unavailable'.
MAKE_SUB_EX(SomeHServerException, SomeAborted)

MAKE_EX_1_DEFMSG(SomeAborted, SubscriptionIsDeleting, String, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeAborted, SubscriptionInvalidError, String, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeAborted, ConsumerInvalidError, String, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeAborted, QueryNotRunning, Text, API.ErrorCodeQueryNotRunning)

-------------------------------------------------------------------------------
-- Exception: SomeOutOfRange

-- $outOfRange
--
-- The operation was attempted past the valid range. E.g., seeking or reading
-- past end-of-file. Unlike INVALID_ARGUMENT, this error indicates a problem
-- that may be fixed if the system state changes. For example, a 32-bit file
-- system will generate INVALID_ARGUMENT if asked to read at an offset that is
-- not in the range [0,2^32-1], but it will generate OUT_OF_RANGE if asked to
-- read from an offset past the current file size. There is a fair bit of
-- overlap between FAILED_PRECONDITION and OUT_OF_RANGE. We recommend using
-- OUT_OF_RANGE (the more specific error) when it applies so that callers who
-- are iterating through a space can easily look for an OUT_OF_RANGE error to
-- detect when they are done.
MAKE_SUB_EX(SomeHServerException, SomeOutOfRange)

-------------------------------------------------------------------------------
-- Exception: SomeInternal

-- $internal
--
-- Internal errors. This means that some invariants expected by the underlying
-- system have been broken. This error code is reserved for serious errors.
MAKE_SUB_EX(SomeHServerException, SomeInternal)

MAKE_EX_1_DEFMSG(SomeInternal, UnexpectedError, String, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeInternal, WrongOffset, String, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeInternal, ZstdCompresstionErr, String, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeInternal, RQLiteNetworkErr, String, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeInternal, RQLiteDecodeErr, String, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeInternal, RQLiteUnspecifiedErr, String, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeInternal, LocalMetaStoreInternalErr, String, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeInternal, SlotAllocDecodeError, String, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeInternal, DiscardedMethod, String, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeInternal, PushQuerySendError, String, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeInternal, ConnectorProcessError, String, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeInternal, SomeStoreInternal, String, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeInternal, SomeServerInternal, String, API.ErrorCodeInternalError)

-------------------------------------------------------------------------------
-- Exception: SomeUnavailable

-- $unavailable
--
-- The service is currently unavailable. This is most likely a transient
-- condition, which can be corrected by retrying with a backoff.
--
-- Note that it is not always safe to retry non-idempotent operations.
MAKE_SUB_EX(SomeHServerException, SomeUnavailable)

MAKE_EX_0(SomeUnavailable, ServerNotAvailable, API.ErrorCodeInternalError, "ServerNotAvailable")
MAKE_EX_1_DEFMSG(SomeUnavailable, ResourceAllocationException, String, API.ErrorCodeInternalError)

-------------------------------------------------------------------------------
-- Exception: SomeDataLoss

-- $dataLoss
--
-- Unrecoverable data loss or corruption.
MAKE_SUB_EX(SomeHServerException, SomeDataLoss)

-------------------------------------------------------------------------------
-- Exception: SomeUnauthenticated

-- $unauthenticated
--
-- The request does not have valid authentication credentials for the operation.
--
-- Incorrect Auth metadata (Credentials failed to get metadata, Incompatible
-- credentials set on channel and call, Invalid host set in :authority metadata,
-- etc.)
MAKE_SUB_EX(SomeHServerException, SomeUnauthenticated)

MAKE_EX_1_DEFMSG(SomeUnauthenticated, RQLiteNoAuth, String, API.ErrorCodeInternalError)

-------------------------------------------------------------------------------
-- Exception: SomeUnimplemented

-- $unimplemented
--
-- The operation is not implemented or is not supported/enabled in this service.
MAKE_SUB_EX(SomeHServerException, SomeUnimplemented)

MAKE_EX_1_DEFMSG(SomeUnimplemented, ExecPlanUnimplemented, String, API.ErrorCodeInternalError)
MAKE_EX_1_DEFMSG(SomeUnimplemented, ConnectorUnimplemented, Text, API.ErrorCodeConnectorUnimplemented)

-------------------------------------------------------------------------------
-- Handlers

mkExceptionHandle :: [E.Handler a] -> IO a -> IO a
mkExceptionHandle = flip E.catches

mkExceptionHandle'
  :: (forall e. Exception e => e -> IO ())
  -> [E.Handler a] -> IO a -> IO a
mkExceptionHandle' whileEx handlers f =
  let handlers' = map (\(E.Handler h) -> E.Handler (\e -> whileEx e >> h e)) handlers
   in f `E.catches` handlers'

-------------------------------------------------------------------------------
-- Handlers (grpc-haskell)

type MkResp t a = StatusCode -> StatusDetails -> ServerResponse t a
type ExceptionHandle a = IO a -> IO a

setRespType :: MkResp t a
            -> [E.Handler (StatusCode, StatusDetails)]
            -> [E.Handler (ServerResponse t a)]
setRespType mkResp = map (uncurry mkResp <$>)

mkStatusDetails :: Exception a => a -> StatusDetails
mkStatusDetails = StatusDetails . BSC.pack . displayException

defaultHServerExHandlers :: [E.Handler (StatusCode, StatusDetails)]
defaultHServerExHandlers =
  [ E.Handler $ \(err :: SomeInvalidArgument) -> do
      Log.warning $ Log.buildString' err
      return (StatusInvalidArgument, mkStatusDetails err)

  , E.Handler $ \(err :: SomeCancelled) -> do
      Log.fatal $ Log.buildString' err
      return (StatusCancelled, mkStatusDetails err)

  , E.Handler $ \(err :: SomeUnknown) -> do
      Log.fatal $ Log.buildString' err
      return (StatusUnknown, mkStatusDetails err)

  , E.Handler $ \(err :: SomeDeadlineExceeded) -> do
      Log.fatal $ Log.buildString' err
      return (StatusDeadlineExceeded, mkStatusDetails err)

  , E.Handler $ \(err :: SomeNotFound) -> do
      Log.warning $ Log.buildString' err
      return (StatusNotFound, mkStatusDetails err)

  , E.Handler $ \(err :: SomeAlreadyExists) -> do
      Log.warning $ Log.buildString' err
      return (StatusAlreadyExists, mkStatusDetails err)

  , E.Handler $ \(err :: SomePermissionDenied) -> do
      Log.warning $ Log.buildString' err
      return (StatusPermissionDenied, mkStatusDetails err)

  , E.Handler $ \(err :: SomeResourceExhausted) -> do
      Log.warning $ Log.buildString' err
      return (StatusResourceExhausted, mkStatusDetails err)

  , E.Handler $ \(err :: SomeFailedPrecondition) -> do
      Log.warning $ Log.buildString' err
      return (StatusFailedPrecondition, mkStatusDetails err)

  , E.Handler $ \(err :: SomeAborted) -> do
      Log.warning $ Log.buildString' err
      return (StatusAborted, mkStatusDetails err)

  , E.Handler $ \(err :: SomeOutOfRange) -> do
      Log.warning $ Log.buildString' err
      return (StatusOutOfRange, mkStatusDetails err)

  , E.Handler $ \(err :: SomeUnavailable) -> do
      Log.fatal $ Log.buildString' err
      return (StatusUnavailable, mkStatusDetails err)

  , E.Handler $ \(err :: SomeDataLoss) -> do
      Log.fatal $ Log.buildString' err
      return (StatusDataLoss, mkStatusDetails err)

  , E.Handler $ \(err :: SomeInternal) -> do
      Log.fatal $ Log.buildString' err
      return (StatusInternal, mkStatusDetails err)

  , E.Handler $ \(err :: SomeUnauthenticated) -> do
      Log.warning $ Log.buildString' err
      return (StatusUnauthenticated, mkStatusDetails err)

  , E.Handler $ \(err :: SomeUnimplemented) -> do
      Log.warning $ Log.buildString' err
      return (StatusUnimplemented, mkStatusDetails err)
  ]

zkExceptionHandlers :: [E.Handler (StatusCode, StatusDetails)]
zkExceptionHandlers =
  [ E.Handler $ \(e :: ZK.ZCONNECTIONLOSS    )   -> handleZKException e StatusUnavailable
  , E.Handler $ \(e :: ZK.ZBADARGUMENTS      )   -> handleZKException e StatusInvalidArgument
  , E.Handler $ \(e :: ZK.ZSSLCONNECTIONERROR)   -> handleZKException e StatusFailedPrecondition
  , E.Handler $ \(e :: ZK.ZRECONFIGINPROGRESS)   -> handleZKException e StatusUnavailable
  , E.Handler $ \(e :: ZK.ZINVALIDSTATE      )   -> handleZKException e StatusUnavailable
  , E.Handler $ \(e :: ZK.ZOPERATIONTIMEOUT  )   -> handleZKException e StatusAborted
  , E.Handler $ \(e :: ZK.ZDATAINCONSISTENCY )   -> handleZKException e StatusAborted
  , E.Handler $ \(e :: ZK.ZRUNTIMEINCONSISTENCY) -> handleZKException e StatusAborted
  , E.Handler $ \(e :: ZK.ZSYSTEMERROR       )   -> handleZKException e StatusInternal
  , E.Handler $ \(e :: ZK.ZMARSHALLINGERROR  )   -> handleZKException e StatusUnknown
  , E.Handler $ \(e :: ZK.ZUNIMPLEMENTED     )   -> handleZKException e StatusUnknown
  , E.Handler $ \(e :: ZK.ZNEWCONFIGNOQUORUM )   -> handleZKException e StatusUnknown
  , E.Handler $ \(e :: ZK.ZNODEEXISTS        )   -> do
      Log.fatal $ Log.buildString' e
      return (StatusAlreadyExists, "object exists")
  , E.Handler $ \(e :: ZK.ZNONODE            )   -> handleZKException e StatusNotFound
  , E.Handler $ \(e :: ZK.ZooException       )   -> handleZKException e StatusInternal
  ]
  where
    handleZKException :: Exception a => a -> StatusCode -> IO (StatusCode, StatusDetails)
    handleZKException e status = do
      Log.fatal $ Log.buildString' e
      return (status, "Zookeeper exception: " <> mkStatusDetails e)

-------------------------------------------------------------------------------
-- Handlers (hs-grpc-server)

mkGrpcStatus :: Exception a => a -> HsGrpc.StatusCode -> HsGrpc.GrpcStatus
mkGrpcStatus e code = HsGrpc.GrpcStatus code (mkStatusMsg e) Nothing
{-# INLINE mkGrpcStatus #-}

mkStatusMsg :: Exception a => a -> Maybe ByteString
mkStatusMsg = Just . BSC.pack . displayException
{-# INLINE mkStatusMsg #-}

hServerExHandlers :: [E.Handler a]
hServerExHandlers =
  [ E.Handler $ \(err :: SomeInvalidArgument) -> do
      Log.warning $ Log.buildString' err
      HsGrpc.throwGrpcError $ mkGrpcStatus err HsGrpc.StatusInvalidArgument

  , E.Handler $ \(err :: SomeCancelled) -> do
      Log.fatal $ Log.buildString' err
      HsGrpc.throwGrpcError $ mkGrpcStatus err HsGrpc.StatusCancelled

  , E.Handler $ \(err :: SomeUnknown) -> do
      Log.fatal $ Log.buildString' err
      HsGrpc.throwGrpcError $ mkGrpcStatus err HsGrpc.StatusUnknown

  , E.Handler $ \(err :: SomeDeadlineExceeded) -> do
      Log.fatal $ Log.buildString' err
      HsGrpc.throwGrpcError $ mkGrpcStatus err HsGrpc.StatusDeadlineExceeded

  , E.Handler $ \(err :: SomeNotFound) -> do
      Log.warning $ Log.buildString' err
      HsGrpc.throwGrpcError $ mkGrpcStatus err HsGrpc.StatusNotFound

  , E.Handler $ \(err :: SomeAlreadyExists) -> do
      Log.warning $ Log.buildString' err
      HsGrpc.throwGrpcError $ mkGrpcStatus err HsGrpc.StatusAlreadyExists

  , E.Handler $ \(err :: SomePermissionDenied) -> do
      Log.warning $ Log.buildString' err
      HsGrpc.throwGrpcError $ mkGrpcStatus err HsGrpc.StatusPermissionDenied

  , E.Handler $ \(err :: SomeResourceExhausted) -> do
      Log.warning $ Log.buildString' err
      HsGrpc.throwGrpcError $ mkGrpcStatus err HsGrpc.StatusResourceExhausted

  , E.Handler $ \(err :: SomeFailedPrecondition) -> do
      Log.fatal $ Log.buildString' err
      HsGrpc.throwGrpcError $ mkGrpcStatus err HsGrpc.StatusFailedPrecondition

  , E.Handler $ \(err :: SomeAborted) -> do
      Log.warning $ Log.buildString' err
      HsGrpc.throwGrpcError $ mkGrpcStatus err HsGrpc.StatusAborted

  , E.Handler $ \(err :: SomeOutOfRange) -> do
      Log.warning $ Log.buildString' err
      HsGrpc.throwGrpcError $ mkGrpcStatus err HsGrpc.StatusOutOfRange

  , E.Handler $ \(err :: SomeUnavailable) -> do
      Log.fatal $ Log.buildString' err
      HsGrpc.throwGrpcError $ mkGrpcStatus err HsGrpc.StatusUnavailable

  , E.Handler $ \(err :: SomeDataLoss) -> do
      Log.fatal $ Log.buildString' err
      HsGrpc.throwGrpcError $ mkGrpcStatus err HsGrpc.StatusDataLoss

  , E.Handler $ \(err :: SomeInternal) -> do
      Log.fatal $ Log.buildString' err
      HsGrpc.throwGrpcError $ mkGrpcStatus err HsGrpc.StatusInternal

  , E.Handler $ \(err :: SomeUnauthenticated) -> do
      Log.warning $ Log.buildString' err
      HsGrpc.throwGrpcError $ mkGrpcStatus err HsGrpc.StatusUnauthenticated

  , E.Handler $ \(err :: SomeUnimplemented) -> do
      Log.warning $ Log.buildString' err
      HsGrpc.throwGrpcError $ mkGrpcStatus err HsGrpc.StatusUnimplemented
  ]

zkExHandlers :: [E.Handler a]
zkExHandlers =
  [ E.Handler $ \(e :: ZK.ZCONNECTIONLOSS    )   -> handleZKException e HsGrpc.StatusUnavailable
  , E.Handler $ \(e :: ZK.ZBADARGUMENTS      )   -> handleZKException e HsGrpc.StatusInvalidArgument
  , E.Handler $ \(e :: ZK.ZSSLCONNECTIONERROR)   -> handleZKException e HsGrpc.StatusFailedPrecondition
  , E.Handler $ \(e :: ZK.ZRECONFIGINPROGRESS)   -> handleZKException e HsGrpc.StatusUnavailable
  , E.Handler $ \(e :: ZK.ZINVALIDSTATE      )   -> handleZKException e HsGrpc.StatusUnavailable
  , E.Handler $ \(e :: ZK.ZOPERATIONTIMEOUT  )   -> handleZKException e HsGrpc.StatusAborted
  , E.Handler $ \(e :: ZK.ZDATAINCONSISTENCY )   -> handleZKException e HsGrpc.StatusAborted
  , E.Handler $ \(e :: ZK.ZRUNTIMEINCONSISTENCY) -> handleZKException e HsGrpc.StatusAborted
  , E.Handler $ \(e :: ZK.ZSYSTEMERROR       )   -> handleZKException e HsGrpc.StatusInternal
  , E.Handler $ \(e :: ZK.ZMARSHALLINGERROR  )   -> handleZKException e HsGrpc.StatusUnknown
  , E.Handler $ \(e :: ZK.ZUNIMPLEMENTED     )   -> handleZKException e HsGrpc.StatusUnknown
  , E.Handler $ \(e :: ZK.ZNEWCONFIGNOQUORUM )   -> handleZKException e HsGrpc.StatusUnknown
  , E.Handler $ \(e :: ZK.ZNODEEXISTS        )   -> handleZKException e HsGrpc.StatusAlreadyExists
  , E.Handler $ \(e :: ZK.ZNONODE            )   -> handleZKException e HsGrpc.StatusNotFound
  , E.Handler $ \(e :: ZK.ZooException       )   -> handleZKException e HsGrpc.StatusInternal
  ]
  where
    handleZKException :: Exception e => e -> HsGrpc.StatusCode -> IO a
    handleZKException err code = do
      Log.fatal $ Log.buildString' err
      HsGrpc.throwGrpcError $ mkGrpcStatus err code
