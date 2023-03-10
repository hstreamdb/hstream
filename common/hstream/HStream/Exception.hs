{-# LANGUAGE CPP                #-}
{-# LANGUAGE GADTs              #-}
{-# LANGUAGE RankNTypes         #-}
{-# OPTIONS_GHC -pgmPcpphs -optP--cpp #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE TemplateHaskell    #-}

module HStream.Exception
  ( SomeHStreamException
    -- Root of Server Exception
  , SomeHServerException (..)

    -- * Exception: SomeCancelled
    --
    -- $cancelled
  , SomeCancelled
  , StreamReadError (..)
  , StreamReadClose (..)
  , StreamWriteError (..)

    -- * Exception: SomeUnknown
    --
    -- $unknown
  , SomeUnknown
  , UnknownPushQueryStatus (..)

    -- * Exception: SomeInvalidArgument
    --
    -- $invalidArgument
  , SomeInvalidArgument
  , InvalidReplicaFactor (..)
  , InvalidObjectIdentifier (..)
  , InvalidShardCount (..)
  , EmptyBatchedRecord (..)
  , InvalidRecordSize (..)
  , InvalidResourceType (..)
  , InvalidShardOffset (..)
  , InvalidSubscriptionOffset (..)
  , DecodeHStreamRecordErr (..)
  , NoRecordHeader (..)
  , UnknownCompressionType (..)
  , InvalidStatsType (..)
  , InvalidStatsInterval (..)
  , InvalidSqlStatement (..)

    -- * Exception: SomeDeadlineExceeded
    --
    -- $deadlineExceeded
  , SomeDeadlineExceeded

    -- * Exception: SomeNotFound
    --
    -- $notFound
  , SomeNotFound
  , NodesNotFound (..)
  , StreamNotFound (..)
  , SubscriptionNotFound (..)
  , ConnectorNotFound (..)
  , ViewNotFound (..)
  , ShardNotFound (..)
  , QueryNotFound (..)
  , RQLiteTableNotFound (..)
  , RQLiteRowNotFound (..)
  , LocalMetaStoreTableNotFound (..)
  , LocalMetaStoreObjectNotFound (..)

    -- * Exception: SomeAlreadyExists
    --
    -- $alreadyExists
  , SomeAlreadyExists
  , StreamExists (..)
  , SubscriptionExists (..)
  , ConsumerExists (..)
  , ShardReaderExists (..)
  , RQLiteTableAlreadyExists (..)
  , RQLiteRowAlreadyExists (..)
  , LocalMetaStoreTableAlreadyExists (..)
  , LocalMetaStoreObjectAlreadyExists (..)
  , PushQueryCreated (..)

    -- * Exception: SomePermissionDenied
    --
    -- $permissionDenied
  , SomePermissionDenied

    -- * Exception: SomeResourceExhausted
    --
    -- $resourceExhausted
  , SomeResourceExhausted

    -- * Exception: SomeFailedPrecondition
    --
    -- $failedPrecondition
  , SomeFailedPrecondition
  , FoundSubscription (..)
  , EmptyShardReader (..)
  , EmptyStream (..)
  , WrongServer (..)
  , WrongExecutionPlan (..)
  , FoundActiveConsumers (..)
  , ShardCanNotSplit (..)
  , ShardCanNotMerge (..)
  , RQLiteRowBadVersion (..)
  , LocalMetaStoreObjectBadVersion (..)
  , ResourceAllocationException (..)

    -- * Exception: SomeAborted
    --
    -- $aborted
  , SomeAborted
  , SubscriptionIsDeleting (..)
  , SubscriptionOnDifferentNode (..)
  , SubscriptionInvalidError (..)
  , ConsumerInvalidError (..)
  , TerminateQueriesError (..)
  , PushQueryTerminated (..)

    -- * Exception: SomeOutOfRange
    --
    -- $outOfRange
  , SomeOutOfRange

    -- * Exception: SomeUnimplemented
    --
    -- $unimplemented
  , SomeUnimplemented
  , ExecPlanUnimplemented (..)

    -- * Exception: SomeInternal
    --
    -- $internal
  , SomeInternal
  , UnexpectedError (..)
  , WrongOffset (..)
  , ZstdCompresstionErr (..)
  , RQLiteNetworkErr (..)
  , RQLiteDecodeErr (..)
  , RQLiteUnspecifiedErr (..)
  , LocalMetaStoreInternalErr (..)
  , DiscardedMethod (..)
  , PushQuerySendError (..)

    -- * Exception: SomeUnavailable
  , SomeUnavailable
  , ServerNotAvailable (..)

    -- * Exception: SomeDataLoss
  , SomeDataLoss

    -- * Exception: SomeUnauthenticated
    --
    -- $unauthenticated
  , SomeUnauthenticated
  , RQLiteNoAuth (..)

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

  , -- * Re-export
  E.Handler
  ) where

import           Control.Exception             (Exception (..))
import qualified Control.Exception             as E
import           Data.ByteString               (ByteString)
import qualified Data.ByteString.Char8         as BSC
import qualified Data.ByteString.Lazy.Char8    as BSLC
import           Data.Text                     (Text)
import qualified Data.Text                     as Text
import           Data.Typeable                 (cast)
import           GHC.Stack                     (CallStack, prettyCallStack)
import qualified HsGrpc.Server.Types           as HsGrpc
import           Network.GRPC.HighLevel.Client (StatusCode (..),
                                                StatusDetails (..))
import           Network.GRPC.HighLevel.Server (ServerResponse (..))
import qualified ZooKeeper.Exception           as ZK

import qualified Data.Aeson                    as J
import qualified Data.Aeson.TH                 as JT
import qualified HStream.Logger                as Log
import qualified HStream.Server.HStreamApi     as API
import qualified Proto3.Wire                   as HsProtobuf

-------------------------------------------------------------------------------
-- The root exception type for all the exceptions in a hstream server

data ErrBody = ErrBody {
    error   :: API.ErrorCode,
    message :: Text,
    extra   :: J.Value
  } deriving (Show, Eq)

instance J.ToJSON ErrBody where
  toJSON ErrBody{..} = J.object [
      "error" J..= (HsProtobuf.fromProtoEnum error),
      "message" J..= message,
      "extra" J..= extra
    ]

errBodyToStr :: ErrBody -> String
errBodyToStr body = BSLC.unpack $ J.encode body

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
  displayException (SUB_E e) = displayException e                              \
};                                                                             \
a##SUB_E##ToException :: Exception e => e -> E.SomeException;                  \
a##SUB_E##ToException = E.toException . SUB_E;                                 \
a##SUB_E##FromException :: Exception e => E.SomeException -> Maybe e;          \
a##SUB_E##FromException x = do SUB_E a <- E.fromException x; cast a;

#define MAKE_STD_EX_0(ExCls, Name, TCode) \
data Name = Name deriving (Show);                                \
instance Exception Name where                                       \
{ toException = a##ExCls##ToException;                              \
  fromException = a##ExCls##FromException;                          \
  displayException Name = errBodyToStr $ ErrBody TCode #Name J.Null;\
};                                                                  \

#define MAKE_STD_EX_0_WITH_MSG(ExCls, Name, TCode, TMsg) \
data Name = Name deriving (Show);                                \
instance Exception Name where                                       \
{ toException = a##ExCls##ToException;                              \
  fromException = a##ExCls##FromException;                          \
  displayException Name = errBodyToStr $ ErrBody TCode TMsg J.Null; \
};                                                                  \

#define MAKE_STD_EX_1(ExCls, Name, Ty, TCode) \
newtype Name = Name Ty deriving (Show);                                       \
instance Exception Name where                                                 \
{ toException = a##ExCls##ToException;                                        \
  fromException = a##ExCls##FromException;                                    \
  displayException (Name s) = errBodyToStr $ ErrBody TCode #Name (J.toJSON s);\
};                                                                            \

#define MAKE_STD_EX_1_WITH_MSG(ExCls, Name, Ty, TCode, TMsg) \
newtype Name = Name Ty deriving (Show);                                       \
instance Exception Name where                                                 \
{ toException = a##ExCls##ToException;                                        \
  fromException = a##ExCls##FromException;                                    \
  displayException (Name s) = errBodyToStr $ ErrBody TCode TMsg (J.toJSON s); \
};                                                                            \

-------------------------------------------------------------------------------

MAKE_SUB_EX(SomeHStreamException, SomeHServerException)

-------------------------------------------------------------------------------
-- Exception: SomeCancelled

-- $cancelled
--
-- The operation was cancelled, typically by the caller.
MAKE_SUB_EX(SomeHServerException, SomeCancelled)

MAKE_STD_EX_1(SomeCancelled, StreamReadError, String, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeCancelled, StreamReadClose, String, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeCancelled, StreamWriteError, String, API.ErrorCodeInternalError)

-------------------------------------------------------------------------------
-- Exception: SomeUnknown

-- $unknown
--
-- Unknown error. For example, this error may be returned when a Status value
-- received from another address space belongs to an error space that is not
-- known in this address space. Also errors raised by APIs that do not return
-- enough error information may be converted to this error.
MAKE_SUB_EX(SomeHServerException, SomeUnknown)

MAKE_STD_EX_1(SomeUnknown, UnknownPushQueryStatus, String, API.ErrorCodeInternalError)

-------------------------------------------------------------------------------
-- Exception: SomeInvalidArgument

-- $invalidArgument
--
-- The client specified an invalid argument. Note that 'InvalidArgument'
-- indicates arguments that are problematic regardless of the state of the
-- system.
MAKE_SUB_EX(SomeHServerException, SomeInvalidArgument)

MAKE_STD_EX_1(SomeInvalidArgument, InvalidReplicaFactor, String, API.ErrorCodeStreamInvalidReplicaFactor)
MAKE_STD_EX_1(SomeInvalidArgument, InvalidObjectIdentifier, String, API.ErrorCodeStreamInvalidObjectIdentifier)
MAKE_STD_EX_1(SomeInvalidArgument, InvalidShardCount, String, API.ErrorCodeStreamInvalidShardCount)
MAKE_STD_EX_0_WITH_MSG(SomeInvalidArgument, EmptyBatchedRecord, API.ErrorCodeStreamEmptyBatchedRecord,
    "BatchedRecord shouldn't be Nothing")
MAKE_STD_EX_1_WITH_MSG(SomeInvalidArgument, InvalidRecordSize, Int, API.ErrorCodeStreamInvalidRecordSize,
    "Record size exceeds the maximum size limit")
MAKE_STD_EX_1(SomeInvalidArgument, InvalidResourceType, String, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeInvalidArgument, InvalidShardOffset, String, API.ErrorCodeInternalError)
MAKE_STD_EX_0(SomeInvalidArgument, InvalidSubscriptionOffset, API.ErrorCodeSubscriptionInvalidOffset)
MAKE_STD_EX_1(SomeInvalidArgument, DecodeHStreamRecordErr, String, API.ErrorCodeInternalError)
MAKE_STD_EX_0_WITH_MSG(SomeInvalidArgument, NoRecordHeader, API.ErrorCodeInternalError,
    "HStreamRecord doesn't have a header.")
MAKE_STD_EX_0(SomeInvalidArgument, UnknownCompressionType, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeInvalidArgument, InvalidStatsType, String, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeInvalidArgument, InvalidStatsInterval, String, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeInvalidArgument, InvalidSqlStatement, String, API.ErrorCodeInternalError)

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

MAKE_STD_EX_1(SomeNotFound, NodesNotFound, Text, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeNotFound, StreamNotFound, Text, API.ErrorCodeStreamNotFound)
MAKE_STD_EX_1(SomeNotFound, SubscriptionNotFound, Text, API.ErrorCodeSubscriptionNotFound)
MAKE_STD_EX_1(SomeNotFound, ConnectorNotFound, Text, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeNotFound, ViewNotFound, Text, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeNotFound, ShardNotFound, Text, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeNotFound, QueryNotFound, Text, API.ErrorCodeQueryNotFound)
MAKE_STD_EX_1(SomeNotFound, RQLiteTableNotFound, String, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeNotFound, RQLiteRowNotFound, String, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeNotFound, LocalMetaStoreTableNotFound, String, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeNotFound, LocalMetaStoreObjectNotFound, String, API.ErrorCodeInternalError)

-------------------------------------------------------------------------------
-- Exception: SomeAlreadyExists

-- $alreadyExists
--
-- The entity that a client attempted to create (e.g., stream) already exists.
MAKE_SUB_EX(SomeHServerException, SomeAlreadyExists)

MAKE_STD_EX_1(SomeAlreadyExists, StreamExists, Text, API.ErrorCodeStreamExists)
MAKE_STD_EX_1(SomeAlreadyExists, SubscriptionExists, Text, API.ErrorCodeSubscriptionExists)
MAKE_STD_EX_1(SomeAlreadyExists, ConsumerExists, String, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeAlreadyExists, ShardReaderExists, Text, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeAlreadyExists, RQLiteTableAlreadyExists, String, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeAlreadyExists, RQLiteRowAlreadyExists, String, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeAlreadyExists, LocalMetaStoreTableAlreadyExists, String, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeAlreadyExists, LocalMetaStoreObjectAlreadyExists, String, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeAlreadyExists, PushQueryCreated, String, API.ErrorCodeInternalError)

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

MAKE_STD_EX_0_WITH_MSG(SomeFailedPrecondition, FoundSubscription, API.ErrorCodeStreamFoundSubscription,
    "Stream still has subscription")
MAKE_STD_EX_1(SomeFailedPrecondition, EmptyShardReader, String, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeFailedPrecondition, EmptyStream, Text, API.ErrorCodeSubscriptionCreationOnEmptyStream)
MAKE_STD_EX_1(SomeFailedPrecondition, WrongServer, String, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeFailedPrecondition, WrongExecutionPlan, String, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeFailedPrecondition, FoundActiveConsumers, String, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeFailedPrecondition, ShardCanNotSplit, String, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeFailedPrecondition, ShardCanNotMerge, String, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeFailedPrecondition, RQLiteRowBadVersion, String, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeFailedPrecondition, LocalMetaStoreObjectBadVersion, String, API.ErrorCodeInternalError)

-------------------------------------------------------------------------------
-- Exception: SomeAborted

-- $aborted
--
-- The operation was aborted, typically due to a concurrency issue such as a
-- sequencer check failure or transaction abort. See the guidelines above for
-- deciding between 'FailedPrecondition', 'Aborted', and 'Unavailable'.
MAKE_SUB_EX(SomeHServerException, SomeAborted)

MAKE_STD_EX_1(SomeAborted, SubscriptionIsDeleting, String, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeAborted, SubscriptionOnDifferentNode, String, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeAborted, SubscriptionInvalidError, String, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeAborted, ConsumerInvalidError, String, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeAborted, TerminateQueriesError, String, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeAborted, PushQueryTerminated, String, API.ErrorCodeInternalError)

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

MAKE_STD_EX_1(SomeInternal, UnexpectedError, String, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeInternal, WrongOffset, String, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeInternal, ZstdCompresstionErr, String, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeInternal, RQLiteNetworkErr, String, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeInternal, RQLiteDecodeErr, String, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeInternal, RQLiteUnspecifiedErr, String, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeInternal, LocalMetaStoreInternalErr, String, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeInternal, DiscardedMethod, String, API.ErrorCodeInternalError)
MAKE_STD_EX_1(SomeInternal, PushQuerySendError, String, API.ErrorCodeInternalError)

-------------------------------------------------------------------------------
-- Exception: SomeUnavailable

-- $unavailable
--
-- The service is currently unavailable. This is most likely a transient
-- condition, which can be corrected by retrying with a backoff.
--
-- Note that it is not always safe to retry non-idempotent operations.
MAKE_SUB_EX(SomeHServerException, SomeUnavailable)

MAKE_STD_EX_0_WITH_MSG(SomeUnavailable, ServerNotAvailable, API.ErrorCodeInternalError, "ServerNotAvailable")
MAKE_STD_EX_1(SomeUnavailable, ResourceAllocationException, String, API.ErrorCodeInternalError)

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

MAKE_STD_EX_1(SomeUnauthenticated, RQLiteNoAuth, String, API.ErrorCodeInternalError)

-------------------------------------------------------------------------------
-- Exception: SomeUnimplemented

-- $unimplemented
--
-- The operation is not implemented or is not supported/enabled in this service.
MAKE_SUB_EX(SomeHServerException, SomeUnimplemented)

MAKE_STD_EX_1(SomeUnimplemented, ExecPlanUnimplemented, String, API.ErrorCodeInternalError)

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
