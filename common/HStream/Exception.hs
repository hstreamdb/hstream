{-# LANGUAGE CPP        #-}
{-# LANGUAGE GADTs      #-}
{-# LANGUAGE RankNTypes #-}
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
  , Cancelled (..)
  , StreamReadError (..)
  , StreamReadClose (..)
  , StreamWriteError (..)

    -- * Exception: SomeUnknown
    --
    -- $unknown
  , SomeUnknown
  , Unknown (..)
  , UnknownPushQueryStatus (..)

    -- * Exception: SomeInvalidArgument
    --
    -- $invalidArgument
  , SomeInvalidArgument
  , InvalidArgument (..)
  , InvalidReplicaFactor (..)
  , InvalidShardCount (..)
  , InvalidRecord (..)
  , InvalidResourceType (..)
  , InvalidShardOffset (..)
  , InvalidSubscriptionOffset (..)
  , DecodeHStreamRecordErr (..)
  , NoRecordHeader (..)
  , UnknownCompressionType (..)
  , InvalidStatsInterval (..)
  , InvalidSqlStatement (..)

    -- * Exception: SomeDeadlineExceeded
    --
    -- $deadlineExceeded
  , SomeDeadlineExceeded
  , DeadlineExceeded (..)

    -- * Exception: SomeNotFound
    --
    -- $notFound
  , SomeNotFound
  , NotFound (..)
  , NodesNotFound (..)
  , StreamNotFound (..)
  , SubscriptionNotFound (..)
  , ConnectorNotFound (..)
  , ViewNotFound (..)
  , ShardNotFound (..)
  , QueryNotFound (..)
  , RQLiteTableNotFound (..)
  , RQLiteRowNotFound (..)

    -- * Exception: SomeAlreadyExists
    --
    -- $alreadyExists
  , SomeAlreadyExists
  , AlreadyExists (..)
  , StreamExists (..)
  , ShardReaderExists (..)
  , RQLiteTableAlreadyExists (..)
  , RQLiteRowAlreadyExists (..)
  , PushQueryCreated (..)

    -- * Exception: SomePermissionDenied
    --
    -- $permissionDenied
  , SomePermissionDenied
  , PermissionDenied (..)

    -- * Exception: SomeResourceExhausted
    --
    -- $resourceExhausted
  , SomeResourceExhausted
  , ResourceExhausted (..)

    -- * Exception: SomeFailedPrecondition
    --
    -- $failedPrecondition
  , SomeFailedPrecondition
  , FailedPrecondition (..)
  , FoundSubscription (..)
  , EmptyShardReader (..)
  , EmptyStream (..)
  , WrongServer (..)
  , WrongExecutionPlan (..)
  , FoundActiveConsumers (..)
  , ShardCanNotSplit (..)
  , ShardCanNotMerge (..)
  , RQLiteRowBadVersion (..)
  , ResourceAllocationException (..)

    -- * Exception: SomeAborted
    --
    -- $aborted
  , SomeAborted
  , Aborted (..)
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
  , OutOfRange (..)

    -- * Exception: SomeUnimplemented
    --
    -- $unimplemented
  , SomeUnimplemented
  , Unimplemented (..)
  , ExecPlanUnimplemented (..)

    -- * Exception: SomeInternal
    --
    -- $internal
  , SomeInternal
  , Internal (..)
  , UnexpectedError (..)
  , WrongOffset (..)
  , ZstdCompresstionErr (..)
  , RQLiteNetworkErr (..)
  , RQLiteDecodeErr (..)
  , RQLiteUnspecifiedErr (..)
  , DiscardedMethod (..)
  , PushQuerySendError (..)

    -- * Exception: SomeUnavailable
  , SomeUnavailable
  , Unavailable (..)
  , ServerNotAvailable (..)

    -- * Exception: SomeDataLoss
  , SomeDataLoss
  , DataLoss (..)

    -- * Exception: SomeUnauthenticated
    --
    -- $unauthenticated
  , SomeUnauthenticated
  , Unauthenticated (..)
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
import           Data.Text                     (Text)
import qualified Data.Text                     as Text
import           Data.Typeable                 (cast)
import           GHC.Stack                     (CallStack, prettyCallStack)
import qualified HsGrpc.Server.Types           as HsGrpc
import           Network.GRPC.HighLevel.Client (StatusCode (..),
                                                StatusDetails (..))
import           Network.GRPC.HighLevel.Server (ServerResponse (..))
import qualified ZooKeeper.Exception           as ZK

import qualified HStream.Logger                as Log

-------------------------------------------------------------------------------
-- The root exception type for all the exceptions in a hstream server

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
};                                                                             \
a##SUB_E##ToException :: Exception e => e -> E.SomeException;                  \
a##SUB_E##ToException = E.toException . SUB_E;                                 \
a##SUB_E##FromException :: Exception e => E.SomeException -> Maybe e;          \
a##SUB_E##FromException x = do SUB_E a <- E.fromException x; cast a;

#define MAKE_PARTICULAR_EX_INS(ExCls, Name, ToString) \
instance Exception Name where                                                  \
{ toException = a##ExCls##ToException;                                         \
  fromException = a##ExCls##FromException;                                     \
  displayException s = ToString s;                                             \
};                                                                             \

#define MAKE_PARTICULAR_EX_0(ExCls, Name, Display) \
data Name = Name deriving (Show);                                              \
instance Exception Name where                                                  \
{ toException = a##ExCls##ToException;                                         \
  fromException = a##ExCls##FromException;                                     \
  displayException Name = Display;                                             \
};                                                                             \

#define MAKE_PARTICULAR_EX_1(ExCls, Name, Ty, ToString) \
newtype Name = Name Ty deriving (Show);                                        \
instance Exception Name where                                                  \
{ toException = a##ExCls##ToException;                                         \
  fromException = a##ExCls##FromException;                                     \
  displayException (Name s) = ToString s;                                      \
};                                                                             \

data ExInfo a = ExInfo
  { exDescription :: a            -- ^ description for this error.
  , exCallStack   :: CallStack    -- ^ lightweight partial call-stack
  }

instance Show a => Show (ExInfo a) where
  show (ExInfo desc stack) = "{description: " <> show desc
                          <> ", callstack: " <> prettyCallStack stack <> "}"

displayExInfo :: (a -> String) -> ExInfo a -> String
displayExInfo toString = toString . exDescription
{-# INLINEABLE displayExInfo #-}

-------------------------------------------------------------------------------

MAKE_SUB_EX(SomeHStreamException, SomeHServerException)

-------------------------------------------------------------------------------
-- Exception: SomeCancelled

-- $cancelled
--
-- The operation was cancelled, typically by the caller.
MAKE_SUB_EX(SomeHServerException, SomeCancelled)

MAKE_PARTICULAR_EX_1(SomeCancelled, Cancelled, (ExInfo String), exDescription)
MAKE_PARTICULAR_EX_1(SomeCancelled, StreamReadError, String, )
MAKE_PARTICULAR_EX_1(SomeCancelled, StreamReadClose, String, )
MAKE_PARTICULAR_EX_1(SomeCancelled, StreamWriteError, String, )

-------------------------------------------------------------------------------
-- Exception: SomeUnknown

-- $unknown
--
-- Unknown error. For example, this error may be returned when a Status value
-- received from another address space belongs to an error space that is not
-- known in this address space. Also errors raised by APIs that do not return
-- enough error information may be converted to this error.
MAKE_SUB_EX(SomeHServerException, SomeUnknown)

MAKE_PARTICULAR_EX_1(SomeUnknown, Unknown, (ExInfo String), exDescription)
MAKE_PARTICULAR_EX_1(SomeUnknown, UnknownPushQueryStatus, String, )

-------------------------------------------------------------------------------
-- Exception: SomeInvalidArgument

-- $invalidArgument
--
-- The client specified an invalid argument. Note that 'InvalidArgument'
-- indicates arguments that are problematic regardless of the state of the
-- system.
MAKE_SUB_EX(SomeHServerException, SomeInvalidArgument)

MAKE_PARTICULAR_EX_1(SomeInvalidArgument, InvalidArgument, (ExInfo String), exDescription)
MAKE_PARTICULAR_EX_1(SomeInvalidArgument, InvalidReplicaFactor, String, )
MAKE_PARTICULAR_EX_1(SomeInvalidArgument, InvalidShardCount, String, )
MAKE_PARTICULAR_EX_1(SomeInvalidArgument, InvalidRecord, String, )
MAKE_PARTICULAR_EX_1(SomeInvalidArgument, InvalidResourceType, String, )
MAKE_PARTICULAR_EX_1(SomeInvalidArgument, InvalidShardOffset, String, )
MAKE_PARTICULAR_EX_1(SomeInvalidArgument, InvalidSubscriptionOffset, String, )
MAKE_PARTICULAR_EX_1(SomeInvalidArgument, DecodeHStreamRecordErr, String, )
MAKE_PARTICULAR_EX_0(SomeInvalidArgument, NoRecordHeader, "HStreamRecord doesn't have a header.")
MAKE_PARTICULAR_EX_0(SomeInvalidArgument, UnknownCompressionType, "UnknownCompressionType")
MAKE_PARTICULAR_EX_1(SomeInvalidArgument, InvalidStatsInterval, String, )
MAKE_PARTICULAR_EX_1(SomeInvalidArgument, InvalidSqlStatement, String, )

-------------------------------------------------------------------------------
-- Exception: SomeDeadlineExceeded

-- $deadlineExceeded
--
-- The deadline expired before the operation could complete. For operations
-- that change the state of the system, this error may be returned even if the
-- operation has completed successfully. For example, a successful response
-- from a server could have been delayed long.
MAKE_SUB_EX(SomeHServerException, SomeDeadlineExceeded)

MAKE_PARTICULAR_EX_1(SomeDeadlineExceeded, DeadlineExceeded, (ExInfo String), exDescription)

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

MAKE_PARTICULAR_EX_1(SomeNotFound, NotFound, (ExInfo String), exDescription)
MAKE_PARTICULAR_EX_1(SomeNotFound, NodesNotFound, Text, Text.unpack)
MAKE_PARTICULAR_EX_1(SomeNotFound, StreamNotFound, Text, Text.unpack)
MAKE_PARTICULAR_EX_1(SomeNotFound, SubscriptionNotFound, Text, Text.unpack)
MAKE_PARTICULAR_EX_1(SomeNotFound, ConnectorNotFound, Text, Text.unpack)
MAKE_PARTICULAR_EX_1(SomeNotFound, ViewNotFound, Text, Text.unpack)
MAKE_PARTICULAR_EX_1(SomeNotFound, ShardNotFound, Text, Text.unpack)
MAKE_PARTICULAR_EX_1(SomeNotFound, QueryNotFound, String, )
MAKE_PARTICULAR_EX_1(SomeNotFound, RQLiteTableNotFound, String, )
MAKE_PARTICULAR_EX_1(SomeNotFound, RQLiteRowNotFound, String, )

-------------------------------------------------------------------------------
-- Exception: SomeAlreadyExists

-- $alreadyExists
--
-- The entity that a client attempted to create (e.g., stream) already exists.
MAKE_SUB_EX(SomeHServerException, SomeAlreadyExists)

MAKE_PARTICULAR_EX_1(SomeAlreadyExists, AlreadyExists, (ExInfo String), exDescription)
MAKE_PARTICULAR_EX_1(SomeAlreadyExists, StreamExists, String, )
MAKE_PARTICULAR_EX_1(SomeAlreadyExists, ShardReaderExists, Text, Text.unpack)
MAKE_PARTICULAR_EX_1(SomeAlreadyExists, RQLiteTableAlreadyExists, String, )
MAKE_PARTICULAR_EX_1(SomeAlreadyExists, RQLiteRowAlreadyExists, String, )
MAKE_PARTICULAR_EX_1(SomeAlreadyExists, PushQueryCreated, String, )

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

MAKE_PARTICULAR_EX_1(SomePermissionDenied, PermissionDenied, (ExInfo String), exDescription)

-------------------------------------------------------------------------------
-- Exception: SomeResourceExhausted

-- $resourceExhausted
--
-- Some resource has been exhausted, perhaps a per-user quota, or perhaps the
-- entire file system is out of space.
MAKE_SUB_EX(SomeHServerException, SomeResourceExhausted)

MAKE_PARTICULAR_EX_1(SomeResourceExhausted, ResourceExhausted, (ExInfo String), exDescription)

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

MAKE_PARTICULAR_EX_1(SomeFailedPrecondition, FailedPrecondition, (ExInfo String), exDescription)
MAKE_PARTICULAR_EX_1(SomeFailedPrecondition, FoundSubscription, String, )
MAKE_PARTICULAR_EX_1(SomeFailedPrecondition, EmptyShardReader, String, )
MAKE_PARTICULAR_EX_1(SomeFailedPrecondition, EmptyStream, String, )
MAKE_PARTICULAR_EX_1(SomeFailedPrecondition, WrongServer, String, )
MAKE_PARTICULAR_EX_1(SomeFailedPrecondition, WrongExecutionPlan, String, )
MAKE_PARTICULAR_EX_1(SomeFailedPrecondition, FoundActiveConsumers, String, )
MAKE_PARTICULAR_EX_1(SomeFailedPrecondition, ShardCanNotSplit, String, )
MAKE_PARTICULAR_EX_1(SomeFailedPrecondition, ShardCanNotMerge, String, )
MAKE_PARTICULAR_EX_1(SomeFailedPrecondition, RQLiteRowBadVersion, String, )

-------------------------------------------------------------------------------
-- Exception: SomeAborted

-- $aborted
--
-- The operation was aborted, typically due to a concurrency issue such as a
-- sequencer check failure or transaction abort. See the guidelines above for
-- deciding between 'FailedPrecondition', 'Aborted', and 'Unavailable'.
MAKE_SUB_EX(SomeHServerException, SomeAborted)

MAKE_PARTICULAR_EX_1(SomeAborted, Aborted, (ExInfo String), exDescription)
MAKE_PARTICULAR_EX_1(SomeAborted, SubscriptionIsDeleting, String, )
MAKE_PARTICULAR_EX_1(SomeAborted, SubscriptionOnDifferentNode, String, )
MAKE_PARTICULAR_EX_1(SomeAborted, SubscriptionInvalidError, String, )
MAKE_PARTICULAR_EX_1(SomeAborted, ConsumerInvalidError, String, )
MAKE_PARTICULAR_EX_1(SomeAborted, TerminateQueriesError, String, )
MAKE_PARTICULAR_EX_1(SomeAborted, PushQueryTerminated, String, )

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

MAKE_PARTICULAR_EX_1(SomeOutOfRange, OutOfRange, (ExInfo String), exDescription)

-------------------------------------------------------------------------------
-- Exception: SomeInternal

-- $internal
--
-- Internal errors. This means that some invariants expected by the underlying
-- system have been broken. This error code is reserved for serious errors.
MAKE_SUB_EX(SomeHServerException, SomeInternal)

MAKE_PARTICULAR_EX_1(SomeInternal, Internal, (ExInfo String), exDescription)
MAKE_PARTICULAR_EX_1(SomeInternal, UnexpectedError, String, )
MAKE_PARTICULAR_EX_1(SomeInternal, WrongOffset, String, )
MAKE_PARTICULAR_EX_1(SomeInternal, ZstdCompresstionErr, String, )
MAKE_PARTICULAR_EX_1(SomeInternal, RQLiteNetworkErr, String, )
MAKE_PARTICULAR_EX_1(SomeInternal, RQLiteDecodeErr, String, )
MAKE_PARTICULAR_EX_1(SomeInternal, RQLiteUnspecifiedErr, String, )
MAKE_PARTICULAR_EX_1(SomeInternal, DiscardedMethod, String, )
MAKE_PARTICULAR_EX_1(SomeInternal, PushQuerySendError, String, )

-------------------------------------------------------------------------------
-- Exception: SomeUnavailable

-- $unavailable
--
-- The service is currently unavailable. This is most likely a transient
-- condition, which can be corrected by retrying with a backoff.
--
-- Note that it is not always safe to retry non-idempotent operations.
MAKE_SUB_EX(SomeHServerException, SomeUnavailable)

MAKE_PARTICULAR_EX_1(SomeUnavailable, Unavailable, (ExInfo String), exDescription)
MAKE_PARTICULAR_EX_0(SomeUnavailable, ServerNotAvailable, "ServerNotAvailable")
MAKE_PARTICULAR_EX_1(SomeUnavailable, ResourceAllocationException, String, )

-------------------------------------------------------------------------------
-- Exception: SomeDataLoss

-- $dataLoss
--
-- Unrecoverable data loss or corruption.
MAKE_SUB_EX(SomeHServerException, SomeDataLoss)

MAKE_PARTICULAR_EX_1(SomeDataLoss, DataLoss, (ExInfo String), exDescription)

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

MAKE_PARTICULAR_EX_1(SomeUnauthenticated, Unauthenticated, (ExInfo String), exDescription)
MAKE_PARTICULAR_EX_1(SomeUnauthenticated, RQLiteNoAuth, String, )

-------------------------------------------------------------------------------
-- Exception: SomeUnimplemented

-- $unimplemented
--
-- The operation is not implemented or is not supported/enabled in this service.
MAKE_SUB_EX(SomeHServerException, SomeUnimplemented)

MAKE_PARTICULAR_EX_1(SomeUnimplemented, Unimplemented, (ExInfo String), exDescription)
MAKE_PARTICULAR_EX_1(SomeUnimplemented, ExecPlanUnimplemented, String, )

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
