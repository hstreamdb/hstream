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

    -- * Exception: Cancelled
    --
    -- $cancelled
  , Cancelled
  , StreamReadError (..)
  , StreamReadClose (..)
  , StreamWriteError (..)

    -- * Exception: Unknown
    --
    -- $unknown
  , Unknown
  , UnknownPushQueryStatus (..)

    -- * Exception: InvalidArgument
    --
    -- $invalidArgument
  , InvalidArgument
  , InvalidReplicaFactor (..)
  , InvalidShardCount (..)
  , InvalidRecord (..)
  , InvalidShardOffset (..)
  , InvalidSubscriptionOffset (..)
  , DecodeHStreamRecordErr (..)
  , NoRecordHeader (..)
  , UnknownCompressionType (..)
  , InvalidStatsInterval (..)
  , InvalidSqlStatement (..)

    -- * Exception: NotFound
    --
    -- $notFound
  , NotFound
  , NodesNotFound (..)
  , StreamNotFound (..)
  , SubscriptionNotFound (..)
  , ConnectorNotFound (..)
  , ViewNotFound (..)
  , ShardNotFound (..)
  , QueryNotFound (..)
  , RQLiteTableNotFound (..)
  , RQLiteRowNotFound (..)

    -- * Exception: AlreadyExists
    --
    -- $alreadyExists
  , AlreadyExists
  , StreamExists (..)
  , ShardReaderExists (..)
  , RQLiteTableAlreadyExists (..)
  , RQLiteRowAlreadyExists (..)
  , PushQueryCreated (..)

    -- * Exception: FailedPrecondition
    --
    -- $failedPrecondition
  , FailedPrecondition
  , FoundSubscription (..)
  , EmptyShardReader (..)
  , EmptyStream (..)
  , WrongServer (..)
  , WrongExecutionPlan (..)
  , FoundActiveConsumers (..)
  , ShardCanNotSplit (..)
  , ShardCanNotMerge (..)
  , RQLiteRowBadVersion (..)

    -- * Exception: Aborted
    --
    -- $aborted
  , Aborted
  , SubscriptionIsDeleting (..)
  , SubscriptionOnDifferentNode (..)
  , SubscriptionInvalidError (..)
  , ConsumerInvalidError (..)
  , TerminateQueriesError (..)
  , PushQueryTerminated (..)

    -- * Exception: Unavailable
  , Unavailable
  , ServerNotAvailable (..)

    -- * Exception: Internal
    --
    -- $internal
  , Internal
  , UnexpectedError (..)
  , WrongOffset (..)
  , ZstdCompresstionErr (..)
  , RQLiteNetworkErr (..)
  , RQLiteDecodeErr (..)
  , RQLiteUnspecifiedErr (..)
  , DiscardedMethod (..)
  , PushQuerySendError (..)

    -- * Exception: Unauthenticated
    --
    -- $unauthenticated
  , Unauthenticated
  , RQLiteNoAuth (..)

    -- * Exception: Unimplemented
    --
    -- $unimplemented
  , Unimplemented
  , ExecPlanUnimplemented (..)

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
import           GHC.Stack                     (CallStack)
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
  } deriving (Show)

displayExInfo :: (a -> String) -> ExInfo a -> String
displayExInfo toString = toString . exDescription
{-# INLINEABLE displayExInfo #-}

-------------------------------------------------------------------------------

MAKE_SUB_EX(SomeHStreamException, SomeHServerException)

-------------------------------------------------------------------------------
-- Exception: Cancelled

-- $cancelled
--
-- The operation was cancelled, typically by the caller.
MAKE_SUB_EX(SomeHServerException, Cancelled)

MAKE_PARTICULAR_EX_1(Cancelled, StreamReadError, String, )
MAKE_PARTICULAR_EX_1(Cancelled, StreamReadClose, String, )
MAKE_PARTICULAR_EX_1(Cancelled, StreamWriteError, String, )

-------------------------------------------------------------------------------
-- Exception: Unknown

-- $unknown
--
-- Unknown error. For example, this error may be returned when a Status value
-- received from another address space belongs to an error space that is not
-- known in this address space. Also errors raised by APIs that do not return
-- enough error information may be converted to this error.
MAKE_SUB_EX(SomeHServerException, Unknown)

MAKE_PARTICULAR_EX_1(Unknown, UnknownPushQueryStatus, String, )

-------------------------------------------------------------------------------
-- Exception: InvalidArgument

-- $invalidArgument
--
-- The client specified an invalid argument. Note that 'InvalidArgument'
-- indicates arguments that are problematic regardless of the state of the
-- system.
MAKE_SUB_EX(SomeHServerException, InvalidArgument)

MAKE_PARTICULAR_EX_1(InvalidArgument, InvalidReplicaFactor, String, )
MAKE_PARTICULAR_EX_1(InvalidArgument, InvalidShardCount, String, )
MAKE_PARTICULAR_EX_1(InvalidArgument, InvalidRecord, String, )
MAKE_PARTICULAR_EX_1(InvalidArgument, InvalidShardOffset, String, )
MAKE_PARTICULAR_EX_1(InvalidArgument, InvalidSubscriptionOffset, String, )
MAKE_PARTICULAR_EX_1(InvalidArgument, DecodeHStreamRecordErr, String, )
MAKE_PARTICULAR_EX_0(InvalidArgument, NoRecordHeader, "HStreamRecord doesn't have a header.")
MAKE_PARTICULAR_EX_0(InvalidArgument, UnknownCompressionType, "UnknownCompressionType")
MAKE_PARTICULAR_EX_1(InvalidArgument, InvalidStatsInterval, String, )
MAKE_PARTICULAR_EX_1(InvalidArgument, InvalidSqlStatement, String, )

-------------------------------------------------------------------------------
-- Exception: NotFound

-- $notFound
--
-- Some requested entity (e.g., stream or subscription) was not found.
-- Note to server developers: if a request is denied for an entire class of
-- users, such as gradual feature rollout or undocumented allowlist, NotFound
-- may be used. If a request is denied for some users within a class of users,
-- such as user-based access control, PermissionDenied must be used.
MAKE_SUB_EX(SomeHServerException, NotFound)

MAKE_PARTICULAR_EX_1(NotFound, NodesNotFound, Text, Text.unpack)
MAKE_PARTICULAR_EX_1(NotFound, StreamNotFound, Text, Text.unpack)
MAKE_PARTICULAR_EX_1(NotFound, SubscriptionNotFound, Text, Text.unpack)
MAKE_PARTICULAR_EX_1(NotFound, ConnectorNotFound, Text, Text.unpack)
MAKE_PARTICULAR_EX_1(NotFound, ViewNotFound, Text, Text.unpack)
MAKE_PARTICULAR_EX_1(NotFound, ShardNotFound, Text, Text.unpack)
MAKE_PARTICULAR_EX_1(NotFound, QueryNotFound, String, )
MAKE_PARTICULAR_EX_1(NotFound, RQLiteTableNotFound, String, )
MAKE_PARTICULAR_EX_1(NotFound, RQLiteRowNotFound, String, )

-------------------------------------------------------------------------------
-- Exception: AlreadyExists

-- $alreadyExists
--
-- The entity that a client attempted to create (e.g., stream) already exists.
MAKE_SUB_EX(SomeHServerException, AlreadyExists)

MAKE_PARTICULAR_EX_1(AlreadyExists, StreamExists, Text, Text.unpack)
MAKE_PARTICULAR_EX_1(AlreadyExists, ShardReaderExists, Text, Text.unpack)
MAKE_PARTICULAR_EX_1(AlreadyExists, RQLiteTableAlreadyExists, String, )
MAKE_PARTICULAR_EX_1(AlreadyExists, RQLiteRowAlreadyExists, String, )
MAKE_PARTICULAR_EX_1(AlreadyExists, PushQueryCreated, String, )

-------------------------------------------------------------------------------
-- Exception: FailedPrecondition

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
MAKE_SUB_EX(SomeHServerException, FailedPrecondition)

MAKE_PARTICULAR_EX_1(FailedPrecondition, FoundSubscription, String, )
MAKE_PARTICULAR_EX_1(FailedPrecondition, EmptyShardReader, String, )
MAKE_PARTICULAR_EX_1(FailedPrecondition, EmptyStream, String, )
MAKE_PARTICULAR_EX_1(FailedPrecondition, WrongServer, String, )
MAKE_PARTICULAR_EX_1(FailedPrecondition, WrongExecutionPlan, String, )
MAKE_PARTICULAR_EX_1(FailedPrecondition, FoundActiveConsumers, String, )
MAKE_PARTICULAR_EX_1(FailedPrecondition, ShardCanNotSplit, String, )
MAKE_PARTICULAR_EX_1(FailedPrecondition, ShardCanNotMerge, String, )
MAKE_PARTICULAR_EX_1(FailedPrecondition, RQLiteRowBadVersion, String, )

-------------------------------------------------------------------------------
-- Exception: Aborted

-- $aborted
--
-- The operation was aborted, typically due to a concurrency issue such as a
-- sequencer check failure or transaction abort. See the guidelines above for
-- deciding between 'FailedPrecondition', 'Aborted', and 'Unavailable'.
MAKE_SUB_EX(SomeHServerException, Aborted)

MAKE_PARTICULAR_EX_1(Aborted, SubscriptionIsDeleting, String, )
MAKE_PARTICULAR_EX_1(Aborted, SubscriptionOnDifferentNode, String, )
MAKE_PARTICULAR_EX_1(Aborted, SubscriptionInvalidError, String, )
MAKE_PARTICULAR_EX_1(Aborted, ConsumerInvalidError, String, )
MAKE_PARTICULAR_EX_1(Aborted, TerminateQueriesError, String, )
MAKE_PARTICULAR_EX_1(Aborted, PushQueryTerminated, String, )

-------------------------------------------------------------------------------
-- Exception: Internal

-- $internal
--
-- Internal errors. This means that some invariants expected by the underlying
-- system have been broken. This error code is reserved for serious errors.
MAKE_SUB_EX(SomeHServerException, Internal)

MAKE_PARTICULAR_EX_1(Internal, UnexpectedError, String, )
MAKE_PARTICULAR_EX_1(Internal, WrongOffset, String, )
MAKE_PARTICULAR_EX_1(Internal, ZstdCompresstionErr, String, )
MAKE_PARTICULAR_EX_1(Internal, RQLiteNetworkErr, String, )
MAKE_PARTICULAR_EX_1(Internal, RQLiteDecodeErr, String, )
MAKE_PARTICULAR_EX_1(Internal, RQLiteUnspecifiedErr, String, )
MAKE_PARTICULAR_EX_1(Internal, DiscardedMethod, String, )
MAKE_PARTICULAR_EX_1(Internal, PushQuerySendError, String, )

-------------------------------------------------------------------------------
-- Exception: Unavailable

-- $unavailable
--
-- The service is currently unavailable. This is most likely a transient
-- condition, which can be corrected by retrying with a backoff.
--
-- Note that it is not always safe to retry non-idempotent operations.
MAKE_SUB_EX(SomeHServerException, Unavailable)

MAKE_PARTICULAR_EX_0(Unavailable, ServerNotAvailable, "ServerNotAvailable")

-------------------------------------------------------------------------------
-- Exception: Unauthenticated

-- $unauthenticated
--
-- The request does not have valid authentication credentials for the operation.
--
-- Incorrect Auth metadata (Credentials failed to get metadata, Incompatible
-- credentials set on channel and call, Invalid host set in :authority metadata,
-- etc.)
MAKE_SUB_EX(SomeHServerException, Unauthenticated)

MAKE_PARTICULAR_EX_1(Unauthenticated, RQLiteNoAuth, String, )

-------------------------------------------------------------------------------
-- Exception: Unimplemented

-- $unimplemented
--
-- The operation is not implemented or is not supported/enabled in this service.
MAKE_SUB_EX(SomeHServerException, Unimplemented)

MAKE_PARTICULAR_EX_1(Unimplemented, ExecPlanUnimplemented, String, )

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
  [ E.Handler $ \(err :: InvalidArgument) -> do
      Log.warning $ Log.buildString' err
      return (StatusInvalidArgument, mkStatusDetails err)

  , E.Handler $ \(err :: Cancelled) -> do
      Log.fatal $ Log.buildString' err
      return (StatusCancelled, mkStatusDetails err)

  , E.Handler $ \(err :: Unknown) -> do
      Log.fatal $ Log.buildString' err
      return (StatusUnknown, mkStatusDetails err)

  , E.Handler $ \(err :: NotFound) -> do
      Log.warning $ Log.buildString' err
      return (StatusNotFound, mkStatusDetails err)

  , E.Handler $ \(err :: AlreadyExists) -> do
      Log.warning $ Log.buildString' err
      return (StatusAlreadyExists, mkStatusDetails err)

  , E.Handler $ \(err :: FailedPrecondition) -> do
      Log.warning $ Log.buildString' err
      return (StatusFailedPrecondition, mkStatusDetails err)

  , E.Handler $ \(err :: Aborted) -> do
      Log.warning $ Log.buildString' err
      return (StatusAborted, mkStatusDetails err)

  , E.Handler $ \(err :: Unavailable) -> do
      Log.fatal $ Log.buildString' err
      return (StatusUnavailable, mkStatusDetails err)

  , E.Handler $ \(err :: Internal) -> do
      Log.fatal $ Log.buildString' err
      return (StatusInternal, mkStatusDetails err)

  , E.Handler $ \(err :: Unauthenticated) -> do
      Log.warning $ Log.buildString' err
      return (StatusUnauthenticated, mkStatusDetails err)

  , E.Handler $ \(err :: Unimplemented) -> do
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
  , E.Handler $ \(e :: ZK.ZNODEEXISTS        )   -> handleZKException e StatusAlreadyExists
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
  [ E.Handler $ \(err :: InvalidArgument) -> do
      Log.warning $ Log.buildString' err
      HsGrpc.throwGrpcError $ mkGrpcStatus err HsGrpc.StatusInvalidArgument

  , E.Handler $ \(err :: Cancelled) -> do
      Log.fatal $ Log.buildString' err
      HsGrpc.throwGrpcError $ mkGrpcStatus err HsGrpc.StatusCancelled

  , E.Handler $ \(err :: Unknown) -> do
      Log.fatal $ Log.buildString' err
      HsGrpc.throwGrpcError $ mkGrpcStatus err HsGrpc.StatusUnknown

  , E.Handler $ \(err :: NotFound) -> do
      Log.warning $ Log.buildString' err
      HsGrpc.throwGrpcError $ mkGrpcStatus err HsGrpc.StatusNotFound

  , E.Handler $ \(err :: AlreadyExists) -> do
      Log.warning $ Log.buildString' err
      HsGrpc.throwGrpcError $ mkGrpcStatus err HsGrpc.StatusAlreadyExists

  , E.Handler $ \(err :: FailedPrecondition) -> do
      Log.fatal $ Log.buildString' err
      HsGrpc.throwGrpcError $ mkGrpcStatus err HsGrpc.StatusFailedPrecondition

  , E.Handler $ \(err :: Aborted) -> do
      Log.warning $ Log.buildString' err
      HsGrpc.throwGrpcError $ mkGrpcStatus err HsGrpc.StatusAborted

  , E.Handler $ \(err :: Unavailable) -> do
      Log.fatal $ Log.buildString' err
      HsGrpc.throwGrpcError $ mkGrpcStatus err HsGrpc.StatusUnavailable

  , E.Handler $ \(err :: Internal) -> do
      Log.fatal $ Log.buildString' err
      HsGrpc.throwGrpcError $ mkGrpcStatus err HsGrpc.StatusInternal

  , E.Handler $ \(err :: Unimplemented) -> do
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
