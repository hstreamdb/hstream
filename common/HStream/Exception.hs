{-# LANGUAGE CPP        #-}
{-# LANGUAGE GADTs      #-}
{-# LANGUAGE RankNTypes #-}
{-# OPTIONS_GHC -pgmPcpphs -optP--cpp #-}

module HStream.Exception
  ( SomeHStreamException
    -- Root of Server Exception
  , SomeHServerException

    -- * Exception Info
  , ExInfo (..)
  , displayExInfo

    -- * InvalidArgument
    --
    -- $invalidArgument
  , InvalidArgument
    -- ** Exceptions
  , InvalidReplicaFactor (..)
  , InvalidShardCount (..)
  , InvalidRecord (..)
  , InvalidShardOffset (..)
  , DecodeHStreamRecordErr (..)
  , NoRecordHeader (..)
  , UnknownCompressionType (..)

    -- * NotFound
    --
    -- $notFound
  , NotFound
    -- ** Exceptions
  , NodesNotFound (..)
  , StreamNotFound (..)
  , SubscriptionNotFound (..)

    -- * AlreadyExists
    --
    -- $alreadyExists
  , AlreadyExists
    -- ** Exceptions
  , StreamExists (..)
  , ShardReaderExists (..)

    -- * FailedPrecondition
    --
    -- $failedPrecondition
  , FailedPrecondition
    -- ** Exceptions
  , FoundSubscription (..)
  , EmptyShardReader (..)
  , EmptyStream (..)
  , WrongServer (..)

    -- * Unavailable
  , Unavailable
  , ServerNotAvailable (..)

    -- * Internal
    --
    -- $internal
  , Internal
    -- ** Exceptions
  , UnexpectedError (..)
  , WrongOffset (..)
  , ZstdCompresstionErr (..)

    -- * Handler
  , MkResp
  , ExceptionHandle
  , setRespType
  , mkStatusDetails
  , mkExceptionHandle
  , mkExceptionHandle'
  , defaultHServerExHandlers

  , -- * Re-export
  E.Handler
  ) where

import           Control.Exception             (Exception (..))
import qualified Control.Exception             as E
import qualified Data.ByteString.Char8         as BSC
import           Data.Text                     (Text)
import qualified Data.Text                     as Text
import           Data.Typeable                 (cast)
import           GHC.Stack                     (CallStack)
import           Network.GRPC.HighLevel.Client (StatusCode (..),
                                                StatusDetails (..))
import           Network.GRPC.HighLevel.Server (ServerResponse (..))

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
-- Exception: Unknown

--MAKE_SUB_EX(SomeHServerException, Unknown)

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
MAKE_PARTICULAR_EX_1(InvalidArgument, DecodeHStreamRecordErr, String, )
MAKE_PARTICULAR_EX_0(InvalidArgument, NoRecordHeader, "HStreamRecord doesn't have a header.")
MAKE_PARTICULAR_EX_0(InvalidArgument, UnknownCompressionType, "UnknownCompressionType")

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

-------------------------------------------------------------------------------
-- Exception: AlreadyExists

-- $alreadyExists
--
-- The entity that a client attempted to create (e.g., stream) already exists.
MAKE_SUB_EX(SomeHServerException, AlreadyExists)

MAKE_PARTICULAR_EX_1(AlreadyExists, StreamExists, Text, Text.unpack)
MAKE_PARTICULAR_EX_1(AlreadyExists, ShardReaderExists, Text, Text.unpack)

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

-------------------------------------------------------------------------------
-- Exception: Aborted

-- $aborted
-- The operation was aborted, typically due to a concurrency issue such as a
-- sequencer check failure or transaction abort. See the guidelines above for
-- deciding between 'FailedPrecondition', 'Aborted', and 'Unavailable'.
--MAKE_SUB_EX(SomeHServerException, Aborted)

-------------------------------------------------------------------------------
-- Exception: Unavailable

-- $unavailable
-- The service is currently unavailable. This is most likely a transient
-- condition, which can be corrected by retrying with a backoff.
--
-- Note that it is not always safe to retry non-idempotent operations.
MAKE_SUB_EX(SomeHServerException, Unavailable)

MAKE_PARTICULAR_EX_0(Unavailable, ServerNotAvailable, "ServerNotAvailable")

-------------------------------------------------------------------------------
-- Handlers

type MkResp t a = StatusCode -> StatusDetails -> ServerResponse t a
type ExceptionHandle a = IO a -> IO a

setRespType :: MkResp t a
            -> [E.Handler (StatusCode, StatusDetails)]
            -> [E.Handler (ServerResponse t a)]
setRespType mkResp = map (uncurry mkResp <$>)

mkStatusDetails :: Exception a => a -> StatusDetails
mkStatusDetails = StatusDetails . BSC.pack . displayException

mkExceptionHandle :: [E.Handler (ServerResponse t a)]
                  -> IO (ServerResponse t a)
                  -> IO (ServerResponse t a)
mkExceptionHandle = flip E.catches

mkExceptionHandle' :: (forall e. Exception e => e -> IO ())
                   -> [E.Handler (ServerResponse t a)]
                   -> IO (ServerResponse t a)
                   -> IO (ServerResponse t a)
mkExceptionHandle' whileEx handlers f =
  let handlers' = map (\(E.Handler h) -> E.Handler (\e -> whileEx e >> h e)) handlers
   in f `E.catches` handlers'

defaultHServerExHandlers :: [E.Handler (StatusCode, StatusDetails)]
defaultHServerExHandlers =
  [ E.Handler $ \(err :: InvalidArgument) -> do
      Log.warning $ Log.buildString' err
      return (StatusInvalidArgument, mkStatusDetails err)

  , E.Handler $ \(err :: NotFound) -> do
      Log.warning $ Log.buildString' err
      return (StatusNotFound, mkStatusDetails err)

  , E.Handler $ \(err :: AlreadyExists) -> do
      Log.warning $ Log.buildString' err
      return (StatusAlreadyExists, mkStatusDetails err)

  , E.Handler $ \(err :: FailedPrecondition) -> do
      Log.warning $ Log.buildString' err
      return (StatusFailedPrecondition, mkStatusDetails err)

  , E.Handler $ \(err :: Unavailable) -> do
      Log.fatal $ Log.buildString' err
      return (StatusUnavailable, mkStatusDetails err)

  , E.Handler $ \(err :: Internal) -> do
      Log.fatal $ Log.buildString' err
      return (StatusInternal, mkStatusDetails err)
  ]
