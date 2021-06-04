{-# LANGUAGE CPP             #-}
{-# LANGUAGE GADTs           #-}
{-# LANGUAGE PatternSynonyms #-}

module HStream.Store.Exception
  ( -- * Store Exception
    SomeHStoreException
  , someHStoreExceptionToException
  , someHStoreExceptionFromException
  , SSEInfo (..)
  , throwStreamError
  , throwStreamErrorIfNotOK
  , throwStreamErrorIfNotOK'
  , throwStoreError

    -- * Custom Errors
  , SubmitError (..)
  , throwSubmitError
  , throwSubmitIfNotOK
  , throwSubmitIfNotOK'

    -- * General Store Exception
  , StoreError               (..)
    -- ** Specific Stream Exception
  , NOTFOUND                 (..)
  , TIMEDOUT                 (..)
  , NOSEQUENCER              (..)
  , CONNFAILED               (..)
  , NOTCONN                  (..)
  , TOOBIG                   (..)
  , TOOMANY                  (..)
  , PREEMPTED                (..)
  , NOBUFS                   (..)
  , NOMEM                    (..)
  , INTERNAL                 (..)
  , SYSLIMIT                 (..)
  , TEMPLIMIT                (..)
  , PERMLIMIT                (..)
  , ACCESS                   (..)
  , ALREADY                  (..)
  , ISCONN                   (..)
  , UNREACHABLE              (..)
  , UNROUTABLE               (..)
  , BADMSG                   (..)
  , DISABLED                 (..)
  , EXISTS                   (..)
  , SHUTDOWN                 (..)
  , NOTINCONFIG              (..)
  , PROTONOSUPPORT           (..)
  , PROTO                    (..)
  , PEER_CLOSED              (..)
  , SEQNOBUFS                (..)
  , WOULDBLOCK               (..)
  , ABORTED                  (..)
  , INPROGRESS               (..)
  , CANCELLED                (..)
  , NOTSTORAGE               (..)
  , AGAIN                    (..)
  , PARTIAL                  (..)
  , GAP                      (..)
  , TRUNCATED                (..)
  , STALE                    (..)
  , NOSPC                    (..)
  , OVERLOADED               (..)
  , PENDING                  (..)
  , PENDING_FULL             (..)
  , FAILED                   (..)
  , SEQSYSLIMIT              (..)
  , REBUILDING               (..)
  , REDIRECTED               (..)
  , RETRY                    (..)
  , BADPAYLOAD               (..)
  , NOSSLCONFIG              (..)
  , NOTREADY                 (..)
  , DROPPED                  (..)
  , FORWARD                  (..)
  , NOTSUPPORTED             (..)
  , NOTINSERVERCONFIG        (..)
  , ISOLATED                 (..)
  , SSLREQUIRED              (..)
  , CBREGISTERED             (..)
  , LOW_ON_SPC               (..)
  , PEER_UNAVAILABLE         (..)
  , NOTSUPPORTEDLOG          (..)
  , DATALOSS                 (..)
  , NEVER_CONNECTED          (..)
  , NOTANODE                 (..)
  , IDLE                     (..)
  , INVALID_PARAM            (..)
  , INVALID_CLUSTER          (..)
  , INVALID_CONFIG           (..)
  , INVALID_THREAD           (..)
  , INVALID_IP               (..)
  , INVALID_OPERATION        (..)
  , UNKNOWN_SETTING          (..)
  , INVALID_SETTING_VALUE    (..)
  , UPTODATE                 (..)
  , EMPTY                    (..)
  , DESTINATION_MISMATCH     (..)
  , INVALID_ATTRIBUTES       (..)
  , NOTEMPTY                 (..)
  , NOTDIR                   (..)
  , ID_CLASH                 (..)
  , LOGS_SECTION_MISSING     (..)
  , CHECKSUM_MISMATCH        (..)
  , COND_WRITE_NOT_READY     (..)
  , COND_WRITE_FAILED        (..)
  , FILE_OPEN                (..)
  , FILE_READ                (..)
  , LOCAL_LOG_STORE_WRITE    (..)
  , CAUGHT_UP                (..)
  , UNTIL_LSN_REACHED        (..)
  , WINDOW_END_REACHED       (..)
  , BYTE_LIMIT_REACHED       (..)
  , MALFORMED_RECORD         (..)
  , LOCAL_LOG_STORE_READ     (..)
  , SHADOW_DISABLED          (..)
  , SHADOW_UNCONFIGURED      (..)
  , SHADOW_FAILED            (..)
  , SHADOW_BUSY              (..)
  , SHADOW_LOADING           (..)
  , SHADOW_SKIP              (..)
  , VERSION_MISMATCH         (..)
  , SOURCE_STATE_MISMATCH    (..)
  , CONDITION_MISMATCH       (..)
  , MAINTENANCE_CLASH        (..)
  , WRITE_STREAM_UNKNOWN     (..)
  , WRITE_STREAM_BROKEN      (..)
  , WRITE_STREAM_IGNORED     (..)

    -- * Patterns
  , pattern C_OK
  , pattern C_NOTFOUND
  , pattern C_TIMEDOUT
  , pattern C_NOSEQUENCER
  , pattern C_CONNFAILED
  , pattern C_NOTCONN
  , pattern C_TOOBIG
  , pattern C_TOOMANY
  , pattern C_PREEMPTED
  , pattern C_NOBUFS
  , pattern C_NOMEM
  , pattern C_INTERNAL
  , pattern C_SYSLIMIT
  , pattern C_TEMPLIMIT
  , pattern C_PERMLIMIT
  , pattern C_ACCESS
  , pattern C_ALREADY
  , pattern C_ISCONN
  , pattern C_UNREACHABLE
  , pattern C_UNROUTABLE
  , pattern C_BADMSG
  , pattern C_DISABLED
  , pattern C_EXISTS
  , pattern C_SHUTDOWN
  , pattern C_NOTINCONFIG
  , pattern C_PROTONOSUPPORT
  , pattern C_PROTO
  , pattern C_PEER_CLOSED
  , pattern C_SEQNOBUFS
  , pattern C_WOULDBLOCK
  , pattern C_ABORTED
  , pattern C_INPROGRESS
  , pattern C_CANCELLED
  , pattern C_NOTSTORAGE
  , pattern C_AGAIN
  , pattern C_PARTIAL
  , pattern C_GAP
  , pattern C_TRUNCATED
  , pattern C_STALE
  , pattern C_NOSPC
  , pattern C_OVERLOADED
  , pattern C_PENDING
  , pattern C_PENDING_FULL
  , pattern C_FAILED
  , pattern C_SEQSYSLIMIT
  , pattern C_REBUILDING
  , pattern C_REDIRECTED
  , pattern C_RETRY
  , pattern C_BADPAYLOAD
  , pattern C_NOSSLCONFIG
  , pattern C_NOTREADY
  , pattern C_DROPPED
  , pattern C_FORWARD
  , pattern C_NOTSUPPORTED
  , pattern C_NOTINSERVERCONFIG
  , pattern C_ISOLATED
  , pattern C_SSLREQUIRED
  , pattern C_CBREGISTERED
  , pattern C_LOW_ON_SPC
  , pattern C_PEER_UNAVAILABLE
  , pattern C_NOTSUPPORTEDLOG
  , pattern C_DATALOSS
  , pattern C_NEVER_CONNECTED
  , pattern C_NOTANODE
  , pattern C_IDLE
  , pattern C_INVALID_PARAM
  , pattern C_INVALID_CLUSTER
  , pattern C_INVALID_CONFIG
  , pattern C_INVALID_THREAD
  , pattern C_INVALID_IP
  , pattern C_INVALID_OPERATION
  , pattern C_UNKNOWN_SETTING
  , pattern C_INVALID_SETTING_VALUE
  , pattern C_UPTODATE
  , pattern C_EMPTY
  , pattern C_DESTINATION_MISMATCH
  , pattern C_INVALID_ATTRIBUTES
  , pattern C_NOTEMPTY
  , pattern C_NOTDIR
  , pattern C_ID_CLASH
  , pattern C_LOGS_SECTION_MISSING
  , pattern C_CHECKSUM_MISMATCH
  , pattern C_COND_WRITE_NOT_READY
  , pattern C_COND_WRITE_FAILED
  , pattern C_FILE_OPEN
  , pattern C_FILE_READ
  , pattern C_LOCAL_LOG_STORE_WRITE
  , pattern C_CAUGHT_UP
  , pattern C_UNTIL_LSN_REACHED
  , pattern C_WINDOW_END_REACHED
  , pattern C_BYTE_LIMIT_REACHED
  , pattern C_MALFORMED_RECORD
  , pattern C_LOCAL_LOG_STORE_READ
  , pattern C_SHADOW_DISABLED
  , pattern C_SHADOW_UNCONFIGURED
  , pattern C_SHADOW_FAILED
  , pattern C_SHADOW_BUSY
  , pattern C_SHADOW_LOADING
  , pattern C_SHADOW_SKIP
  , pattern C_VERSION_MISMATCH
  , pattern C_SOURCE_STATE_MISMATCH
  , pattern C_CONDITION_MISMATCH
  , pattern C_MAINTENANCE_CLASH
  , pattern C_WRITE_STREAM_UNKNOWN
  , pattern C_WRITE_STREAM_BROKEN
  , pattern C_WRITE_STREAM_IGNORED
  ) where

import           Control.Exception            (Exception (..))
import qualified Control.Exception            as E
import           Data.String                  (fromString)
import           Data.Typeable                (cast)
import           GHC.Stack                    (CallStack, HasCallStack,
                                               callStack, prettyCallStack)
import qualified Z.Data.Text                  as T
import qualified Z.Data.Text.Print            as T
import qualified Z.Foreign                    as Z

import           HStream.Store.Internal.Types

-------------------------------------------------------------------------------

-- | The root type of all stream exceptions, you can catch some stream exception
-- by catching this root type.
data SomeHStoreException = forall e . E.Exception e => SomeHStoreException e

instance Show SomeHStoreException where
  show (SomeHStoreException e) = show e

instance T.Print SomeHStoreException where
  toUTF8BuilderP _ (SomeHStoreException e) = fromString $ show e

instance E.Exception SomeHStoreException

someHStoreExceptionToException :: E.Exception e => e -> E.SomeException
someHStoreExceptionToException = E.toException . SomeHStoreException

someHStoreExceptionFromException :: E.Exception e => E.SomeException -> Maybe e
someHStoreExceptionFromException x = do
  SomeHStoreException a <- E.fromException x
  cast a

-- | SomeHStoreException informations.
data SSEInfo = SSEInfo
  { sseName        :: T.Text      -- ^ the errno name, empty if no errno.
  , sseDescription :: T.Text      -- ^ description for this io error.
  , sseCallStack   :: CallStack   -- ^ lightweight partial call-stack
  }

instance T.Print SSEInfo where
  toUTF8BuilderP _ (SSEInfo errno desc cstack) = do
    "{name:"
    T.text errno
    ", description:"
    T.text desc
    ", callstack:"
    T.stringUTF8 (prettyCallStack cstack)
    "}"

instance Show SSEInfo where
  show = T.toString

#define MAKE_SSE(e) \
newtype e = e SSEInfo deriving (Show); \
instance Exception e where                         \
{ toException = someHStoreExceptionToException;    \
  fromException = someHStoreExceptionFromException \
}
#define MAKE_THROW_SSE(c, e) \
throwStreamError c stack = do \
  name <- T.validate <$> Z.fromNullTerminated (c_show_error_name c); \
  desc <- T.validate <$> Z.fromNullTerminated (c_show_error_description c); \
  E.throwIO $ e (SSEInfo name desc stack)

MAKE_SSE(NOTFOUND         )
MAKE_SSE(TIMEDOUT         )
MAKE_SSE(NOSEQUENCER      )
MAKE_SSE(CONNFAILED       )
MAKE_SSE(NOTCONN          )
MAKE_SSE(TOOBIG           )
MAKE_SSE(TOOMANY          )
MAKE_SSE(PREEMPTED        )
MAKE_SSE(NOBUFS           )
MAKE_SSE(NOMEM            )
MAKE_SSE(INTERNAL         )
-- SYSLIMIT: for example, the maximum number of threads. Changing the
-- configuration of your execution environment may fix this.
MAKE_SSE(SYSLIMIT         )
MAKE_SSE(TEMPLIMIT        )
MAKE_SSE(PERMLIMIT        )
MAKE_SSE(ACCESS           )
MAKE_SSE(ALREADY          )
MAKE_SSE(ISCONN           )
MAKE_SSE(UNREACHABLE      )
-- UNROUTABLE: most likely a config error
MAKE_SSE(UNROUTABLE       )
MAKE_SSE(BADMSG           )
-- DISABLED: connection failed because the server is currently marked down
-- (disabled) after a series of unsuccessful connection attempts
MAKE_SSE(DISABLED         )
MAKE_SSE(EXISTS           )
MAKE_SSE(SHUTDOWN         )
MAKE_SSE(NOTINCONFIG      )
MAKE_SSE(PROTONOSUPPORT   )
MAKE_SSE(PROTO            )
MAKE_SSE(PEER_CLOSED      )
MAKE_SSE(SEQNOBUFS        )
MAKE_SSE(WOULDBLOCK       )
MAKE_SSE(ABORTED          )
MAKE_SSE(INPROGRESS       )
MAKE_SSE(CANCELLED        )
-- NOTSTORAGE: request that can only be completed by a storage node was sent
-- to a node not configured to run a local log store, such as a pure sequencer
-- node.
MAKE_SSE(NOTSTORAGE       )
MAKE_SSE(AGAIN            )
MAKE_SSE(PARTIAL          )
MAKE_SSE(GAP              )
MAKE_SSE(TRUNCATED        )
MAKE_SSE(STALE            )
MAKE_SSE(NOSPC            )
MAKE_SSE(OVERLOADED       )
MAKE_SSE(PENDING          )
MAKE_SSE(PENDING_FULL     )
MAKE_SSE(FAILED           )
MAKE_SSE(SEQSYSLIMIT      )
MAKE_SSE(REBUILDING       )
MAKE_SSE(REDIRECTED       )
MAKE_SSE(RETRY            )
MAKE_SSE(BADPAYLOAD       )
MAKE_SSE(NOSSLCONFIG      )
MAKE_SSE(NOTREADY         )
MAKE_SSE(DROPPED          )
MAKE_SSE(FORWARD          )
MAKE_SSE(NOTSUPPORTED     )
MAKE_SSE(NOTINSERVERCONFIG)
MAKE_SSE(ISOLATED         )
MAKE_SSE(SSLREQUIRED      )
MAKE_SSE(CBREGISTERED     )
MAKE_SSE(LOW_ON_SPC       )
MAKE_SSE(PEER_UNAVAILABLE )
MAKE_SSE(NOTSUPPORTEDLOG  )
MAKE_SSE(DATALOSS         )
MAKE_SSE(NEVER_CONNECTED  )
MAKE_SSE(NOTANODE         )
MAKE_SSE(IDLE             )

-- Errors about invalid entities

MAKE_SSE(INVALID_PARAM    )
MAKE_SSE(INVALID_CLUSTER  )
MAKE_SSE(INVALID_CONFIG   )
-- INVALID_THREAD: for example, thread not running an event loop
MAKE_SSE(INVALID_THREAD   )
MAKE_SSE(INVALID_IP       )
MAKE_SSE(INVALID_OPERATION)
MAKE_SSE(UNKNOWN_SETTING  )
MAKE_SSE(INVALID_SETTING_VALUE)
MAKE_SSE(UPTODATE             )
MAKE_SSE(EMPTY                )
MAKE_SSE(DESTINATION_MISMATCH )
MAKE_SSE(INVALID_ATTRIBUTES   )
MAKE_SSE(NOTEMPTY             )
MAKE_SSE(NOTDIR               )
MAKE_SSE(ID_CLASH             )
MAKE_SSE(LOGS_SECTION_MISSING )
MAKE_SSE(CHECKSUM_MISMATCH    )
MAKE_SSE(COND_WRITE_NOT_READY )
MAKE_SSE(COND_WRITE_FAILED    )

-- Configuration errors

MAKE_SSE(FILE_OPEN)
MAKE_SSE(FILE_READ)

-- Local log store errors

MAKE_SSE(LOCAL_LOG_STORE_WRITE)
MAKE_SSE(CAUGHT_UP)
MAKE_SSE(UNTIL_LSN_REACHED)
MAKE_SSE(WINDOW_END_REACHED)
MAKE_SSE(BYTE_LIMIT_REACHED)
MAKE_SSE(MALFORMED_RECORD)
MAKE_SSE(LOCAL_LOG_STORE_READ)

-- Traffic shadowing errors

MAKE_SSE(SHADOW_DISABLED)
MAKE_SSE(SHADOW_UNCONFIGURED)
MAKE_SSE(SHADOW_FAILED)
MAKE_SSE(SHADOW_BUSY)
MAKE_SSE(SHADOW_LOADING)
MAKE_SSE(SHADOW_SKIP)

-- Storage membership errors
MAKE_SSE(VERSION_MISMATCH)
MAKE_SSE(SOURCE_STATE_MISMATCH)
MAKE_SSE(CONDITION_MISMATCH)

-- Maintenance errors
MAKE_SSE(MAINTENANCE_CLASH)

-- Write Stream errors
MAKE_SSE(WRITE_STREAM_UNKNOWN)
MAKE_SSE(WRITE_STREAM_BROKEN)
MAKE_SSE(WRITE_STREAM_IGNORED)

MAKE_SSE(StoreError)

-- Unknown error code
MAKE_SSE(UNKNOWN_CODE)

throwStreamErrorIfNotOK :: HasCallStack => IO ErrorCode -> IO ErrorCode
throwStreamErrorIfNotOK = (throwStreamErrorIfNotOK' =<<)

throwStreamErrorIfNotOK' :: HasCallStack => ErrorCode -> IO ErrorCode
throwStreamErrorIfNotOK' code
  | code == C_OK = return C_OK
  | otherwise = throwStreamError code callStack

throwStreamError :: ErrorCode -> CallStack -> IO a
MAKE_THROW_SSE(C_NOTFOUND             , NOTFOUND              )
MAKE_THROW_SSE(C_TIMEDOUT             , TIMEDOUT              )
MAKE_THROW_SSE(C_NOSEQUENCER          , NOSEQUENCER           )
MAKE_THROW_SSE(C_CONNFAILED           , CONNFAILED            )
MAKE_THROW_SSE(C_NOTCONN              , NOTCONN               )
MAKE_THROW_SSE(C_TOOBIG               , TOOBIG                )
MAKE_THROW_SSE(C_TOOMANY              , TOOMANY               )
MAKE_THROW_SSE(C_PREEMPTED            , PREEMPTED             )
MAKE_THROW_SSE(C_NOBUFS               , NOBUFS                )
MAKE_THROW_SSE(C_NOMEM                , NOMEM                 )
MAKE_THROW_SSE(C_INTERNAL             , INTERNAL              )
MAKE_THROW_SSE(C_SYSLIMIT             , SYSLIMIT              )
MAKE_THROW_SSE(C_TEMPLIMIT            , TEMPLIMIT             )
MAKE_THROW_SSE(C_PERMLIMIT            , PERMLIMIT             )
MAKE_THROW_SSE(C_ACCESS               , ACCESS                )
MAKE_THROW_SSE(C_ALREADY              , ALREADY               )
MAKE_THROW_SSE(C_ISCONN               , ISCONN                )
MAKE_THROW_SSE(C_UNREACHABLE          , UNREACHABLE           )
MAKE_THROW_SSE(C_UNROUTABLE           , UNROUTABLE            )
MAKE_THROW_SSE(C_BADMSG               , BADMSG                )
MAKE_THROW_SSE(C_DISABLED             , DISABLED              )
MAKE_THROW_SSE(C_EXISTS               , EXISTS                )
MAKE_THROW_SSE(C_SHUTDOWN             , SHUTDOWN              )
MAKE_THROW_SSE(C_NOTINCONFIG          , NOTINCONFIG           )
MAKE_THROW_SSE(C_PROTONOSUPPORT       , PROTONOSUPPORT        )
MAKE_THROW_SSE(C_PROTO                , PROTO                 )
MAKE_THROW_SSE(C_PEER_CLOSED          , PEER_CLOSED           )
MAKE_THROW_SSE(C_SEQNOBUFS            , SEQNOBUFS             )
MAKE_THROW_SSE(C_WOULDBLOCK           , WOULDBLOCK            )
MAKE_THROW_SSE(C_ABORTED              , ABORTED               )
MAKE_THROW_SSE(C_INPROGRESS           , INPROGRESS            )
MAKE_THROW_SSE(C_CANCELLED            , CANCELLED             )
MAKE_THROW_SSE(C_NOTSTORAGE           , NOTSTORAGE            )
MAKE_THROW_SSE(C_AGAIN                , AGAIN                 )
MAKE_THROW_SSE(C_PARTIAL              , PARTIAL               )
MAKE_THROW_SSE(C_GAP                  , GAP                   )
MAKE_THROW_SSE(C_TRUNCATED            , TRUNCATED             )
MAKE_THROW_SSE(C_STALE                , STALE                 )
MAKE_THROW_SSE(C_NOSPC                , NOSPC                 )
MAKE_THROW_SSE(C_OVERLOADED           , OVERLOADED            )
MAKE_THROW_SSE(C_PENDING              , PENDING               )
MAKE_THROW_SSE(C_PENDING_FULL         , PENDING_FULL          )
MAKE_THROW_SSE(C_FAILED               , FAILED                )
MAKE_THROW_SSE(C_SEQSYSLIMIT          , SEQSYSLIMIT           )
MAKE_THROW_SSE(C_REBUILDING           , REBUILDING            )
MAKE_THROW_SSE(C_REDIRECTED           , REDIRECTED            )
MAKE_THROW_SSE(C_RETRY                , RETRY                 )
MAKE_THROW_SSE(C_BADPAYLOAD           , BADPAYLOAD            )
MAKE_THROW_SSE(C_NOSSLCONFIG          , NOSSLCONFIG           )
MAKE_THROW_SSE(C_NOTREADY             , NOTREADY              )
MAKE_THROW_SSE(C_DROPPED              , DROPPED               )
MAKE_THROW_SSE(C_FORWARD              , FORWARD               )
MAKE_THROW_SSE(C_NOTSUPPORTED         , NOTSUPPORTED          )
MAKE_THROW_SSE(C_NOTINSERVERCONFIG    , NOTINSERVERCONFIG     )
MAKE_THROW_SSE(C_ISOLATED             , ISOLATED              )
MAKE_THROW_SSE(C_SSLREQUIRED          , SSLREQUIRED           )
MAKE_THROW_SSE(C_CBREGISTERED         , CBREGISTERED          )
MAKE_THROW_SSE(C_LOW_ON_SPC           , LOW_ON_SPC            )
MAKE_THROW_SSE(C_PEER_UNAVAILABLE     , PEER_UNAVAILABLE      )
MAKE_THROW_SSE(C_NOTSUPPORTEDLOG      , NOTSUPPORTEDLOG       )
MAKE_THROW_SSE(C_DATALOSS             , DATALOSS              )
MAKE_THROW_SSE(C_NEVER_CONNECTED      , NEVER_CONNECTED       )
MAKE_THROW_SSE(C_NOTANODE             , NOTANODE              )
MAKE_THROW_SSE(C_IDLE                 , IDLE                  )
MAKE_THROW_SSE(C_INVALID_PARAM        , INVALID_PARAM         )
MAKE_THROW_SSE(C_INVALID_CLUSTER      , INVALID_CLUSTER       )
MAKE_THROW_SSE(C_INVALID_CONFIG       , INVALID_CONFIG        )
MAKE_THROW_SSE(C_INVALID_THREAD       , INVALID_THREAD        )
MAKE_THROW_SSE(C_INVALID_IP           , INVALID_IP            )
MAKE_THROW_SSE(C_INVALID_OPERATION    , INVALID_OPERATION     )
MAKE_THROW_SSE(C_UNKNOWN_SETTING      , UNKNOWN_SETTING       )
MAKE_THROW_SSE(C_INVALID_SETTING_VALUE, INVALID_SETTING_VALUE )
MAKE_THROW_SSE(C_UPTODATE             , UPTODATE              )
MAKE_THROW_SSE(C_EMPTY                , EMPTY                 )
MAKE_THROW_SSE(C_DESTINATION_MISMATCH , DESTINATION_MISMATCH  )
MAKE_THROW_SSE(C_INVALID_ATTRIBUTES   , INVALID_ATTRIBUTES    )
MAKE_THROW_SSE(C_NOTEMPTY             , NOTEMPTY              )
MAKE_THROW_SSE(C_NOTDIR               , NOTDIR                )
MAKE_THROW_SSE(C_ID_CLASH             , ID_CLASH              )
MAKE_THROW_SSE(C_LOGS_SECTION_MISSING , LOGS_SECTION_MISSING  )
MAKE_THROW_SSE(C_CHECKSUM_MISMATCH    , CHECKSUM_MISMATCH     )
MAKE_THROW_SSE(C_COND_WRITE_NOT_READY , COND_WRITE_NOT_READY  )
MAKE_THROW_SSE(C_COND_WRITE_FAILED    , COND_WRITE_FAILED     )
MAKE_THROW_SSE(C_FILE_OPEN            , FILE_OPEN             )
MAKE_THROW_SSE(C_FILE_READ            , FILE_READ             )
MAKE_THROW_SSE(C_LOCAL_LOG_STORE_WRITE, LOCAL_LOG_STORE_WRITE )
MAKE_THROW_SSE(C_CAUGHT_UP            , CAUGHT_UP             )
MAKE_THROW_SSE(C_UNTIL_LSN_REACHED    , UNTIL_LSN_REACHED     )
MAKE_THROW_SSE(C_WINDOW_END_REACHED   , WINDOW_END_REACHED    )
MAKE_THROW_SSE(C_BYTE_LIMIT_REACHED   , BYTE_LIMIT_REACHED    )
MAKE_THROW_SSE(C_MALFORMED_RECORD     , MALFORMED_RECORD      )
MAKE_THROW_SSE(C_LOCAL_LOG_STORE_READ , LOCAL_LOG_STORE_READ  )
MAKE_THROW_SSE(C_SHADOW_DISABLED      , SHADOW_DISABLED       )
MAKE_THROW_SSE(C_SHADOW_UNCONFIGURED  , SHADOW_UNCONFIGURED   )
MAKE_THROW_SSE(C_SHADOW_FAILED        , SHADOW_FAILED         )
MAKE_THROW_SSE(C_SHADOW_BUSY          , SHADOW_BUSY           )
MAKE_THROW_SSE(C_SHADOW_LOADING       , SHADOW_LOADING        )
MAKE_THROW_SSE(C_SHADOW_SKIP          , SHADOW_SKIP           )
MAKE_THROW_SSE(C_VERSION_MISMATCH     , VERSION_MISMATCH      )
MAKE_THROW_SSE(C_SOURCE_STATE_MISMATCH, SOURCE_STATE_MISMATCH )
MAKE_THROW_SSE(C_CONDITION_MISMATCH   , CONDITION_MISMATCH    )
MAKE_THROW_SSE(C_MAINTENANCE_CLASH    , MAINTENANCE_CLASH     )
MAKE_THROW_SSE(C_WRITE_STREAM_UNKNOWN , WRITE_STREAM_UNKNOWN  )
MAKE_THROW_SSE(C_WRITE_STREAM_BROKEN  , WRITE_STREAM_BROKEN   )
MAKE_THROW_SSE(C_WRITE_STREAM_IGNORED , WRITE_STREAM_IGNORED  )
throwStreamError code stack =
  let codeBS = "UNKNOWN_CODE:" <> T.validate (T.toUTF8Bytes code)
   in E.throwIO $ UNKNOWN_CODE (SSEInfo codeBS "" stack)

data SubmitError = SubmitError T.Text CallStack
  deriving (Show)

instance Exception SubmitError where
  toException = someHStoreExceptionToException
  fromException = someHStoreExceptionFromException

throwSubmitError :: CallStack -> IO a
throwSubmitError = E.throwIO . SubmitError "submit error"

throwSubmitIfNotOK :: CallStack -> Int  -> IO Int
throwSubmitIfNotOK stack ret = if ret == 0 then return 0 else throwSubmitError stack

throwSubmitIfNotOK' :: HasCallStack => Int  -> IO Int
throwSubmitIfNotOK' = throwSubmitIfNotOK callStack

throwStoreError :: T.Text -> CallStack -> IO a
throwStoreError desc stack =
  E.throwIO $ StoreError (SSEInfo "1000" desc stack)
