{-# LANGUAGE CPP               #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}

module HStream.Exception
  ( -- * Stream Exception
    SomeStreamException
  , someStreamExceptionToException
  , someStreamExceptionFromException
  , SSEInfo (..)
  , throwStreamError
  , throwStreamErrorIfNotOK
    -- ** Specific Stream Exception
  , NOTFOUND
  , TIMEDOUT
  , NOSEQUENCER
  , CONNFAILED
  , NOTCONN
  , TOOBIG
  , TOOMANY
  , PREEMPTED
  , NOBUFS
  , NOMEM
  , INTERNAL
  , SYSLIMIT
  , TEMPLIMIT
  , PERMLIMIT
  , ACCESS
  , ALREADY
  , ISCONN
  , UNREACHABLE
  , UNROUTABLE
  , BADMSG
  , DISABLED
  , EXISTS
  , SHUTDOWN
  , NOTINCONFIG
  , PROTONOSUPPORT
  , PROTO
  , PEER_CLOSED
  , SEQNOBUFS
  , WOULDBLOCK
  , ABORTED
  , INPROGRESS
  , CANCELLED
  , NOTSTORAGE
  , AGAIN
  , PARTIAL
  , GAP
  , TRUNCATED
  , STALE
  , NOSPC
  , OVERLOADED
  , PENDING
  , PENDING_FULL
  , FAILED
  , SEQSYSLIMIT
  , REBUILDING
  , REDIRECTED
  , RETRY
  , BADPAYLOAD
  , NOSSLCONFIG
  , NOTREADY
  , DROPPED
  , FORWARD
  , NOTSUPPORTED
  , NOTINSERVERCONFIG
  , ISOLATED
  , SSLREQUIRED
  , CBREGISTERED
  , LOW_ON_SPC
  , PEER_UNAVAILABLE
  , NOTSUPPORTEDLOG
  , DATALOSS
  , NEVER_CONNECTED
  , NOTANODE
  , IDLE
  , INVALID_PARAM
  , INVALID_CLUSTER
  , INVALID_CONFIG
  , INVALID_THREAD
  , INVALID_IP
  , INVALID_OPERATION
  , UNKNOWN_SETTING
  , INVALID_SETTING_VALUE
  , UPTODATE
  , EMPTY
  , DESTINATION_MISMATCH
  , INVALID_ATTRIBUTES
  , NOTEMPTY
  , NOTDIR
  , ID_CLASH
  , LOGS_SECTION_MISSING
  , CHECKSUM_MISMATCH
  , COND_WRITE_NOT_READY
  , COND_WRITE_FAILED
  , FILE_OPEN
  , FILE_READ
  , LOCAL_LOG_STORE_WRITE
  , CAUGHT_UP
  , UNTIL_LSN_REACHED
  , WINDOW_END_REACHED
  , BYTE_LIMIT_REACHED
  , MALFORMED_RECORD
  , LOCAL_LOG_STORE_READ
  , SHADOW_DISABLED
  , SHADOW_UNCONFIGURED
  , SHADOW_FAILED
  , SHADOW_BUSY
  , SHADOW_LOADING
  , SHADOW_SKIP
  , VERSION_MISMATCH
  , SOURCE_STATE_MISMATCH
  , CONDITION_MISMATCH
  , MAINTENANCE_CLASH
  , WRITE_STREAM_UNKNOWN
  , WRITE_STREAM_BROKEN
  , WRITE_STREAM_IGNORED
  ) where

import           Control.Exception    (Exception (..))
import qualified Control.Exception    as E
import           Data.Typeable        (cast)
import           GHC.Stack            (CallStack, callStack, prettyCallStack)
import qualified Z.Data.Text          as T
import qualified Z.Data.Text.ShowT    as T
import qualified Z.Foreign            as Z

import qualified HStream.Internal.FFI as FFI

-------------------------------------------------------------------------------

-- | The root type of all stream exceptions, you can catch some stream exception
-- by catching this root type.
data SomeStreamException = forall e . E.Exception e => SomeStreamException e

instance Show SomeStreamException where
  show (SomeStreamException e) = show e

instance E.Exception SomeStreamException

someStreamExceptionToException :: E.Exception e => e -> E.SomeException
someStreamExceptionToException = E.toException . E.SomeException

someStreamExceptionFromException :: E.Exception e => E.SomeException -> Maybe e
someStreamExceptionFromException x = do
  SomeStreamException a <- E.fromException x
  cast a

-- | SomeStreamException informations.
data SSEInfo = SSEInfo
  { sseName        :: T.Text      -- ^ the errno name, empty if no errno.
  , sseDescription :: T.Text      -- ^ description for this io error.
  , sseCallStack   :: CallStack   -- ^ lightweight partial call-stack
  }

instance T.ShowT SSEInfo where
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
{ toException = someStreamExceptionToException;    \
  fromException = someStreamExceptionFromException \
}
#define MAKE_THROW_SSE(c, e) \
throwStreamError c stack = do \
  name <- T.validate <$> Z.fromNullTerminated (FFI.c_show_error_name c); \
  desc <- T.validate <$> Z.fromNullTerminated (FFI.c_show_error_description c); \
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

-- Unknown error code
MAKE_SSE(UNKNOWN_CODE)

throwStreamErrorIfNotOK :: IO FFI.ErrorCode -> IO FFI.ErrorCode
throwStreamErrorIfNotOK f = do
  code <- f
  if code == 0 then return 0 else throwStreamError code callStack

throwStreamError :: FFI.ErrorCode -> CallStack -> IO a
MAKE_THROW_SSE(  1, NOTFOUND              )
MAKE_THROW_SSE(  2, TIMEDOUT              )
MAKE_THROW_SSE(  3, NOSEQUENCER           )
MAKE_THROW_SSE(  4, CONNFAILED            )
MAKE_THROW_SSE(  5, NOTCONN               )
MAKE_THROW_SSE(  6, TOOBIG                )
MAKE_THROW_SSE(  7, TOOMANY               )
MAKE_THROW_SSE(  8, PREEMPTED             )
MAKE_THROW_SSE(  9, NOBUFS                )
MAKE_THROW_SSE( 10, NOMEM                 )
MAKE_THROW_SSE( 11, INTERNAL              )
MAKE_THROW_SSE( 12, SYSLIMIT              )
MAKE_THROW_SSE( 13, TEMPLIMIT             )
MAKE_THROW_SSE( 14, PERMLIMIT             )
MAKE_THROW_SSE( 15, ACCESS                )
MAKE_THROW_SSE( 16, ALREADY               )
MAKE_THROW_SSE( 17, ISCONN                )
MAKE_THROW_SSE( 18, UNREACHABLE           )
MAKE_THROW_SSE( 19, UNROUTABLE            )
MAKE_THROW_SSE( 20, BADMSG                )
MAKE_THROW_SSE( 21, DISABLED              )
MAKE_THROW_SSE( 22, EXISTS                )
MAKE_THROW_SSE( 23, SHUTDOWN              )
MAKE_THROW_SSE( 24, NOTINCONFIG           )
MAKE_THROW_SSE( 25, PROTONOSUPPORT        )
MAKE_THROW_SSE( 26, PROTO                 )
MAKE_THROW_SSE( 27, PEER_CLOSED           )
MAKE_THROW_SSE( 28, SEQNOBUFS             )
MAKE_THROW_SSE( 29, WOULDBLOCK            )
MAKE_THROW_SSE( 30, ABORTED               )
MAKE_THROW_SSE( 31, INPROGRESS            )
MAKE_THROW_SSE( 32, CANCELLED             )
MAKE_THROW_SSE( 33, NOTSTORAGE            )
MAKE_THROW_SSE( 34, AGAIN                 )
MAKE_THROW_SSE( 35, PARTIAL               )
MAKE_THROW_SSE( 36, GAP                   )
MAKE_THROW_SSE( 37, TRUNCATED             )
MAKE_THROW_SSE( 38, STALE                 )
MAKE_THROW_SSE( 39, NOSPC                 )
MAKE_THROW_SSE( 40, OVERLOADED            )
MAKE_THROW_SSE( 41, PENDING               )
MAKE_THROW_SSE( 42, PENDING_FULL          )
MAKE_THROW_SSE( 43, FAILED                )
MAKE_THROW_SSE( 44, SEQSYSLIMIT           )
MAKE_THROW_SSE( 45, REBUILDING            )
MAKE_THROW_SSE( 46, REDIRECTED            )
MAKE_THROW_SSE( 47, RETRY                 )
MAKE_THROW_SSE( 48, BADPAYLOAD            )
MAKE_THROW_SSE( 49, NOSSLCONFIG           )
MAKE_THROW_SSE( 50, NOTREADY              )
MAKE_THROW_SSE( 51, DROPPED               )
MAKE_THROW_SSE( 52, FORWARD               )
MAKE_THROW_SSE( 53, NOTSUPPORTED          )
MAKE_THROW_SSE( 54, NOTINSERVERCONFIG     )
MAKE_THROW_SSE( 55, ISOLATED              )
MAKE_THROW_SSE( 56, SSLREQUIRED           )
MAKE_THROW_SSE( 57, CBREGISTERED          )
MAKE_THROW_SSE( 58, LOW_ON_SPC            )
MAKE_THROW_SSE( 59, PEER_UNAVAILABLE      )
MAKE_THROW_SSE( 60, NOTSUPPORTEDLOG       )
MAKE_THROW_SSE( 61, DATALOSS              )
MAKE_THROW_SSE( 62, NEVER_CONNECTED       )
MAKE_THROW_SSE( 63, NOTANODE              )
MAKE_THROW_SSE( 64, IDLE                  )
MAKE_THROW_SSE(100, INVALID_PARAM         )
MAKE_THROW_SSE(101, INVALID_CLUSTER       )
MAKE_THROW_SSE(102, INVALID_CONFIG        )
MAKE_THROW_SSE(103, INVALID_THREAD        )
MAKE_THROW_SSE(104, INVALID_IP            )
MAKE_THROW_SSE(105, INVALID_OPERATION     )
MAKE_THROW_SSE(106, UNKNOWN_SETTING       )
MAKE_THROW_SSE(107, INVALID_SETTING_VALUE )
MAKE_THROW_SSE(108, UPTODATE              )
MAKE_THROW_SSE(109, EMPTY                 )
MAKE_THROW_SSE(110, DESTINATION_MISMATCH  )
MAKE_THROW_SSE(111, INVALID_ATTRIBUTES    )
MAKE_THROW_SSE(112, NOTEMPTY              )
MAKE_THROW_SSE(113, NOTDIR                )
MAKE_THROW_SSE(114, ID_CLASH              )
MAKE_THROW_SSE(115, LOGS_SECTION_MISSING  )
MAKE_THROW_SSE(116, CHECKSUM_MISMATCH     )
MAKE_THROW_SSE(117, COND_WRITE_NOT_READY  )
MAKE_THROW_SSE(118, COND_WRITE_FAILED     )
MAKE_THROW_SSE(200, FILE_OPEN             )
MAKE_THROW_SSE(201, FILE_READ             )
MAKE_THROW_SSE(300, LOCAL_LOG_STORE_WRITE )
MAKE_THROW_SSE(301, CAUGHT_UP             )
MAKE_THROW_SSE(302, UNTIL_LSN_REACHED     )
MAKE_THROW_SSE(303, WINDOW_END_REACHED    )
MAKE_THROW_SSE(304, BYTE_LIMIT_REACHED    )
MAKE_THROW_SSE(305, MALFORMED_RECORD      )
MAKE_THROW_SSE(306, LOCAL_LOG_STORE_READ  )
MAKE_THROW_SSE(401, SHADOW_DISABLED       )
MAKE_THROW_SSE(402, SHADOW_UNCONFIGURED   )
MAKE_THROW_SSE(403, SHADOW_FAILED         )
MAKE_THROW_SSE(404, SHADOW_BUSY           )
MAKE_THROW_SSE(405, SHADOW_LOADING        )
MAKE_THROW_SSE(406, SHADOW_SKIP           )
MAKE_THROW_SSE(500, VERSION_MISMATCH      )
MAKE_THROW_SSE(501, SOURCE_STATE_MISMATCH )
MAKE_THROW_SSE(502, CONDITION_MISMATCH    )
MAKE_THROW_SSE(600, MAINTENANCE_CLASH     )
MAKE_THROW_SSE(700, WRITE_STREAM_UNKNOWN  )
MAKE_THROW_SSE(701, WRITE_STREAM_BROKEN   )
MAKE_THROW_SSE(702, WRITE_STREAM_IGNORED  )
throwStreamError _ stack =
  E.throwIO $ UNKNOWN_CODE (SSEInfo "UNKNOWN_CODE" "" stack)
