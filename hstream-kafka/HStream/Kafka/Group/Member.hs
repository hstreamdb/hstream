{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedRecordDot   #-}
{-# LANGUAGE OverloadedStrings     #-}

module HStream.Kafka.Group.Member where

import qualified Control.Concurrent     as C
import           Control.Monad          (join)
import qualified Data.ByteString        as BS
import           Data.Int               (Int32, Int64)
import qualified Data.IORef             as IO
import           Data.Maybe             (fromMaybe)
import qualified Data.Text              as T
import qualified Kafka.Protocol.Service as K

data Member
  = Member
  { memberId          :: T.Text
  , sessionTimeoutMs  :: Int32
  , assignment        :: IO.IORef BS.ByteString
  , lastHeartbeat     :: IO.IORef Int64
  , heartbeatThread   :: IO.IORef (Maybe C.ThreadId)

  -- protocols
  , protocolType      :: T.Text
  , supportedProtcols :: [(T.Text, BS.ByteString)]

  -- client information
  , clientId          :: T.Text
  , clientHost        :: T.Text
  }

newMember :: K.RequestContext -> T.Text -> Int32 -> T.Text -> [(T.Text, BS.ByteString)] -> IO Member
newMember reqCtx memberId sessionTimeoutMs protocolType supportedProtcols = do
  assignment <- IO.newIORef BS.empty
  lastHeartbeat <- IO.newIORef 0
  heartbeatThread <- IO.newIORef Nothing

  -- TODO: read from request context
  let clientId = fromMaybe "" (join reqCtx.clientId)
      clientHost = T.pack reqCtx.clientHost

  -- TODO: check request
  return $ Member {..}
