{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedRecordDot   #-}
{-# LANGUAGE OverloadedStrings     #-}

module HStream.Kafka.Group.Member where

import qualified Control.Concurrent             as C
import           Control.Monad                  (join)
import qualified Data.ByteString                as BS
import           Data.Int                       (Int32, Int64)
import qualified Data.IORef                     as IO
import           Data.Maybe                     (fromMaybe)
import qualified Data.Text                      as T
import qualified HStream.Common.Server.MetaData as CM
import qualified HStream.Kafka.Common.Utils     as Utils
import qualified Kafka.Protocol                 as K
import qualified Kafka.Protocol.Service         as K

data Member
  = Member
  { memberId           :: T.Text
  , rebalanceTimeoutMs :: IO.IORef Int32
  , sessionTimeoutMs   :: IO.IORef Int32
  , assignment         :: IO.IORef BS.ByteString
  , lastHeartbeat      :: IO.IORef Int64
  , heartbeatThread    :: IO.IORef (Maybe C.ThreadId)

  -- protocols
  , protocolType       :: T.Text
  , supportedProtocols :: IO.IORef [(T.Text, BS.ByteString)]

  -- client information
  , clientId           :: T.Text
  , clientHost         :: T.Text
  }

newMemberFromReq :: K.RequestContext -> K.JoinGroupRequest -> T.Text -> [(T.Text, BS.ByteString)] -> IO Member
newMemberFromReq reqCtx req memberId supportedProtocols = do
  sessionTimeoutMs <- IO.newIORef req.sessionTimeoutMs
  rebalanceTimeoutMs <- IO.newIORef req.sessionTimeoutMs

  assignment <- IO.newIORef BS.empty

  lastHeartbeat <- IO.newIORef 0
  heartbeatThread <- IO.newIORef Nothing

  supportedProtocols' <- IO.newIORef supportedProtocols

  return $ Member {
      memberId=memberId
    , rebalanceTimeoutMs=rebalanceTimeoutMs
    , sessionTimeoutMs=sessionTimeoutMs

    , assignment=assignment

    , lastHeartbeat=lastHeartbeat
    , heartbeatThread=heartbeatThread

    , protocolType=req.protocolType
    , supportedProtocols=supportedProtocols'

    , clientId=fromMaybe "" (join reqCtx.clientId)
    , clientHost=T.pack reqCtx.clientHost
    }

newMemberFromValue :: CM.GroupMetadataValue -> CM.MemberMetadataValue -> IO Member
newMemberFromValue groupValue value = do
  sessionTimeoutMs <- IO.newIORef value.sessionTimeout
  rebalanceTimeoutMs <- IO.newIORef value.rebalanceTimeout

  assignment <- IO.newIORef (Utils.decodeBase64 value.assignment)

  lastHeartbeat <- IO.newIORef 0
  heartbeatThread <- IO.newIORef Nothing

  supportedProtocols' <- IO.newIORef [(fromMaybe "" groupValue.prototcolName, Utils.decodeBase64 value.subscription)]

  return $ Member {
      memberId=value.memberId
    , rebalanceTimeoutMs=rebalanceTimeoutMs
    , sessionTimeoutMs=sessionTimeoutMs

    , assignment=assignment

    , lastHeartbeat=lastHeartbeat
    , heartbeatThread=heartbeatThread

    , protocolType=groupValue.protocolType
    , supportedProtocols=supportedProtocols'

    , clientId=value.clientId
    , clientHost=value.clientHost
    }
