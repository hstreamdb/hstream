{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Gossip.Utils where

import           Control.Concurrent.STM           (STM, TQueue, TVar, readTVar,
                                                   stateTVar, writeTQueue,
                                                   writeTVar)
import           Control.Monad                    (unless)
import           Data.ByteString                  (ByteString)
import qualified Data.ByteString                  as BS
import qualified Data.Map                         as Map
import           Data.Serialize                   (decode)
import           Data.Text.Encoding               (decodeUtf8)
import           Data.Word                        (Word32)
import qualified Text.Layout.Table                as Table

import           Data.Foldable                    (foldl')
import           HStream.Gossip.HStreamGossip     (ServerNodeInternal (..))
import           HStream.Gossip.Types             (BroadcastPool,
                                                   EventMessage (..),
                                                   GossipOpts (..), LamportTime,
                                                   Message (..), ServerState,
                                                   ServerStatus (..),
                                                   StateDelta,
                                                   StateMessage (..),
                                                   getMsgNode)
import           HStream.Server.HStreamApi        (ServerNode (..))
import           Network.GRPC.HighLevel.Generated (ClientConfig (..),
                                                   ClientRequest (..),
                                                   GRPCMethodType (..),
                                                   Host (..), Port (..),
                                                   ServerResponse (..),
                                                   StatusCode (..),
                                                   StatusDetails (..))
import           Text.Layout.Table                (def)

returnResp :: Monad m => a -> m (ServerResponse 'Normal a)
returnResp resp = return (ServerNormalResponse (Just resp) mempty StatusOk "")

mkServerErrResp :: StatusCode -> StatusDetails -> ServerResponse 'Normal a
mkServerErrResp = ServerNormalResponse Nothing mempty

returnErrResp :: Monad m
  => StatusCode -> StatusDetails -> m (ServerResponse 'Normal a)
returnErrResp = (return .) . mkServerErrResp

mkGRPCClientConf :: ServerNodeInternal -> ClientConfig
mkGRPCClientConf ServerNodeInternal {..} = mkGRPCClientConf' serverNodeInternalHost (fromIntegral serverNodeInternalGossipPort)

mkGRPCClientConf' :: ByteString -> Int -> ClientConfig
mkGRPCClientConf' host port =
    ClientConfig
    { clientServerHost = Host host
    , clientServerPort = Port port
    , clientArgs = []
    , clientSSLConfig = Nothing
    , clientAuthority = Nothing
    }

mkClientNormalRequest :: req -> ClientRequest 'Normal req resp
mkClientNormalRequest x = ClientNormalRequest x requestTimeout mempty

requestTimeout :: Int
requestTimeout = 100

getMsgInc :: StateMessage -> Word32
getMsgInc (Join _)          = 0
getMsgInc (Suspect inc _ _) = inc
getMsgInc (Alive   inc _ _) = inc
getMsgInc (Confirm inc _ _) = inc

fromServerNodeInternal :: ServerNodeInternal -> ServerNode
fromServerNodeInternal ServerNodeInternal {..} = ServerNode {
    serverNodeId   = serverNodeInternalId
  , serverNodeHost = decodeUtf8 serverNodeInternalHost
  , serverNodePort = serverNodeInternalPort
  }

decodeThenBroadCast :: ByteString -> TQueue StateMessage -> TQueue EventMessage ->  STM ()
decodeThenBroadCast msgBS statePool eventPool = unless (BS.null msgBS) $
  case decode msgBS of
    Left err  -> error err
    Right (msgs :: [Message])-> do
      sequence_ $ (\case
        StateMessage msg -> writeTQueue statePool msg
        EventMessage msg -> writeTQueue eventPool msg) <$> msgs

isStateMessage :: Message -> Bool
isStateMessage (StateMessage _) = True
isStateMessage _                = False

-- TODO: add max resend
getMessagesToSend :: Word32 -> BroadcastPool -> ([Message], BroadcastPool)
getMessagesToSend l = foldl' f ([], mempty)
  where
    l' = 4 * ceiling (log $ fromIntegral l + 2 :: Double)
    f (msgs, new)  (msg, i) = if l' >= succ i
      then (msg : msgs, (msg, succ i) : new)
      else (msgs,       (msg, i) : new)

getStateMessagesToHandle :: StateDelta -> ([StateMessage], StateDelta)
getStateMessagesToHandle = Map.mapAccum f []
  where
    f xs (msg, handled) = (if handled then xs else msg:xs, (msg, True))

insertStateMessage :: (StateMessage, Bool) -> StateDelta -> (Maybe (StateMessage, Bool), StateDelta)
insertStateMessage msg@(x, _) = Map.insertLookupWithKey f (serverNodeInternalId $ getMsgNode x) msg
  where
    f _key (v', p') (v, p) = if v' > v then (v', p') else (v, p)

cleanStateMessages :: [StateMessage] -> [StateMessage]
cleanStateMessages = Map.elems . foldl' (flip insertMsg) mempty
  where
    insertMsg x = Map.insertWith max (serverNodeInternalId $ getMsgNode x) x

broadcastMessage :: Message -> BroadcastPool -> BroadcastPool
broadcastMessage msg xs = (msg, 0) : xs

updateStatus :: ServerStatus -> StateMessage -> ServerState -> STM Bool
updateStatus ServerStatus{..} msg state = do
  msg' <- readTVar latestMessage
  -- TODO: catch error
  if msg > msg'
    then do
      writeTVar latestMessage msg
      writeTVar serverState state
      return True
    else
      return False

updateLamportTime :: TVar LamportTime -> LamportTime -> STM LamportTime
updateLamportTime localClock eventTime = do
  localTime <- readTVar localClock
  if localTime < eventTime
    then do
      writeTVar localClock (eventTime + 1)
      return (eventTime + 1)
    else
      return localTime

incrementTVar :: Enum a => TVar a -> STM a
incrementTVar localClock = stateTVar localClock (\x -> let y = succ x in (y, y))

showNodesTable :: [ServerNode] -> String
showNodesTable nodes =
  Table.tableString colSpec Table.asciiS
    (Table.fullH (repeat $ Table.headerColumn Table.left Nothing) titles)
    (Table.colsAllG Table.center <$> rows) ++ "\n"
  where
    titles = [ "Server ID"
             , "Server Host"
             , "Server Port"
             ]
    formatRow ServerNode {..} =
      [ [show serverNodeId]
      , [show serverNodeHost]
      , [show serverNodePort]
      ]
    rows = map formatRow nodes
    colSpec = [ Table.column Table.expand Table.left def def
              , Table.column Table.expand Table.left def def
              , Table.column Table.expand Table.left def def
              , Table.column Table.expand Table.left def def
              ]

defaultGossipOpts :: GossipOpts
defaultGossipOpts = GossipOpts {
    gossipFanout     = 3
  , retransmitMult   = 4
  , gossipInterval   = 1 * 1000 * 1000
  , probeInterval    = 2 * 1000 * 1000
  , roundtripTimeout = 500 * 1000
  }
