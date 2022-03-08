{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE MultiWayIf          #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
{-# LANGUAGE TypeApplications    #-}

module HStream.Server.Handler.Subscription
  (
    createSubscriptionHandler,
    deleteSubscriptionHandler,
    listSubscriptionsHandler,
    checkSubscriptionExistHandler,
    watchSubscriptionHandler,
    streamingFetchHandler,
    routineForSubs,
    stopSendingRecords
  )
where

import           Control.Concurrent
import           Control.Concurrent.Async         (concurrently_)
import           Control.Exception                (catch, onException, throwIO,
                                                   try)
import           Control.Monad                    (forM_, unless, when)
import           Data.Function                    (on)
import           Data.Functor
import qualified Data.HashMap.Strict              as HM
import           Data.IORef                       (modifyIORef', newIORef,
                                                   readIORef)
import qualified Data.List                        as L
import qualified Data.Map.Strict                  as Map
import           Data.Maybe                       (fromJust, isNothing)
import qualified Data.Set                         as Set
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Data.Word                        (Word32, Word64, Word8)
import           Network.GRPC.HighLevel           (StreamRecv, StreamSend)
import           Network.GRPC.HighLevel.Generated
import           Z.Data.Vector                    (Bytes)
import qualified Z.Data.Vector                    as ZV
import           Z.Foreign                        (toByteString)
import           Z.IO.LowResTimer                 (registerLowResTimer)
import           ZooKeeper.Types                  (ZHandle)

import           HStream.Common.ConsistentHashing (getAllocatedNodeId)
import           HStream.Connector.HStore         (transToStreamName)
import qualified HStream.Logger                   as Log
import qualified HStream.Server.Core.Subscription as Core
import           HStream.Server.Exception         (StreamNotExist (..),
                                                   SubscribeInnerError (..),
                                                   SubscriptionIdNotFound (..),
                                                   SubscriptionWatchOnDifferentNode (..),
                                                   defaultExceptionHandle)
import           HStream.Server.HStreamApi
import           HStream.Server.Handler.Common    (bindSubToStreamPath,
                                                   getCommitRecordId,
                                                   getSuccessor,
                                                   insertAckedRecordId,
                                                   orderingKeyToStoreKey,
                                                   removeSubFromStreamPath)
import           HStream.Server.Persistence       (ObjRepType (..),
                                                   mkPartitionKeysPath)
import qualified HStream.Server.Persistence       as P
import           HStream.Server.Types
import qualified HStream.Stats                    as Stats
import qualified HStream.Store                    as S
import           HStream.ThirdParty.Protobuf      as PB
import           HStream.Utils                    (cBytesToText, returnResp,
                                                   textToCBytes)

--------------------------------------------------------------------------------

createSubscriptionHandler
  :: ServerContext
  -> ServerRequest 'Normal Subscription Subscription
  -> IO (ServerResponse 'Normal Subscription)
createSubscriptionHandler ctx@ServerContext{..} (ServerNormalRequest _metadata sub@Subscription{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive createSubscription request: " <> Log.buildString' sub
  bindSubToStreamPath zkHandle streamName subName
  catch (Core.createSubscription ctx sub) $
    \(e :: StreamNotExist) -> removeSubFromStreamPath zkHandle streamName subName >> throwIO e
  returnResp sub
  where
    streamName = textToCBytes subscriptionStreamName
    subName = textToCBytes subscriptionSubscriptionId
--------------------------------------------------------------------------------

-- FIXME: depend on memory info to deal with delete operation may be wrong, even if all the create/delete requests
-- are redirected to same server. What if this server crash, or the consistante hash choose another server to deal
-- these requests? We need some way to rebuild all these memory infos first.
deleteSubscriptionHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteSubscriptionRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteSubscriptionHandler ctx@ServerContext{..} (ServerNormalRequest _metadata req@DeleteSubscriptionRequest
  { deleteSubscriptionRequestSubscriptionId = subId }) = defaultExceptionHandle $ do
  hr <- readMVar loadBalanceHashRing
  unless (getAllocatedNodeId hr subId == serverID) $
    throwIO SubscriptionWatchOnDifferentNode

  Log.debug $ "Receive deleteSubscription request: " <> Log.buildString' req
  subscription <- P.getObject @ZHandle @'SubRep subId zkHandle
  when (isNothing subscription) $ throwIO (SubscriptionIdNotFound subId)
  Core.deleteSubscription ctx (fromJust subscription)
  returnResp Empty
--------------------------------------------------------------------------------

checkSubscriptionExistHandler
  :: ServerContext
  -> ServerRequest 'Normal CheckSubscriptionExistRequest CheckSubscriptionExistResponse
  -> IO (ServerResponse 'Normal CheckSubscriptionExistResponse)
checkSubscriptionExistHandler ServerContext {..} (ServerNormalRequest _metadata req@CheckSubscriptionExistRequest {..}) = do
  Log.debug $ "Receive checkSubscriptionExistHandler request: " <> Log.buildString (show req)
  let sid = checkSubscriptionExistRequestSubscriptionId
  res <- P.checkIfExist @ZHandle @'SubRep sid zkHandle
  returnResp . CheckSubscriptionExistResponse $ res
--------------------------------------------------------------------------------

listSubscriptionsHandler
  :: ServerContext
  -> ServerRequest 'Normal ListSubscriptionsRequest ListSubscriptionsResponse
  -> IO (ServerResponse 'Normal ListSubscriptionsResponse)
listSubscriptionsHandler sc (ServerNormalRequest _metadata ListSubscriptionsRequest) = defaultExceptionHandle $ do
  Log.debug "Receive listSubscriptions request"
  res <- ListSubscriptionsResponse <$> Core.listSubscriptions sc
  Log.debug $ Log.buildString "Result of listSubscriptions: " <> Log.buildString (show res)
  returnResp res
--------------------------------------------------------------------------------

streamingFetchHandler
  :: ServerContext
  -> ServerRequest 'BiDiStreaming StreamingFetchRequest StreamingFetchResponse
  -> IO (ServerResponse 'BiDiStreaming StreamingFetchResponse)
streamingFetchHandler ctx bidiRequest = undefined


streamingFetchInternal
  :: ServerContext
  -> ServerRequest 'BiDiStreaming StreamingFetchRequest StreamingFetchResponse
  -> IO ()
streamingFetchInternal ctx@ServerContext {..} (ServerBiDiRequest _ streamRecv streamSend) = do
  StreamingFetchRequest {..} <- firstRecv 
  wrapper@SubscribeContextWrapper {..} <- initSub ctx subId
  initConsumer scwContext consumerName streamSend
  where
    firstRecv :: IO StreamingFetchRequest 
    firstRecv = undefined

initSub :: ServerContext -> SubscriptionId -> IO SubscribeContextWrapper
initSub ServerContext {..} subId = do
  (needInit, SubscribeContextNewWrapper {..}) <- atomically $ do
    subMap <- readTVar scSubscribeContexts 
    case HM.lookup subId subMap of
      Nothing -> do
        state <- newTVar SubscribeStateNew
        ctx <- newTVar Nothing
        let wrapper = SubscribeContextNewWrapper {scnwState = state, scnwContext = ctx}
        let newSubMap = HM.insert subId wrapper subMap
        writeTVar subContexts newSubMap
        return (True, wrapper)
      Just wrapper@SubscribeContextNewWrapper {..} -> do
        state <- readTVar scnwState
        case state of
          SubscribeStateNew -> retry
          SubscribeStateRunning -> return (False, wrapper)
          -- TODO: how to deal with other state ?
  if needInit
    then do
      subCtx <- doSubInit subId
      atomically $ do
        writeTVar scnwContext (Just subCtx)
        writeTVar scnwState SubscribeStateRunning
        return SubscribeContextWrapper {scwState = scnwState, scwContext = subCtx}
    else do
      mctx <- readTVarIO scnwContext
      return $ SubscribeContextWrapper {scwState = scnwState, scwContext = fromJust mctx}

doSubInit :: ServerContext -> SubscriptionId -> IO SubscribeContext
doSubInit ServerContext{..} subId = do
  P.getObject streamingFetchRequestSubscriptionId zkHandle >>= \case
    Nothing -> do
      Log.error $ "unexpected error: subscription " <> Log.buildText streamingFetchRequestSubscriptionId <> " not exist."
      throwIO $ SubscriptionIdNotFound streamingFetchRequestSubscriptionId
    Just Subscription {..} -> do
      -- create a ldCkpReader for reading new records
      let readerName = textToCBytes streamingFetchRequestSubscriptionId
      ldCkpReader <-
        --TODO: check this
        S.newLDRsmCkpReader scLDClient readerName S.checkpointStoreLogID 5000 1 Nothing 10
      Log.debug $ "created a ldCkpReader for subscription {" <> Log.buildText streamingFetchRequestSubscriptionId <> "}"

      -- create a ldReader for rereading unacked records
      --TODO: check this
      ldReader <- S.newLDReader scLDClient 5000 Nothing
      Log.debug $ "created a ldReader for subscription {" <> Log.buildText streamingFetchRequestSubscriptionId <> "}"

      consumerContexts <- newTVar HM.empty 
      shardContexts <- newTVar HM.empty 
      assignment <- mkEmptyAssignment
      let shardInfo =
            SubscribeContext
              { subSubscriptionId = subId,
                subStreamName = subscriptionStreamName ,
                --TODO: check unit
                subAckTimeoutSeconds = subscriptionAckTimeoutSeconds,
                subLdCkpReader = ldCkpReader, 
                subLdReader = ldReader,
                subConsumerContexts = consumerContexts,
                subShardContexts = shardContexts,
                subAssignment = assignment
              }
  where
    mkEmptyAssignment :: IO Assignment
    mkEmptyAssignment = do 
      total <- newTVarIO Set.empty
      unassign <- newTVarIO [] 
      waitingRead <- newTVarIO []
      waitingReassign <- newTVarIO []
      wc <- newTVarIO []
      s2c <- newTVarIO HM.empty
      c2s <- newTVarIO HM.empty
      cws <- newTVarIO Set.empty

      return 
        Assignment
          { totalShards = total, 
            unassignedShards = unassign, 
            waitingReadShards = waitingRead, 
            waitingReassignedShards = waitingReassign, 
            waitingConsumers = wc, 
            shard2Consumer = s2c,
            consumer2Shards = c2s,
            consumerWorkloads = cws 
          }


