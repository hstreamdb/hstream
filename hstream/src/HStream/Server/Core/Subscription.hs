{-# LANGUAGE DataKinds        #-}
{-# LANGUAGE TypeApplications #-}
module HStream.Server.Core.Subscription where

import           Control.Concurrent.STM
import           Control.Exception          (Exception, throwIO)
import           Control.Monad              (unless)
import qualified Data.HashMap.Strict        as HM
import qualified Data.Map.Strict            as Map
import           Data.Maybe                 (fromJust)
import           Data.Text                  (Text)
import           Data.Unique                (hashUnique, newUnique)
import qualified Data.Vector                as V
import qualified Z.Data.CBytes              as CB
import           Z.Data.CBytes              (CBytes)
import           ZooKeeper.Recipe           (withLock)
import           ZooKeeper.Types            (ZHandle)

import           HStream.Connector.HStore   (transToStreamName)
import qualified HStream.Logger             as Log
import           HStream.Server.Exception
import           HStream.Server.HStreamApi  (Subscription (..))
import qualified HStream.Server.Persistence as P
import           HStream.Server.Types
import qualified HStream.Store              as S
import           HStream.Utils              (textToCBytes)

--------------------------------------------------------------------------------

listSubscriptions :: ServerContext -> IO (V.Vector Subscription)
listSubscriptions ServerContext{..} =
  V.fromList . Map.elems <$> P.listObjects zkHandle

createSubscription :: ServerContext -> Subscription -> IO ()
createSubscription ServerContext {..} sub@Subscription{..} = do
  let streamName = transToStreamName subscriptionStreamName
  streamExists <- S.doesStreamExist scLDClient streamName
  unless streamExists $ do
    Log.debug $ "Try to create a subscription to a nonexistent stream. Stream Name: "
              <> Log.buildString' streamName
    throwIO StreamNotExist
  P.storeObject subscriptionSubscriptionId sub zkHandle

deleteSubscription :: ServerContext -> Subscription -> Bool -> IO ()
deleteSubscription ServerContext{..} Subscription{subscriptionSubscriptionId = subId
  , subscriptionStreamName = streamName} forced = do
  (status, msub) <- atomically $ do
    res <- getSubState
    case res of
      Nothing -> pure (NotExist, Nothing)
      Just (subCtx, stateVar) -> do
        state <- readTVar stateVar
        case state of
          SubscribeStateNew -> retry
          SubscribeStateRunning -> do
            isActive <- hasValidConsumers subCtx
            if isActive
            then if forced
                 then do
                   writeTVar stateVar SubscribeStateStopping
                   pure (CanDelete, Just (subCtx, stateVar))
                 else
                   pure (CanNotDelete, Just (subCtx, stateVar))
            else do
              writeTVar stateVar SubscribeStateStopping
              pure (CanDelete, Just (subCtx, stateVar))
          SubscribeStateStopping -> pure (Signaled, Just (subCtx, stateVar))
          SubscribeStateStopped  -> pure (Signaled, Just (subCtx, stateVar))
  Log.debug $ "Subscription deletion has state " <> Log.buildString' status
  case status of
    NotExist  ->  doRemove
    CanDelete -> do
      let (subCtx, subState) = fromJust msub
      atomically $ waitingStopped subCtx subState
      Log.info "Subscription stopped, start deleting "
      atomically removeSubFromCtx
      doRemove
    CanNotDelete -> throwIO FoundActiveConsumers
    Signaled     -> throwIO SubscriptionIsDeleting
  where
    doRemove :: IO ()
    doRemove = do
      -- FIXME: There are still inconsistencies here. If any failure occurs after removeSubFromStreamPath
      -- and if the client doesn't retry, then we will find that the subscription still binds to the stream but we
      -- can't get the related subscription's information
      removeSubFromStreamPath zkHandle (textToCBytes streamName) (textToCBytes subId)
      P.removeObject @ZHandle @'P.SubRep subId zkHandle

    getSubState :: STM (Maybe (SubscribeContext, TVar SubscribeState))
    getSubState = do
      scs <- readTVar scSubscribeContexts
      case HM.lookup subId scs of
        Nothing -> return Nothing
        Just SubscribeContextNewWrapper {..}  -> do
          subState <- readTVar scnwState
          case subState of
            SubscribeStateNew -> retry
            _ -> do
              subCtx <- takeTMVar scnwContext
              return $ Just (subCtx, scnwState)

    hasValidConsumers :: SubscribeContext -> STM Bool
    hasValidConsumers SubscribeContext {..} = do
      consumers <- readTVar subConsumerContexts
      pure $ not $ HM.null consumers

    waitingStopped :: SubscribeContext -> TVar SubscribeState -> STM ()
    waitingStopped SubscribeContext {..} subState = do
      consumers <- readTVar subConsumerContexts
      if HM.null consumers
      then pure()
      else retry
      writeTVar subState SubscribeStateStopped

    removeSubFromCtx :: STM ()
    removeSubFromCtx =  do
      scs <- readTVar scSubscribeContexts
      writeTVar scSubscribeContexts (HM.delete subId scs)

data DeleteSubStatus = NotExist | CanDelete | CanNotDelete | Signaled
  deriving (Show)

removeSubFromStreamPath :: ZHandle -> CBytes -> CBytes -> IO ()
removeSubFromStreamPath zk streamName subName = do
  let lockPath = P.mkStreamSubsLockPath streamName
  let subscriptionPath = P.mkStreamSubsPath streamName <> "/" <> subName
  uniq <- newUnique
  withLock zk lockPath (CB.pack . show . hashUnique $ uniq) $ do
    P.tryDeletePath zk subscriptionPath

-- -------------------------------------------------------------------------- --
-- Exceptions

newtype ConsumerExist = ConsumerExist Text
  deriving (Show)
instance Exception ConsumerExist

data FoundActiveConsumers = FoundActiveConsumers
  deriving (Show)
instance Exception FoundActiveConsumers

data SubscriptionIsDeleting = SubscriptionIsDeleting
  deriving (Show)
instance Exception SubscriptionIsDeleting

data SubscriptionWatchOnDifferentNode = SubscriptionWatchOnDifferentNode
