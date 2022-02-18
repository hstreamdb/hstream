{-# LANGUAGE DataKinds        #-}
{-# LANGUAGE TypeApplications #-}
module HStream.Server.Core.Subscription where

import           Control.Concurrent            (modifyMVar_, withMVar)
import           Control.Exception             (throwIO)
import           Control.Monad                 (unless)
import qualified Data.HashMap.Strict           as HM
import qualified Data.Map.Strict               as Map
import qualified Data.Set                      as Set
import qualified Data.Vector                   as V
import           ZooKeeper.Types               (ZHandle)

import           HStream.Connector.HStore      (transToStreamName)
import qualified HStream.Logger                as Log
import           HStream.Server.Exception
import           HStream.Server.HStreamApi
import           HStream.Server.Handler.Common (removeSubFromStreamPath)
import qualified HStream.Server.Persistence    as P
import           HStream.Server.Types
import qualified HStream.Store                 as S
import           HStream.Utils                 (textToCBytes)

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

deleteSubscription :: ServerContext -> Subscription -> IO ()
deleteSubscription ServerContext {..} Subscription{subscriptionSubscriptionId = subId
  , subscriptionStreamName = streamName} = do
  mInfo <- withMVar scSubscribeRuntimeInfo (return . HM.lookup subId)
  case mInfo of
    Just SubscribeRuntimeInfo {..} ->
      withMVar sriWatchContext checkNotActive
    Nothing -> pure ()
  -- FIXME: There are still inconsistencies here. If any failure occurs after removeSubFromStreamPath
  -- and if the client doesn't retry, then we will find that the subscription still binds to the stream but we
  -- can't get the related subscription's information
  removeSubFromStreamPath zkHandle (textToCBytes streamName) (textToCBytes subId)
  P.removeObject @ZHandle @'P.SubRep subId zkHandle
  modifyMVar_ scSubscribeRuntimeInfo $ \subMap -> do
    return $ HM.delete subId subMap

--------------------------------------------------------------------------------

checkNotActive :: WatchContext -> IO ()
checkNotActive WatchContext {..}
  | HM.null wcWatchStopSignals && Set.null wcWorkingConsumers
              = return ()
  | otherwise = throwIO FoundActiveConsumers
