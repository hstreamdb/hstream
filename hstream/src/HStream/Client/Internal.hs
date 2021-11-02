{-# LANGUAGE DataKinds       #-}
{-# LANGUAGE GADTs           #-}
{-# LANGUAGE RecordWildCards #-}

module HStream.Client.Internal
  ( callSubscription
  , callDeleteSubscription
  , callListSubscriptions
  , callStreamingFetch
  ) where

import           Control.Concurrent               (readMVar, threadDelay)
import           Control.Monad                    (void)
import           Data.Maybe                       (fromJust, isJust)
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Client
import           Network.GRPC.HighLevel.Generated
import           Proto3.Suite                     (Enumerated (Enumerated))

import           HStream.Client.Execute           (executeWithAddr_)
import           HStream.Client.Gadget
import           HStream.Client.Type
import           HStream.Client.Utils
import qualified HStream.Server.HStreamApi        as API
import           HStream.Utils                    (HStreamClientApi)

callSubscription :: ClientContext -> T.Text -> T.Text -> IO ()
callSubscription ctx subId stream = void $ execute ctx getRespApp handleRespApp
  where
    getRespApp API.HStreamApi{..}  = do
      let subReq = API.Subscription
                   { API.subscriptionSubscriptionId = subId
                   , API.subscriptionStreamName = stream
                   , API.subscriptionOffset = Just $ API.SubscriptionOffset
                     (Just $ API.SubscriptionOffsetOffsetSpecialOffset
                       (Enumerated (Right API.SubscriptionOffset_SpecialOffsetLATEST))
                     )
                   , API.subscriptionAckTimeoutSeconds = 1
                   }
      hstreamApiCreateSubscription (mkClientNormalRequest subReq)
    handleRespApp :: ClientResult 'Normal API.Subscription -> IO ()
    handleRespApp resp = case resp of
      (ClientNormalResponse resp_ _meta1 _meta2 _code _details) -> do
        putStrLn "-----------------"
        print resp_
        putStrLn "-----------------"
      _ -> putStrLn "Failed!"

callDeleteSubscription :: ClientContext -> T.Text -> IO ()
callDeleteSubscription ctx subId = void $ execute ctx getRespApp handleRespApp
  where
    getRespApp API.HStreamApi{..} = do
      let req = API.DeleteSubscriptionRequest
                { deleteSubscriptionRequestSubscriptionId = subId
                }
      hstreamApiDeleteSubscription (mkClientNormalRequest req)
    handleRespApp resp = case resp of
      ClientNormalResponse {} -> do
        putStrLn "-----------------"
        putStrLn "Done."
        putStrLn "-----------------"
      _ -> putStrLn "Failed!"

callListSubscriptions :: ClientContext -> IO ()
callListSubscriptions ctx = void $ execute ctx getRespApp handleRespApp
  where
    getRespApp API.HStreamApi{..} = do
      let req = API.ListSubscriptionsRequest
      hstreamApiListSubscriptions (mkClientNormalRequest req)
    handleRespApp :: ClientResult 'Normal API.ListSubscriptionsResponse -> IO ()
    handleRespApp resp = case resp of
      (ClientNormalResponse (API.ListSubscriptionsResponse subs) _meta1 _meta2 _code _details) -> do
        putStrLn "-----------------"
        mapM_ print subs
        putStrLn "-----------------"
      _ -> putStrLn "Failed!"

callStreamingFetch :: ClientContext -> V.Vector API.RecordId -> T.Text -> T.Text -> IO ()
callStreamingFetch ctx startRecordIds subId clientId = do
  curNode <- readMVar (currentServer ctx)
  m_node <- lookupSubscription ctx curNode subId
  case m_node of
    Nothing   -> putStrLn "Subscription not found"
    Just node -> withGRPCClient (mkGRPCClientConf . serverNodeToSocketAddr $ node) $ \client -> do
      API.HStreamApi{..} <- API.hstreamApiClient client
      void $ hstreamApiStreamingFetch (ClientBiDiRequest 10000 mempty action)
  where
    action _clientCall _meta streamRecv streamSend _writeDone = do
      go startRecordIds
      where
        go recordIds = do
          let req = API.StreamingFetchRequest
                    { API.streamingFetchRequestSubscriptionId = subId
                    , API.streamingFetchRequestConsumerName = clientId
                    , API.streamingFetchRequestAckIds = recordIds
                    }
          void $ streamSend req
          m_recv <- streamRecv
          case m_recv of
            Left err -> print err
            Right (Just resp@API.StreamingFetchResponse{..}) -> do
              let recIds = V.map fromJust $ V.filter isJust $ API.receivedRecordRecordId <$> streamingFetchResponseReceivedRecords
              print resp
              go recIds
            Right Nothing -> do
              putStrLn "Stopped. Redirecting..."
              threadDelay 2000000
              callStreamingFetch ctx recordIds subId clientId

execute :: ClientContext
  -> (HStreamClientApi -> IO (ClientResult 'Normal a))
  -> (ClientResult 'Normal a -> IO ())
  -> IO ()
execute ctx@ClientContext{..} action cont = do
  addr <- readMVar currentServer
  executeWithAddr_ ctx addr action cont
