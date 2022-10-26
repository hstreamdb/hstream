{-# LANGUAGE DataKinds       #-}
{-# LANGUAGE GADTs           #-}
{-# LANGUAGE RecordWildCards #-}

module HStream.Client.Internal
  ( callSubscription
  , callDeleteSubscription
  , callDeleteSubscriptionAll
  , callListSubscriptions
  , callStreamingFetch
  ) where

import           Control.Concurrent               (readMVar)
import           Control.Monad                    (forM_, void)
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated (ClientRequest (..),
                                                   ClientResult (..),
                                                   GRPCMethodType (..),
                                                   withGRPCClient)
import           Proto3.Suite                     (Enumerated (Enumerated))

import           HStream.Client.Execute           (executeWithAddr_,
                                                   lookupSubscription)
import           HStream.Client.Types             (HStreamSqlContext (..))
import           HStream.Client.Utils             (mkClientNormalRequest')
import qualified HStream.Server.HStreamApi        as API
import           HStream.Utils                    (HStreamClientApi,
                                                   mkGRPCClientConfWithSSL,
                                                   serverNodeToSocketAddr)

callSubscription :: HStreamSqlContext -> T.Text -> T.Text -> IO ()
callSubscription ctx subId stream = void $ execute ctx getRespApp handleRespApp
  where
    getRespApp API.HStreamApi{..}  = do
      let subReq = API.Subscription
                   { API.subscriptionSubscriptionId = subId
                   , API.subscriptionStreamName = stream
                   , API.subscriptionAckTimeoutSeconds = 1
                   , API.subscriptionMaxUnackedRecords = 100
                   , API.subscriptionOffset = Enumerated (Right API.SpecialOffsetLATEST)
                   }
      hstreamApiCreateSubscription (mkClientNormalRequest' subReq)
    handleRespApp :: ClientResult 'Normal API.Subscription -> IO ()
    handleRespApp resp = case resp of
      (ClientNormalResponse resp_ _meta1 _meta2 _code _details) -> do
        putStrLn "-----------------"
        print resp_
        putStrLn "-----------------"
      _ -> putStrLn "Failed!"

callDeleteSubscription :: HStreamSqlContext -> T.Text -> IO ()
callDeleteSubscription ctx subId = void $ execute ctx getRespApp handleRespApp
  where
    getRespApp API.HStreamApi{..} = do
      let req = API.DeleteSubscriptionRequest
                { deleteSubscriptionRequestSubscriptionId = subId,
                  deleteSubscriptionRequestForce = True
                }
      hstreamApiDeleteSubscription (mkClientNormalRequest' req)
    handleRespApp resp = case resp of
      ClientNormalResponse {} -> do
        putStrLn "-----------------"
        putStrLn "Done."
        putStrLn "-----------------"
      _ -> putStrLn "Failed!"

callDeleteSubscriptionAll :: HStreamSqlContext -> IO ()
callDeleteSubscriptionAll HStreamSqlContext{..} = do
  curNode <- readMVar currentServer
  withGRPCClient (mkGRPCClientConfWithSSL curNode sslConfig) $ \client -> do
    API.HStreamApi{..} <- API.hstreamApiClient client
    let listReq = API.ListSubscriptionsRequest
    listResp <- hstreamApiListSubscriptions (mkClientNormalRequest' listReq)
    case listResp of
      (ClientNormalResponse (API.ListSubscriptionsResponse subs) _meta1 _meta2 _code _details) -> do
        forM_ subs $ \API.Subscription{..} -> do
          let delReq = API.DeleteSubscriptionRequest
                       { deleteSubscriptionRequestSubscriptionId = subscriptionSubscriptionId
                       , deleteSubscriptionRequestForce = True
                       }
          _ <- hstreamApiDeleteSubscription (mkClientNormalRequest' delReq)
          return ()
        putStrLn "-----------------"
        putStrLn "Done."
        putStrLn "-----------------"
      _ -> putStrLn "Failed!"

callListSubscriptions :: HStreamSqlContext -> IO ()
callListSubscriptions ctx = void $ execute ctx getRespApp handleRespApp
  where
    getRespApp API.HStreamApi{..} = do
      let req = API.ListSubscriptionsRequest
      hstreamApiListSubscriptions (mkClientNormalRequest' req)
    handleRespApp :: ClientResult 'Normal API.ListSubscriptionsResponse -> IO ()
    handleRespApp resp = case resp of
      (ClientNormalResponse (API.ListSubscriptionsResponse subs) _meta1 _meta2 _code _details) -> do
        putStrLn "-----------------"
        mapM_ print subs
        putStrLn "-----------------"
      _ -> putStrLn "Failed!"

callStreamingFetch :: HStreamSqlContext -> V.Vector API.RecordId -> T.Text -> T.Text -> IO ()
callStreamingFetch ctx@HStreamSqlContext{..} startRecordIds subId clientId = do
  curNode <- readMVar currentServer
  m_node <- lookupSubscription ctx curNode subId
  case m_node of
    Nothing   -> putStrLn "Subscription not found"
    Just node -> withGRPCClient (mkGRPCClientConfWithSSL (serverNodeToSocketAddr node) sslConfig) $ \client -> do
      API.HStreamApi{..} <- API.hstreamApiClient client
      void $ hstreamApiStreamingFetch (ClientBiDiRequest 10000 mempty action)
  where
    action _clientCall _meta streamRecv streamSend _writeDone = do
      let initReq = API.StreamingFetchRequest
                    { API.streamingFetchRequestSubscriptionId = subId
                    , API.streamingFetchRequestConsumerName = clientId
                    , API.streamingFetchRequestAckIds = startRecordIds
                    }
      _ <- streamSend initReq
      recving
      where
        recving :: IO ()
        recving = do
          m_recv <- streamRecv
          case m_recv of
            Left err -> print err
            Right (Just resp@API.StreamingFetchResponse{..}) -> do
              let recIds = maybe V.empty API.receivedRecordRecordIds streamingFetchResponseReceivedRecords
              print resp
              let ackReq = API.StreamingFetchRequest
                           { API.streamingFetchRequestSubscriptionId = subId
                           , API.streamingFetchRequestConsumerName = clientId
                           , API.streamingFetchRequestAckIds = recIds
                           }
              _ <- streamSend ackReq
              recving
            Right Nothing -> do
              putStrLn "Stopped."

execute :: HStreamSqlContext
  -> (HStreamClientApi -> IO (ClientResult 'Normal a))
  -> (ClientResult 'Normal a -> IO ())
  -> IO ()
execute ctx@HStreamSqlContext{..} action cont = do
  addr <- readMVar currentServer
  executeWithAddr_ ctx addr action cont
