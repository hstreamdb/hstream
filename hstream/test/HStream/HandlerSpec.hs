{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.HandlerSpec (spec) where

import           Control.Concurrent               (forkIO, killThread,
                                                   threadDelay)
import           Control.Monad                    (forM_, forever, replicateM,
                                                   void)
import qualified Data.ByteString                  as B
import qualified Data.ByteString.Lazy             as BL
import qualified Data.List                        as L
import qualified Data.Map.Strict                  as Map
import           Data.Maybe                       (fromJust, isJust)
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import           Data.Word                        (Word32, Word64)
import           Network.GRPC.HighLevel.Client    (Client)
import           Network.GRPC.HighLevel.Generated
import           Proto3.Suite                     (Enumerated (..))
import           Test.Hspec
import           ThirdParty.Google.Protobuf.Empty
import           Z.Foreign                        (toByteString)

import           HStream.Server.HStreamApi
import           HStream.SpecUtils
import           HStream.Store
import           HStream.Utils                    (getProtoTimestamp)
import           HStream.Utils.BuildRecord

requestTimeout :: Int
requestTimeout = 1000

mkReceivedRecord :: V.Vector B.ByteString -> V.Vector RecordId -> V.Vector ReceivedRecord
mkReceivedRecord payloads recordId = V.zipWith (ReceivedRecord . Just) recordId payloads

rebuildReceivedRecord :: ReceivedRecord -> ReceivedRecord
rebuildReceivedRecord record@ReceivedRecord{..} =
  let payload = toByteString . getPayload . decodeByteStringRecord $ receivedRecordRecord
  in record { receivedRecordRecord = payload }

mkAppendPayload :: HStreamRecordHeader -> V.Vector B.ByteString -> V.Vector B.ByteString
mkAppendPayload header = V.map (toByteString . encodeRecord . buildRecord header . BL.fromStrict)

createStreamRequest :: Client -> Stream -> IO (Maybe Stream)
createStreamRequest client stream = do
  HStreamApi{..} <- hstreamApiClient client
  resp <- hstreamApiCreateStream $ ClientNormalRequest stream requestTimeout $ MetadataMap Map.empty
  case resp of
    ClientNormalResponse respStream _meta1 _meta2 status details -> do
      case status of
        StatusOk -> return $ Just respStream
        _        -> putStrLn ("Server Error: " <> show details) >> return Nothing
    ClientErrorResponse clientError                              -> do
      putStrLn ("Client error: " <> show clientError) >> return Nothing

listStreamRequest :: Client -> IO (Maybe (V.Vector Stream))
listStreamRequest client = do
  HStreamApi{..} <- hstreamApiClient client
  resp <- hstreamApiListStreams $ ClientNormalRequest Empty requestTimeout $ MetadataMap Map.empty
  case resp of
    ClientNormalResponse respStream _meta1 _meta2 status details -> do
      case status of
        StatusOk -> return . Just $ listStreamsResponseStreams respStream
        _        -> putStrLn ("Server Error: " <> show details) >> return Nothing
    ClientErrorResponse clientError                              -> do
      putStrLn ("Client error: " <> show clientError) >> return Nothing

deleteStreamRequest :: Client -> TL.Text -> IO Bool
deleteStreamRequest client streamName = do
  HStreamApi{..} <- hstreamApiClient client
  let req = DeleteStreamRequest streamName
  resp <- hstreamApiDeleteStream $ ClientNormalRequest req requestTimeout $ MetadataMap Map.empty
  case resp of
    ClientNormalResponse _ _meta1 _meta2 status details -> do
      case status of
        StatusOk -> return True
        _        -> putStrLn ("Server Error: " <> show details) >> return False
    ClientErrorResponse clientError                     -> do
      putStrLn ("Client Error: " <> show clientError) >> return False

subscribeRequest :: Client -> TL.Text -> TL.Text -> SubscriptionOffset -> IO Bool
subscribeRequest client subscribeId streamName offset = do
  HStreamApi{..} <- hstreamApiClient client
  let req = Subscription subscribeId streamName $ Just offset
  resp <- hstreamApiSubscribe $ ClientNormalRequest req requestTimeout $ MetadataMap Map.empty
  case resp of
    ClientNormalResponse _ _meta1 _meta2 status details -> do
      case status of
        StatusOk -> return True
        _        -> putStrLn ("Server Error: " <> show details) >> return False
    ClientErrorResponse clientError                     -> do
      putStrLn ("Client Error: " <> show clientError) >> return False

listSubscriptionRequest :: Client -> IO (Maybe (V.Vector Subscription))
listSubscriptionRequest client = do
  HStreamApi{..} <- hstreamApiClient client
  resp <- hstreamApiListSubscriptions $ ClientNormalRequest Empty requestTimeout $ MetadataMap Map.empty
  case resp of
    ClientNormalResponse res _meta1 _meta2 status details -> do
      case status of
        StatusOk -> return . Just . listSubscriptionsResponseSubscription $ res
        _        -> putStrLn ("Server Error: " <> show details) >> return Nothing
    ClientErrorResponse clientError                       -> do
      putStrLn ("Client Error: " <> show clientError) >> return Nothing

deleteSubscribeRequest :: Client -> TL.Text -> IO Bool
deleteSubscribeRequest client subscribeId = do
  HStreamApi{..} <- hstreamApiClient client
  let req = DeleteSubscriptionRequest subscribeId
  resp <- hstreamApiDeleteSubscription $ ClientNormalRequest req requestTimeout $ MetadataMap Map.empty
  case resp of
    ClientNormalResponse _ _meta1 _meta2 status details -> do
      case status of
        StatusOk -> return True
        _        -> putStrLn ("Server Error: " <> show details) >> return False
    ClientErrorResponse clientError                     -> do
      putStrLn ("Client Error: " <> show clientError) >> return False

appendRequest :: Client -> TL.Text -> V.Vector B.ByteString -> IO (Maybe AppendResponse)
appendRequest client streamName records = do
  HStreamApi{..} <- hstreamApiClient client
  let req = AppendRequest streamName records
  resp <- hstreamApiAppend $ ClientNormalRequest req requestTimeout $ MetadataMap Map.empty
  case resp of
    ClientNormalResponse x@AppendResponse{} _meta1 _meta2 status details -> do
      case status of
        StatusOk -> return $ Just x
        _        -> putStrLn ("Server Error: " <> show details) >> return Nothing
    ClientErrorResponse clientError -> do
      putStrLn ("Client Error: " <> show clientError) >> return Nothing

fetchRequest :: Client -> TL.Text -> Word64 -> Word32 -> IO (Maybe (V.Vector ReceivedRecord))
fetchRequest client subscribeId timeout maxSize = do
  HStreamApi{..} <- hstreamApiClient client
  let req = FetchRequest subscribeId timeout maxSize
  resp <- hstreamApiFetch $ ClientNormalRequest req requestTimeout $ MetadataMap Map.empty
  case resp of
    ClientNormalResponse res _meta1 _meta2 status details -> do
      case status of
        StatusOk -> return . Just $ fetchResponseReceivedRecords res
        _        -> putStrLn ("Server Error: " <> show details) >> return Nothing
    ClientErrorResponse clientError                       -> do
      putStrLn ("Client Error: " <> show clientError) >> return Nothing

commitOffsetRequest :: Client -> TL.Text -> TL.Text -> RecordId -> IO Bool
commitOffsetRequest client subscriptionId streamName recordId = do
  HStreamApi{..} <- hstreamApiClient client
  let req = CommittedOffset subscriptionId streamName $ Just recordId
  resp <- hstreamApiCommitOffset $ ClientNormalRequest req requestTimeout $ MetadataMap Map.empty
  case resp of
    ClientNormalResponse _ _meta1 _meta2 status details -> do
      case status of
        StatusOk -> return True
        _        -> putStrLn ("Server Error: " <> show details) >> return False
    ClientErrorResponse clientError                     -> do
      putStrLn ("Client Error: " <> show clientError) >> return False

sendHeartbeatRequest :: Client -> TL.Text -> IO Bool
sendHeartbeatRequest client subscriptionId = do
  HStreamApi{..} <- hstreamApiClient client
  let req = ConsumerHeartbeatRequest subscriptionId
  resp <- hstreamApiSendConsumerHeartbeat $ ClientNormalRequest req requestTimeout $ MetadataMap Map.empty
  case resp of
    ClientNormalResponse _ _meta1 _meta2 status details -> do
      case status of
        StatusOk -> return True
        _        -> putStrLn ("Server Error: " <> show details) >> return False
    ClientErrorResponse clientError                       -> do
      putStrLn ("Client Error: " <> show clientError) >> return False

-----------------------------------------------------------------------------------------------------------

spec :: Spec
spec = describe "HStream.BasicHandlerSpec" $ do
   runIO setupSigsegvHandler

   it "test create request" $
      (do
         withGRPCClient clientConfig $ \client -> do
           randomStreamName <- TL.fromStrict <$> newRandomText 20
           let req = Stream randomStreamName 3
           -- The first create should success
           resp1 <- createStreamRequest client req
           res1 <- case resp1 of
             Just _  -> return True
             Nothing -> return False
           -- The second create should fail
           resp2 <- createStreamRequest client req
           res2 <- case resp2 of
             Just _  -> return True
             Nothing -> return False
           void $ deleteStreamRequest client randomStreamName
           return [res1, res2]
      ) `shouldReturn` [True, False]

   it "test listStream request" $
      (do
         withGRPCClient clientConfig $ \client -> do
           randomStreamNames <- V.replicateM 5 $ TL.fromStrict <$> newRandomText 20
           let createStreamReqs = V.zipWith Stream randomStreamNames $ V.fromList [1, 2, 3, 3, 2]
           V.forM_ createStreamReqs $ \req -> do
             isJust <$> createStreamRequest client req `shouldReturn` True
           resp <- listStreamRequest client
           res <- case resp of
              Just streams -> do
                let sortedReqs = L.sortBy (\x y -> compare (streamStreamName x) (streamStreamName y)) $ V.toList createStreamReqs
                    sortedRes = L.sortBy (\x y -> compare (streamStreamName x) (streamStreamName y)) $ V.toList streams
                sortedRes `shouldBe` sortedReqs
                return True
              Nothing -> return False
           V.forM_ randomStreamNames $ \sName -> do
             deleteStreamRequest client sName `shouldReturn` True
           return res
      ) `shouldReturn` True

   it "test delete request" $
     (do
        withGRPCClient clientConfig $ \client -> do
          randomStreamName <- TL.fromStrict <$> newRandomText 20
          void $ createStreamRequest client $ Stream randomStreamName 1
          resp <- deleteStreamRequest client randomStreamName
          if resp
          then do
            fromJust <$> listStreamRequest client `shouldReturn` V.empty
            return True
          else return resp
     ) `shouldReturn` True

   it "test subscribe request" $ withGRPCClient clientConfig $ \client ->
     do
       randomSubsciptionId <- TL.fromStrict <$> newRandomText 10
       randomStreamName <- TL.fromStrict <$> newRandomText 20
       let offset = SubscriptionOffset . Just . SubscriptionOffsetOffsetSpecialOffset . Enumerated . Right $ SubscriptionOffset_SpecialOffsetLATEST
       -- subscribe unexisted stream should return False
       subscribeRequest client randomSubsciptionId randomStreamName offset `shouldReturn` False
       void $ createStreamRequest client $ Stream randomStreamName 1
       -- subscribe existed stream should return True
       subscribeRequest client randomSubsciptionId randomStreamName offset `shouldReturn` True
       -- resubscribe a subscribed stream should return False
       subscribeRequest client randomSubsciptionId randomStreamName offset `shouldReturn` False
       -- after some delay without send heartbeat, the subscribe should be release and resubscribe should success
       threadDelay 3000000
       subscribeRequest client randomSubsciptionId randomStreamName offset `shouldReturn` True
       deleteSubscribeRequest client randomSubsciptionId `shouldReturn` True
       deleteStreamRequest client randomStreamName `shouldReturn` True

   it "test listSubscription request" $ withGRPCClient clientConfig $ \client ->
     do
       randomSubsciptionIds <- replicateM 5 $ TL.fromStrict <$> newRandomText 20
       randomStreamNames <- replicateM 5 $ TL.fromStrict <$> newRandomText 10
       let offset = Just . SubscriptionOffset . Just . SubscriptionOffsetOffsetSpecialOffset . Enumerated . Right $ SubscriptionOffset_SpecialOffsetLATEST
       let subscriptions = zipWith3 Subscription randomSubsciptionIds randomStreamNames $ replicate 5 offset
       forM_ subscriptions $ \Subscription{..} -> do
         isJust <$> createStreamRequest client (Stream subscriptionStreamName 1) `shouldReturn` True
         subscribeRequest client subscriptionSubscriptionId subscriptionStreamName (fromJust subscriptionOffset) `shouldReturn` True
       resp <- listSubscriptionRequest client
       isJust resp `shouldBe` True
       L.sort (V.toList . fromJust $ resp) `shouldBe` L.sort subscriptions
       forM_ subscriptions $ \Subscription{..} -> do
         deleteSubscribeRequest client subscriptionSubscriptionId `shouldReturn` True
         deleteStreamRequest client subscriptionStreamName `shouldReturn` True

   it "test deleteSubscribe request" $ withGRPCClient clientConfig $ \client ->
     do
       randomSubsciptionId <- TL.fromStrict <$> newRandomText 10
       -- delete unsubscribed stream should return False
       deleteSubscribeRequest client randomSubsciptionId `shouldReturn` False
       randomStreamName <- TL.fromStrict <$> newRandomText 20
       void $ createStreamRequest client $ Stream randomStreamName 1
       let offset = SubscriptionOffset . Just . SubscriptionOffsetOffsetSpecialOffset . Enumerated . Right $ SubscriptionOffset_SpecialOffsetLATEST
       subscribeRequest client randomSubsciptionId randomStreamName offset `shouldReturn` True
       -- delete subscribed stream should return true
       deleteSubscribeRequest client randomSubsciptionId `shouldReturn` True
       res <- listSubscriptionRequest client
       V.length (fromJust res) `shouldBe` 0
       deleteStreamRequest client randomStreamName `shouldReturn` True

   it "test append request" $
      (do
         withGRPCClient clientConfig $ \client -> do
           timeStamp <- getProtoTimestamp
           records <- V.replicateM 5 $ newRandomByteString 20
           let streamName = TL.pack "testStream"
               header = buildRecordHeader rawPayloadFlag Map.empty timeStamp TL.empty
               payloads = mkAppendPayload header records
           isJust <$> createStreamRequest client (Stream streamName 3) `shouldReturn` True
           resp <- appendRequest client streamName payloads
           res <- case resp of
             Just _  -> return True
             Nothing -> return False
           deleteStreamRequest client streamName `shouldReturn` True
           return res
       ) `shouldReturn` True

   it "test fetch request" $ withGRPCClient clientConfig $ \client ->
     do
       randomStreamName <- TL.fromStrict <$> newRandomText 20
       void $ createStreamRequest client $ Stream randomStreamName 1
       randomSubsciptionId <- TL.fromStrict <$> newRandomText 10
       let offset = SubscriptionOffset . Just . SubscriptionOffsetOffsetSpecialOffset . Enumerated . Right $ SubscriptionOffset_SpecialOffsetLATEST
       subscribeRequest client randomSubsciptionId randomStreamName offset `shouldReturn` True

       timeStamp <- getProtoTimestamp
       let header = buildRecordHeader rawPayloadFlag Map.empty timeStamp TL.empty
       batchRecords <- V.forM (V.fromList [1..5]) $ \num -> do
         records <- V.replicateM num $ newRandomByteString 2
         let payloads = mkAppendPayload header records
         res <- appendRequest client randomStreamName payloads
         case res of
           Just AppendResponse{..} -> return . V.toList $ mkReceivedRecord records appendResponseRecordIds
           Nothing -> return . V.toList $ V.empty

       resp <- fetchRequest client randomSubsciptionId (fromIntegral requestTimeout) 100
       case resp of
         Just res -> do
           let resPayloads = V.map rebuildReceivedRecord res
               reqPayloads = concat batchRecords
           reqPayloads `shouldBe` V.toList resPayloads
         Nothing  -> putStrLn "error"
       deleteSubscribeRequest client randomSubsciptionId `shouldReturn` True
       deleteStreamRequest client randomStreamName `shouldReturn` True

   it "test commitOffset request" $ withGRPCClient clientConfig $ \client ->
     do
       randomStreamName <- TL.fromStrict <$> newRandomText 20
       void $ createStreamRequest client $ Stream randomStreamName 1
       randomSubsciptionId <- TL.fromStrict <$> newRandomText 10
       let offset = SubscriptionOffset . Just . SubscriptionOffsetOffsetSpecialOffset . Enumerated . Right $ SubscriptionOffset_SpecialOffsetLATEST
       subscribeRequest client randomSubsciptionId randomStreamName offset `shouldReturn` True
       tid <- forkIO $ do
         forever $ do
           void $ sendHeartbeatRequest client randomSubsciptionId
           threadDelay 500000

       timeStamp <- getProtoTimestamp
       let header = buildRecordHeader rawPayloadFlag Map.empty timeStamp TL.empty
       batchRecords <- V.forM (V.fromList [1..3]) $ \num -> do
         records <- V.replicateM num $ newRandomByteString 2
         let payloads = mkAppendPayload header records
         res <- appendRequest client randomStreamName payloads
         case res of
           Just AppendResponse{..} -> return . V.toList $ mkReceivedRecord records appendResponseRecordIds
           Nothing -> return . V.toList $ V.empty
       let reqPayloads = concat batchRecords

       resp1 <- fetchRequest client randomSubsciptionId (fromIntegral requestTimeout) 1
       isJust resp1 `shouldBe` True
       let receivedRecord1 = V.head . fromJust $ resp1
       let resPayload1 = rebuildReceivedRecord receivedRecord1
       resPayload1 `shouldBe` reqPayloads !! 0

       let recordId1 = fromJust . receivedRecordRecordId $ receivedRecord1
       commitOffsetRequest client randomSubsciptionId randomStreamName recordId1 `shouldReturn` True

       resp2 <- fetchRequest client randomSubsciptionId (fromIntegral requestTimeout) 1
       isJust resp2 `shouldBe` True
       let receivedRecord2 = V.head . fromJust $ resp2
       let resPayload2 = rebuildReceivedRecord receivedRecord2
       resPayload2 `shouldBe` reqPayloads !! 1

       void $ killThread tid
       deleteSubscribeRequest client randomSubsciptionId `shouldReturn` True
       deleteStreamRequest client randomStreamName `shouldReturn` True

   it "test sendHeartbeat request" $ withGRPCClient clientConfig $ \client ->
     do
       randomSubsciptionId <- TL.fromStrict <$> newRandomText 10
       -- send heartbeat request to an unsubscribed subscription shoud return false
       sendHeartbeatRequest client randomSubsciptionId `shouldReturn` False

       randomStreamName <- TL.fromStrict <$> newRandomText 20
       void $ createStreamRequest client $ Stream randomStreamName 1
       let offset = SubscriptionOffset . Just . SubscriptionOffsetOffsetSpecialOffset . Enumerated . Right $ SubscriptionOffset_SpecialOffsetLATEST
       subscribeRequest client randomSubsciptionId randomStreamName offset `shouldReturn` True
       -- send heartbeat request to an exist subscription should return True
       sendHeartbeatRequest client randomSubsciptionId `shouldReturn` True
       -- after send heartbeat responsed, resubscribe same subscription should return False
       subscribeRequest client randomSubsciptionId randomStreamName offset `shouldReturn` False

       deleteSubscribeRequest client randomSubsciptionId `shouldReturn` True
       deleteStreamRequest client randomStreamName `shouldReturn` True
