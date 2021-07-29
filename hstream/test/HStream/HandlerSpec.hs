{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.HandlerSpec (spec) where

import           Control.Concurrent               (forkIO, killThread,
                                                   threadDelay)
import           Control.Monad                    (forM, forM_, forever, void)
import qualified Data.ByteString                  as B
import qualified Data.ByteString.Lazy             as BL
import qualified Data.List                        as L
import qualified Data.Map.Strict                  as Map
import           Data.Maybe                       (fromJust, isJust)
import qualified Data.Set                         as Set
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import           Data.Word                        (Word32, Word64)
import           Network.GRPC.HighLevel.Client    (Client)
import           Network.GRPC.HighLevel.Generated
import           Proto3.Suite                     (Enumerated (..))
import           System.IO.Unsafe                 (unsafePerformIO)
import           Test.Hspec
import           Z.Foreign                        (toByteString)

import           HStream.Server.HStreamApi
import           HStream.SpecUtils
import           HStream.Store.Logger
import           HStream.ThirdParty.Protobuf      (Empty (Empty))
import           HStream.Utils

randomStreamNames :: V.Vector TL.Text
randomStreamNames = unsafePerformIO $ V.replicateM 5 $ ("StreamSpec_" <>) . TL.fromStrict <$> newRandomText 20
{-# NOINLINE randomStreamNames #-}

randomStreamName :: TL.Text
randomStreamName = V.head randomStreamNames
{-# NOINLINE randomStreamName #-}

randomSubsciptionIds :: V.Vector TL.Text
randomSubsciptionIds = unsafePerformIO $ V.replicateM 5 $ ("SubscribeSpec_" <>) . TL.fromStrict <$> newRandomText 20
{-# NOINLINE randomSubsciptionIds #-}

randomSubsciptionId :: TL.Text
randomSubsciptionId = V.head randomSubsciptionIds
{-# NOINLINE randomSubsciptionId #-}

spec :: Spec
spec =  do
  runIO setupSigsegvHandler
  runIO $ setLogDeviceDbgLevel C_DBG_ERROR

  aroundAll (\runTest -> do
             withGRPCClient clientConfig $ \client -> do
               runTest client
            ) $ describe "HStream.BasicHandlerSpec" $ do

    basicSpec
    subscribeSpec
    consumerSpec

----------------------------------------------------------------------------------------------------------
-- StreamSpec

cleanStream :: TL.Text -> Client -> IO ()
cleanStream name client = void $ deleteStreamRequest client name

cleanStreams :: V.Vector TL.Text -> Client -> IO ()
cleanStreams names client = V.mapM_ (`cleanStream` client) names

basicSpec :: SpecWith Client
basicSpec = describe "HStream.BasicHandlerSpec.basic" $ do

  after (cleanStream randomStreamName) $ it "test createStream request" $ \client -> do
    let req = Stream randomStreamName 3
    -- The first create should success
    isJust <$> createStreamRequest client req `shouldReturn` True
    -- The second create should fail
    isJust <$> createStreamRequest client req `shouldReturn` False

  after (cleanStreams randomStreamNames) $ it "test listStream request" $ \client -> do
    let createStreamReqs = V.zipWith Stream randomStreamNames $ V.fromList [1, 2, 3, 3, 2]
    V.forM_ createStreamReqs $ \req -> do
      isJust <$> createStreamRequest client req `shouldReturn` True
    resp <- fromJust <$> listStreamRequest client
    let sortedRes = Set.fromList $ V.toList resp
        sortedReqs = Set.fromList $ V.toList createStreamReqs
    sortedReqs `shouldSatisfy` (`Set.isSubsetOf` sortedRes)

  it "test delete request" $ \client -> do
    void $ createStreamRequest client $ Stream randomStreamName 1
    deleteStreamRequest client randomStreamName `shouldReturn` True
    resp <- fromJust <$> listStreamRequest client
    V.map streamStreamName resp `shouldNotSatisfy` V.elem randomStreamName

  after (cleanSubscriptionEnv randomSubsciptionId randomStreamName) $ it "test sendHeartbeat request" $ \client -> do
    -- send heartbeat request to an unsubscribed subscription shoud return false
    sendHeartbeatRequest client randomSubsciptionId `shouldReturn` False

    void $ createStreamRequest client $ Stream randomStreamName 1
    let offset = SubscriptionOffset . Just . SubscriptionOffsetOffsetSpecialOffset . Enumerated . Right $ SubscriptionOffset_SpecialOffsetLATEST
    subscribeRequest client randomSubsciptionId randomStreamName offset `shouldReturn` True
    -- send heartbeat request to an existing subscription should return True
    sendHeartbeatRequest client randomSubsciptionId `shouldReturn` True
    -- after send heartbeat responsed, resubscribe same subscription should return False
    subscribeRequest client randomSubsciptionId randomStreamName offset `shouldReturn` False

    -- after heartbeat timeout, sendHeartbeatRequest should return False
    threadDelay 2000000
    sendHeartbeatRequest client randomSubsciptionId `shouldReturn` False

-------------------------------------------------------------------------------------------------

createStreamRequest :: Client -> Stream -> IO (Maybe Stream)
createStreamRequest client stream = do
  HStreamApi{..} <- hstreamApiClient client
  resp <- hstreamApiCreateStream $ ClientNormalRequest stream requestTimeout $ MetadataMap Map.empty
  case resp of
    ClientNormalResponse respStream _meta1 _meta2 StatusOk _details -> return $ Just respStream
    ClientErrorResponse clientError                                 -> do
      putStrLn ("Create Stream Error: " <> show clientError) >> return Nothing

listStreamRequest :: Client -> IO (Maybe (V.Vector Stream))
listStreamRequest client = do
  HStreamApi{..} <- hstreamApiClient client
  resp <- hstreamApiListStreams $ ClientNormalRequest ListStreamsRequest requestTimeout $ MetadataMap Map.empty
  case resp of
    ClientNormalResponse respStream _meta1 _meta2 StatusOk _details -> do
      return . Just $ listStreamsResponseStreams respStream
    ClientErrorResponse clientError                                 -> do
      putStrLn ("List Stream Error: " <> show clientError) >> return Nothing

deleteStreamRequest :: Client -> TL.Text -> IO Bool
deleteStreamRequest client streamName = do
  HStreamApi{..} <- hstreamApiClient client
  let req = DeleteStreamRequest { deleteStreamRequestStreamName = streamName
                                , deleteStreamRequestIgnoreNonExist = False
                                }
  resp <- hstreamApiDeleteStream $ ClientNormalRequest req requestTimeout $ MetadataMap Map.empty
  case resp of
    ClientNormalResponse _ _meta1 _meta2 StatusOk _details -> return True
    ClientErrorResponse clientError                        -> do
      putStrLn ("Delete Stream Error: " <> show clientError) >> return False

sendHeartbeatRequest :: Client -> TL.Text -> IO Bool
sendHeartbeatRequest client subscriptionId = do
  HStreamApi{..} <- hstreamApiClient client
  let req = ConsumerHeartbeatRequest subscriptionId
  resp <- hstreamApiSendConsumerHeartbeat $ ClientNormalRequest req requestTimeout $ MetadataMap Map.empty
  case resp of
    ClientNormalResponse _ _meta1 _meta2 StatusOk _details -> return True
    ClientErrorResponse clientError                        -> do
      putStrLn ("Send Heartbeat Error: " <> show clientError) >> return False

----------------------------------------------------------------------------------------------------------
-- SubscribeSpec

-- | cleanSubscriptionEnv will clean both subscription and streams
cleanSubscriptionEnv :: TL.Text -> TL.Text -> Client -> IO ()
cleanSubscriptionEnv sId sName client = do
  void $ deleteSubscriptionRequest client sId
  cleanStream sName client

cleanSubscriptionsEnv :: V.Vector TL.Text -> V.Vector TL.Text -> Client -> IO ()
cleanSubscriptionsEnv sIds sNames client = do
  V.zipWithM_ (\subId subName -> cleanSubscriptionEnv subId subName client) sIds sNames

subscribeSpec :: SpecWith Client
subscribeSpec = describe "HStream.BasicHandlerSpec.Subscribe" $ do

  let offset = SubscriptionOffset . Just . SubscriptionOffsetOffsetSpecialOffset . Enumerated . Right $ SubscriptionOffset_SpecialOffsetLATEST

  after (cleanSubscriptionEnv randomSubsciptionId randomStreamName) $ it "test subscribe request" $ \client -> do
    -- subscribe to a nonexistent stream should return False
    subscribeRequest client randomSubsciptionId randomStreamName offset `shouldReturn` False
    void $ createStreamRequest client $ Stream randomStreamName 1
    -- subscribe to an existing stream should return True
    subscribeRequest client randomSubsciptionId randomStreamName offset `shouldReturn` True
    -- resubscribe to a subscribed stream should return False
    subscribeRequest client randomSubsciptionId randomStreamName offset `shouldReturn` False
    -- after some delay without send heartbeat, the subscribe should be released and resubscribe should success
    threadDelay 2000000
    subscribeRequest client randomSubsciptionId randomStreamName offset `shouldReturn` True

  after (cleanSubscriptionsEnv randomSubsciptionIds randomStreamNames) $ it "test listSubscription request" $ \client -> do
    let subscriptions = V.zipWith3 Subscription randomSubsciptionIds randomStreamNames $ V.replicate 5 (Just offset)
    forM_ subscriptions $ \Subscription{..} -> do
      isJust <$> createStreamRequest client (Stream subscriptionStreamName 1) `shouldReturn` True
      subscribeRequest client subscriptionSubscriptionId subscriptionStreamName (fromJust subscriptionOffset) `shouldReturn` True
    resp <- listSubscriptionRequest client
    isJust resp `shouldBe` True
    (V.toList . fromJust $ resp) `shouldMatchList` V.toList subscriptions

  after (cleanSubscriptionEnv randomSubsciptionId randomStreamName) $ it "test deleteSubscription request" $ \client -> do
    -- delete unsubscribed stream should return false
    deleteSubscriptionRequest client randomSubsciptionId `shouldReturn` False
    void $ createStreamRequest client $ Stream randomStreamName 1
    subscribeRequest client randomSubsciptionId randomStreamName offset `shouldReturn` True
    -- delete subscribed stream should return true
    deleteSubscriptionRequest client randomSubsciptionId `shouldReturn` True
    -- after delete subscription, send heartbeat shouldReturn False
    sendHeartbeatRequest client randomSubsciptionId `shouldReturn` False
    res <- listSubscriptionRequest client
    V.length (fromJust res) `shouldBe` 0

  after (cleanSubscriptionsEnv randomSubsciptionIds randomStreamNames) $ it "test hasSubscription request" $ \client -> do
    void $ createStreamRequest client $ Stream randomStreamName 1
    subscribeRequest client randomSubsciptionId randomStreamName offset `shouldReturn` True
    hasSubscriptionRequest client randomSubsciptionId `shouldReturn` True
    threadDelay 2000000
    -- the subscription still exists when the reader's status is released
    hasSubscriptionRequest client randomSubsciptionId `shouldReturn` True
    -- validate the reader's status is released
    subscribeRequest client randomSubsciptionId randomStreamName offset `shouldReturn` True
    deleteSubscriptionRequest client randomSubsciptionId `shouldReturn` True
    hasSubscriptionRequest client randomSubsciptionId `shouldReturn` False

----------------------------------------------------------------------------------------------------------

subscribeRequest :: Client -> TL.Text -> TL.Text -> SubscriptionOffset -> IO Bool
subscribeRequest client subscribeId streamName offset = do
  HStreamApi{..} <- hstreamApiClient client
  let req = Subscription subscribeId streamName $ Just offset
  resp <- hstreamApiSubscribe $ ClientNormalRequest req requestTimeout $ MetadataMap Map.empty
  case resp of
    ClientNormalResponse _ _meta1 _meta2 StatusOk _details -> return True
    ClientErrorResponse clientError                        -> do
      putStrLn ("Subscribe Error: " <> show clientError) >> return False

listSubscriptionRequest :: Client -> IO (Maybe (V.Vector Subscription))
listSubscriptionRequest client = do
  HStreamApi{..} <- hstreamApiClient client
  resp <- hstreamApiListSubscriptions $ ClientNormalRequest ListSubscriptionsRequest requestTimeout $ MetadataMap Map.empty
  case resp of
    ClientNormalResponse res _meta1 _meta2 StatusOk _details -> do
      return . Just . listSubscriptionsResponseSubscription $ res
    ClientErrorResponse clientError                          -> do
      putStrLn ("List Subscription Error: " <> show clientError) >> return Nothing

deleteSubscriptionRequest :: Client -> TL.Text -> IO Bool
deleteSubscriptionRequest client subscribeId = do
  HStreamApi{..} <- hstreamApiClient client
  let req = DeleteSubscriptionRequest subscribeId
  resp <- hstreamApiDeleteSubscription $ ClientNormalRequest req requestTimeout $ MetadataMap Map.empty
  case resp of
    ClientNormalResponse _ _meta1 _meta2 StatusOk _details -> return True
    ClientErrorResponse clientError                        -> do
      putStrLn ("Delete Subscription Error: " <> show clientError) >> return False

hasSubscriptionRequest :: Client -> TL.Text -> IO Bool
hasSubscriptionRequest client subscribeId = do
  HStreamApi{..} <- hstreamApiClient client
  let req = HasSubscriptionRequest subscribeId
  resp <- hstreamApiHasSubscription $ ClientNormalRequest req requestTimeout $ MetadataMap Map.empty
  case resp of
    ClientNormalResponse res _meta1 _meta2 StatusOk _details ->
      return $ hasSubscriptionResponseExists res
    ClientErrorResponse clientError                          -> do
      putStrLn ("Find Subscription Error: " <> show clientError) >> return False

----------------------------------------------------------------------------------------------------------
-- ConsumerSpec

mkConsumerSpecEnv :: ((Client, V.Vector B.ByteString) -> IO a) -> Client -> IO ()
mkConsumerSpecEnv runTest client = do
  let offset = SubscriptionOffset . Just . SubscriptionOffsetOffsetSpecialOffset . Enumerated . Right $ SubscriptionOffset_SpecialOffsetLATEST
  void $ createStreamRequest client $ Stream randomStreamName 1
  void $ subscribeRequest client randomSubsciptionId randomStreamName offset

  timeStamp <- getProtoTimestamp
  let header = buildRecordHeader HStreamRecordHeader_FlagRAW Map.empty timeStamp TL.empty
  batchedBS <- forM [1..5] $ \num -> do
    payloads <- V.replicateM num $ newRandomByteString 2
    let records = V.map (buildRecord header) payloads
    AppendResponse{..} <- fromJust <$> appendRequest client randomStreamName records
    return payloads

  void $ runTest (client, V.concat batchedBS)

  void $ deleteSubscriptionRequest client randomSubsciptionId
  void $ deleteStreamRequest client randomStreamName

consumerSpec :: SpecWith Client
consumerSpec = aroundWith mkConsumerSpecEnv $ describe "HStream.BasicHandlerSpec.Consumer" $ do

  -- FIXME:
  it "test fetch request" $ \(client, reqPayloads) -> do
    resp <- fetchRequest client randomSubsciptionId (fromIntegral requestTimeout) 100
    let resPayloads = V.map rebuildReceivedRecord $ fromJust resp
    reqPayloads `shouldBe` resPayloads

  -- TODO
  --it "test commitOffset request" $ \(client, reqPayloads) -> do
  --  tid <- forkIO $ do
  --    forever $ do
  --      void $ sendHeartbeatRequest client randomSubsciptionId
  --      threadDelay 500000

  --  resp1 <- fetchRequest client randomSubsciptionId (fromIntegral requestTimeout) 1
  --  isJust resp1 `shouldBe` True
  --  let receivedRecord1 = V.head . fromJust $ resp1
  --  let resPayload1 = rebuildReceivedRecord receivedRecord1
  --  resPayload1 `shouldBe` head reqPayloads

  --  let recordId1 = fromJust . receivedRecordRecordId $ receivedRecord1
  --  commitOffsetRequest client randomSubsciptionId randomStreamName recordId1 `shouldReturn` True

  --  resp2 <- fetchRequest client randomSubsciptionId (fromIntegral requestTimeout) 1
  --  isJust resp2 `shouldBe` True
  --  let receivedRecord2 = V.head . fromJust $ resp2
  --  let resPayload2 = rebuildReceivedRecord receivedRecord2
  --  resPayload2 `shouldBe` reqPayloads !! 1

  --  void $ killThread tid

----------------------------------------------------------------------------------------------------------

appendRequest :: Client -> TL.Text -> V.Vector HStreamRecord -> IO (Maybe AppendResponse)
appendRequest client streamName records = do
  HStreamApi{..} <- hstreamApiClient client
  let req = AppendRequest streamName records
  resp <- hstreamApiAppend $ ClientNormalRequest req requestTimeout $ MetadataMap Map.empty
  case resp of
    ClientNormalResponse resp _meta1 _meta2 StatusOk _details -> return $ Just resp
    ClientErrorResponse clientError                           -> do
      putStrLn ("AppendRequest Error: " <> show clientError) >> return Nothing

fetchRequest :: Client -> TL.Text -> Word64 -> Word32 -> IO (Maybe (V.Vector ReceivedRecord))
fetchRequest client subscribeId timeout maxSize = do
  HStreamApi{..} <- hstreamApiClient client
  let req = FetchRequest subscribeId timeout maxSize
  resp <- hstreamApiFetch $ ClientNormalRequest req requestTimeout $ MetadataMap Map.empty
  case resp of
    ClientNormalResponse res _meta1 _meta2 StatusOk _details -> do
      return . Just $ fetchResponseReceivedRecords res
    ClientErrorResponse clientError                          -> do
      putStrLn ("FetchRequest Error: " <> show clientError) >> return Nothing

commitOffsetRequest :: Client -> TL.Text -> TL.Text -> RecordId -> IO Bool
commitOffsetRequest client subscriptionId streamName recordId = do
  HStreamApi{..} <- hstreamApiClient client
  let req = CommittedOffset subscriptionId streamName $ Just recordId
  resp <- hstreamApiCommitOffset $ ClientNormalRequest req requestTimeout $ MetadataMap Map.empty
  case resp of
    ClientNormalResponse _ _meta1 _meta2 StatusOk _details -> return True
    ClientErrorResponse clientError                        -> do
      putStrLn ("Commite Error: " <> show clientError) >> return False

requestTimeout :: Int
requestTimeout = 1000

mkReceivedRecord :: V.Vector HStreamRecord -> V.Vector RecordId -> V.Vector ReceivedRecord
mkReceivedRecord payloads recordIds =
  V.zipWith (\offset record -> ReceivedRecord (Just offset) (encodeRecord record)) recordIds payloads

rebuildReceivedRecord :: ReceivedRecord -> B.ByteString
rebuildReceivedRecord record@ReceivedRecord{..} =
  toByteString . getPayload . decodeByteStringRecord $ receivedRecordRecord
