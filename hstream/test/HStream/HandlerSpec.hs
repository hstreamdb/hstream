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
import qualified Data.Map.Strict                  as Map
import           Data.Maybe                       (fromJust, isJust)
import qualified Data.Set                         as Set
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import           Data.Word                        (Word32, Word64)
import           Network.GRPC.HighLevel.Client    (Client)
import           Network.GRPC.HighLevel.Generated
import           Proto3.Suite                     (Enumerated (..))
import           Proto3.Suite.Class               (HasDefault (def))
import           System.IO.Unsafe                 (unsafePerformIO)
import           Test.Hspec
import           Z.Foreign                        (toByteString)

import           HStream.Server.HStreamApi
import           HStream.SpecUtils
import           HStream.Store.Logger
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

  it "test deleteStream request" $ \client -> do
    void $ createStreamRequest client $ Stream randomStreamName 1
    deleteStreamRequest client randomStreamName `shouldReturn` True
    resp <- fromJust <$> listStreamRequest client
    V.map streamStreamName resp `shouldNotSatisfy` V.elem randomStreamName

  after (cleanSubscriptionEnv randomSubsciptionId randomStreamName) $ it "test sendHeartbeat request" $ \client -> do
    -- send heartbeat request to an unsubscribed subscription shoud return false
    sendHeartbeatRequest client randomSubsciptionId `shouldReturn` False

    void $ createStreamRequest client $ Stream randomStreamName 1
    let offset = SubscriptionOffset . Just . SubscriptionOffsetOffsetSpecialOffset . Enumerated . Right $ SubscriptionOffset_SpecialOffsetLATEST
    createSubscriptionRequest client randomSubsciptionId randomStreamName offset `shouldReturn` True
    subscribeRequest client randomSubsciptionId `shouldReturn` True
    -- send heartbeat request to an existing subscription should return True
    sendHeartbeatRequest client randomSubsciptionId `shouldReturn` True
    -- after send heartbeat responsed, resubscribe same subscription should return False
    subscribeRequest client randomSubsciptionId `shouldReturn` False

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
  let req = def { deleteStreamRequestStreamName = streamName }
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

  after (cleanSubscriptionEnv randomSubsciptionId randomStreamName) $ it "test createSubscribe request" $ \client -> do
    -- createSubscribe with a nonexistent stream should return False
    createSubscriptionRequest client randomSubsciptionId randomStreamName offset `shouldReturn` False
    isJust <$> createStreamRequest client (Stream randomStreamName 1) `shouldReturn` True
    -- createSubscribe with an existing stream should return True
    createSubscriptionRequest client randomSubsciptionId randomStreamName offset `shouldReturn` True
    -- re-createSubscribe with a used subscriptionId should return False
    createSubscriptionRequest client randomSubsciptionId randomStreamName offset `shouldReturn` False

  aroundWith
    (\runTest client -> do
        void $ createStreamRequest client $ Stream randomStreamName 1
        void $ createSubscriptionRequest client randomSubsciptionId randomStreamName offset
        runTest client
        void $ cleanSubscriptionEnv randomSubsciptionId randomStreamName client
    ) $ it "test subscribe request" $ \client -> do

        let sId = V.last randomSubsciptionIds
        -- subscribe a nonexistent subscriptionId should return False
        subscribeRequest client sId `shouldReturn` False
        -- subscribe a Released subscriptionId should return True
        subscribeRequest client randomSubsciptionId `shouldReturn` True
        -- subscribe a Occupied subscriptionId should return False
        subscribeRequest client randomSubsciptionId `shouldReturn` False
        threadDelay 2000000
        -- after some delay the subscriptionId should be released and resubscribe should success
        subscribeRequest client randomSubsciptionId `shouldReturn` True

  aroundWith
    (\runTest client -> do
        let subscriptions = V.zipWith3 Subscription randomSubsciptionIds randomStreamNames $ V.replicate 5 (Just offset)
        forM_ subscriptions $ \Subscription{..} -> do
          void $ createStreamRequest client (Stream subscriptionStreamName 1)
          void $ createSubscriptionRequest client subscriptionSubscriptionId subscriptionStreamName (fromJust subscriptionOffset)
          subscribeRequest client subscriptionSubscriptionId `shouldReturn` True
        runTest (client, subscriptions)
        void $ cleanSubscriptionsEnv randomSubsciptionIds randomStreamNames client
    ) $ it "test listSubscription request" $ \(client, subscriptions) -> do

        resp <- listSubscriptionRequest client
        isJust resp `shouldBe` True
        let respSet = Set.fromList $ V.toList .fromJust $ resp
            reqSet = Set.fromList $ V.toList subscriptions
        reqSet `shouldSatisfy` (`Set.isSubsetOf` respSet)

  after (cleanSubscriptionEnv randomSubsciptionId randomStreamName) $ it "test deleteSubscription request" $ \client -> do
    -- delete unsubscribed stream should return false
    deleteSubscriptionRequest client randomSubsciptionId `shouldReturn` False
    void $ createStreamRequest client $ Stream randomStreamName 1
    void $ createSubscriptionRequest client randomSubsciptionId randomStreamName offset
    subscribeRequest client randomSubsciptionId `shouldReturn` True
    -- delete subscribed stream should return true
    deleteSubscriptionRequest client randomSubsciptionId `shouldReturn` True
    -- after delete subscription, send heartbeat shouldReturn False
    sendHeartbeatRequest client randomSubsciptionId `shouldReturn` False
    res <- fromJust <$> listSubscriptionRequest client
    V.find (\Subscription{..} -> subscriptionSubscriptionId == randomSubsciptionId) res `shouldBe` Nothing

  after (cleanStream randomStreamName) $ it "test hasSubscription request" $ \client -> do
    void $ createStreamRequest client $ Stream randomStreamName 1
    -- check a nonexistent subscriptionId should return False
    checkSubscriptionExistRequest client randomSubsciptionId `shouldReturn` False
    createSubscriptionRequest client randomSubsciptionId randomStreamName offset `shouldReturn` True
    -- the subscription should exists when the reader's status is released
    checkSubscriptionExistRequest client randomSubsciptionId `shouldReturn` True
    subscribeRequest client randomSubsciptionId `shouldReturn` True
    -- the subscription should exists when the reader's status is Occupied
    checkSubscriptionExistRequest client randomSubsciptionId `shouldReturn` True
    deleteSubscriptionRequest client randomSubsciptionId `shouldReturn` True
    checkSubscriptionExistRequest client randomSubsciptionId `shouldReturn` False

----------------------------------------------------------------------------------------------------------

createSubscriptionRequest :: Client -> TL.Text -> TL.Text -> SubscriptionOffset -> IO Bool
createSubscriptionRequest client subscriptionId streamName offset = do
  HStreamApi{..} <- hstreamApiClient client
  let req = Subscription subscriptionId streamName $ Just offset
  resp <- hstreamApiCreateSubscription $ ClientNormalRequest req requestTimeout $ MetadataMap Map.empty
  case resp of
    ClientNormalResponse _ _meta1 _meta2 StatusOk _details -> return True
    ClientErrorResponse clientError                        -> do
      putStrLn ("createSubscribe Error: " <> show clientError) >> return False

subscribeRequest :: Client -> TL.Text -> IO Bool
subscribeRequest client subscribeId = do
  HStreamApi{..} <- hstreamApiClient client
  let req = SubscribeRequest subscribeId
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

checkSubscriptionExistRequest :: Client -> TL.Text -> IO Bool
checkSubscriptionExistRequest client subscribeId = do
  HStreamApi{..} <- hstreamApiClient client
  let req = CheckSubscriptionExistRequest subscribeId
  resp <- hstreamApiCheckSubscriptionExist $ ClientNormalRequest req requestTimeout $ MetadataMap Map.empty
  case resp of
    ClientNormalResponse res _meta1 _meta2 StatusOk _details ->
      return $ checkSubscriptionExistResponseExists res
    ClientErrorResponse clientError                          -> do
      putStrLn ("Find Subscription Error: " <> show clientError) >> return False

----------------------------------------------------------------------------------------------------------
-- ConsumerSpec

mkConsumerSpecEnv :: ((Client, V.Vector B.ByteString) -> IO a) -> Client -> IO ()
mkConsumerSpecEnv runTest client = do
  let offset = SubscriptionOffset . Just . SubscriptionOffsetOffsetSpecialOffset . Enumerated . Right $ SubscriptionOffset_SpecialOffsetLATEST
  void $ createStreamRequest client $ Stream randomStreamName 1
  void $ createSubscriptionRequest client randomSubsciptionId randomStreamName offset
  void $ subscribeRequest client randomSubsciptionId

  timeStamp <- getProtoTimestamp
  let header = buildRecordHeader HStreamRecordHeader_FlagRAW Map.empty timeStamp TL.empty
  batchedBS <- replicateM 5 $ do
    payloads <- newRandomByteString 2
    let records = buildRecord header payloads
    void $ appendRequest client randomStreamName $ V.singleton records
    return payloads

  void $ runTest (client, V.fromList batchedBS)

  void $ deleteSubscriptionRequest client randomSubsciptionId
  void $ deleteStreamRequest client randomStreamName

consumerSpec :: SpecWith Client
consumerSpec = aroundWith mkConsumerSpecEnv $ describe "HStream.BasicHandlerSpec.Consumer" $ do

  -- FIXME:
  it "test fetch request" $ \(client, reqPayloads) -> do
    putStrLn $ "reqPayloads = " <> show reqPayloads
    resp <- fetchRequest client randomSubsciptionId (fromIntegral requestTimeout) 100
    let resPayloads = V.map getReceivedRecordPayload resp
    putStrLn $ "respPayload = " <> show resPayloads
    resPayloads `shouldBe` reqPayloads

  it "test multi consumer without commit" $ \(client, reqPayloads) -> do
    putStrLn $ "payloads = " <> show reqPayloads
    tid <- forkIO $ do
      forever $ do
        void $ sendHeartbeatRequest client randomSubsciptionId
        threadDelay 500000

    resp1 <- fetchRequest client randomSubsciptionId (fromIntegral requestTimeout) 1
    resp2 <- fetchRequest client randomSubsciptionId (fromIntegral requestTimeout) 1
    let resPayloads1 =  V.map getReceivedRecordPayload resp1
    let resPayloads2 =  V.map getReceivedRecordPayload resp2
    let (first2Req, _) = V.splitAt 2 reqPayloads
    resPayloads1 V.++ resPayloads2 `shouldBe` first2Req
    void $ killThread tid

    threadDelay 2000000
    -- after heartbeat timeout, re-subscribe to origin subscriptionId should success, and
    -- fetch will get from the basic checkpoint
    subscribeRequest client randomSubsciptionId `shouldReturn` True
    resp3 <- fetchRequest client randomSubsciptionId (fromIntegral requestTimeout) 1
    let resPayloads3 = V.head $ V.map getReceivedRecordPayload resp3
    resPayloads3 `shouldBe` V.head reqPayloads

  it "test commitOffset request" $ \(client, reqPayloads) -> do
    putStrLn $ "payloads = " <> show reqPayloads
    tid <- forkIO $ do
      forever $ do
        void $ sendHeartbeatRequest client randomSubsciptionId
        threadDelay 500000

    resp1 <- fetchRequest client randomSubsciptionId (fromIntegral requestTimeout) 1
    resp2 <- fetchRequest client randomSubsciptionId (fromIntegral requestTimeout) 1
    let resPayloads1 =  V.map getReceivedRecordPayload resp1
    let resPayloads2 =  V.map getReceivedRecordPayload resp2
    let (first2Req, remained) = V.splitAt 2 reqPayloads
    putStrLn $ "first2Req = " <> show first2Req <> ", remained: " <> show remained
    resPayloads1 V.++ resPayloads2 `shouldBe` first2Req

    let recordId = fromJust . receivedRecordRecordId $ V.head resp2
    commitOffsetRequest client randomSubsciptionId randomStreamName recordId `shouldReturn` True
    -- commitOffset should not affect the progress of the current reader.
    res <- V.replicateM 2 $ do
      resp <-fetchRequest client randomSubsciptionId (fromIntegral requestTimeout) 1
      return $ getReceivedRecordPayload . V.head $ resp
    putStrLn $ "res = " <> show res
    let (another2Req , _) = V.splitAt 2 remained
    putStrLn $ "another2Req = " <> show another2Req
    res `shouldBe` another2Req

    void $ killThread tid
    threadDelay 2000000
    -- when a new client subscribe the same subscriptionId, it should consume from the checkpoint.
    subscribeRequest client randomSubsciptionId `shouldReturn` True
    resp3 <- fetchRequest client randomSubsciptionId (fromIntegral requestTimeout) 1
    let resPayloads3 = V.head $ V.map getReceivedRecordPayload resp3
    resPayloads3 `shouldBe` V.head remained

----------------------------------------------------------------------------------------------------------

appendRequest :: Client -> TL.Text -> V.Vector HStreamRecord -> IO (Maybe AppendResponse)
appendRequest client streamName records = do
  HStreamApi{..} <- hstreamApiClient client
  let req = AppendRequest streamName records
  resp <- hstreamApiAppend $ ClientNormalRequest req requestTimeout $ MetadataMap Map.empty
  case resp of
    ClientNormalResponse resp' _meta1 _meta2 StatusOk _details -> return $ Just resp'
    ClientErrorResponse clientError                           -> do
      putStrLn ("AppendRequest Error: " <> show clientError) >> return Nothing

fetchRequest :: Client -> TL.Text -> Word64 -> Word32 -> IO (V.Vector ReceivedRecord)
fetchRequest client subscribeId timeout maxSize = do
  HStreamApi{..} <- hstreamApiClient client
  let req = FetchRequest subscribeId timeout maxSize
  resp <- hstreamApiFetch $ ClientNormalRequest req requestTimeout $ MetadataMap Map.empty
  case resp of
    ClientNormalResponse res _meta1 _meta2 StatusOk _details -> do
      return $ fetchResponseReceivedRecords res
    ClientErrorResponse clientError                          -> do
      putStrLn ("FetchRequest Error: " <> show clientError) >> return V.empty

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
requestTimeout = 5

getReceivedRecordPayload :: ReceivedRecord -> B.ByteString
getReceivedRecordPayload ReceivedRecord{..} =
  toByteString . getPayload . decodeByteStringRecord $ receivedRecordRecord
