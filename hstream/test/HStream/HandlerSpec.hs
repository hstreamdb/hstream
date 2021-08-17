{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.HandlerSpec (spec) where

import           Control.Concurrent               (forkIO, killThread,
                                                   threadDelay)
import           Control.Monad                    (forM_, forever, replicateM,
                                                   unless, void, when)
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

import qualified HStream.Logger                   as Log
import           HStream.Server.HStreamApi
import           HStream.SpecUtils
import           HStream.Store.Logger             (pattern C_DBG_ERROR,
                                                   setLogDeviceDbgLevel)
import qualified HStream.ThirdParty.Protobuf      as PB
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

  streamSpec

  aroundAll (\runTest -> do
             withGRPCClient clientConfig $ \client -> do
               runTest client
            ) $ describe "HStream.BasicHandlerSpec" $ do
   subscribeSpec
   consumerSpec

withRandomStreamName :: ActionWith (HStreamClientApi, TL.Text) -> HStreamClientApi -> IO ()
withRandomStreamName = provideRunTest setup clean
  where
    setup _api = TL.fromStrict <$> newRandomText 20
    clean HStreamApi{..} name = do
      let req = def { deleteStreamRequestStreamName = name }
      hstreamApiDeleteStream (ClientNormalRequest req requestTimeout $ MetadataMap Map.empty)
        `grpcShouldReturn` PB.Empty

withRandomStreamNames :: ActionWith (HStreamClientApi, [TL.Text]) -> HStreamClientApi -> IO ()
withRandomStreamNames = provideRunTest setup clean
  where
    setup _api = replicateM 5 $ TL.fromStrict <$> newRandomText 20
    clean HStreamApi{..} names = forM_ names $ \name -> do
      let req = def { deleteStreamRequestStreamName = name }
      hstreamApiDeleteStream (ClientNormalRequest req requestTimeout $ MetadataMap Map.empty)
        `grpcShouldReturn` PB.Empty

----------------------------------------------------------------------------------------------------------
-- StreamSpec

streamSpec :: Spec
streamSpec = aroundAll provideHstreamApi $ describe "StreamSpec" $ parallel $ do

  aroundWith withRandomStreamName $ do
    it "test CreateStream request" $ \(HStreamApi{..}, name) -> do
      let stream = Stream name 3
      hstreamApiCreateStream (ClientNormalRequest stream requestTimeout $ MetadataMap Map.empty)
        `grpcShouldReturn` stream
      -- create an existed stream should fail
      hstreamApiCreateStream (ClientNormalRequest stream requestTimeout $ MetadataMap Map.empty)
        `grpcShouldThrow` anyException

  aroundWith withRandomStreamNames $ do
    it "test listStream request" $ \(HStreamApi{..}, names) -> do
      let createStreamReqs = zipWith Stream names [1, 2, 3, 3, 2]
      forM_ createStreamReqs $ \stream -> do
        hstreamApiCreateStream (ClientNormalRequest stream requestTimeout $ MetadataMap Map.empty)
          `grpcShouldReturn` stream

      resp <- getServerResp =<< hstreamApiListStreams (ClientNormalRequest ListStreamsRequest requestTimeout $ MetadataMap Map.empty)
      let streamsResp = listStreamsResponseStreams resp
      let sortedResp = Set.fromList $ V.toList streamsResp
          sortedReqs = Set.fromList createStreamReqs
      sortedReqs `shouldSatisfy` (`Set.isSubsetOf` sortedResp)

  it "test deleteStream request" $ \HStreamApi{..} -> do
    name <- TL.fromStrict <$> newRandomText 20
    let stream = Stream name 1

    hstreamApiCreateStream (ClientNormalRequest stream requestTimeout $ MetadataMap Map.empty)
      `grpcShouldReturn` stream

    resp <- getServerResp =<< hstreamApiListStreams (ClientNormalRequest ListStreamsRequest requestTimeout $ MetadataMap Map.empty)
    listStreamsResponseStreams resp `shouldSatisfy` V.elem stream

    let req = def { deleteStreamRequestStreamName = name }
    hstreamApiDeleteStream (ClientNormalRequest req requestTimeout $ MetadataMap Map.empty)
      `grpcShouldReturn` PB.Empty

    resp' <- getServerResp =<< hstreamApiListStreams (ClientNormalRequest ListStreamsRequest requestTimeout $ MetadataMap Map.empty)
    listStreamsResponseStreams resp' `shouldNotSatisfy` V.elem stream

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

cleanStream :: TL.Text -> Client -> IO ()
cleanStream name client = void $ deleteStreamRequest client name

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
        -- subscribe an existed subscriptionId should return True
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
    void $ createStreamRequest client $ Stream randomStreamName 1
    void $ createSubscriptionRequest client randomSubsciptionId randomStreamName offset
    subscribeRequest client randomSubsciptionId `shouldReturn` True
    -- delete subscribed stream should return true
    deleteSubscriptionRequest client randomSubsciptionId `shouldReturn` True

  after (cleanSubscriptionEnv randomSubsciptionId randomStreamName) $ it "delete a subscription with underlying stream deleted should success" $ \client -> do
    void $ createStreamRequest client $ Stream randomStreamName 1
    void $ createSubscriptionRequest client randomSubsciptionId randomStreamName offset
    subscribeRequest client randomSubsciptionId `shouldReturn` True
    deleteStreamRequest client randomStreamName `shouldReturn` True
    deleteSubscriptionRequest client randomSubsciptionId `shouldReturn` True
    res <- fromJust <$> listSubscriptionRequest client
    V.find (\Subscription{..} -> subscriptionSubscriptionId == randomSubsciptionId) res `shouldBe` Nothing

  after (cleanStream randomStreamName) $ it "test hasSubscription request" $ \client -> do
    void $ createStreamRequest client $ Stream randomStreamName 1
    -- check a nonexistent subscriptionId should return False
    checkSubscriptionExistRequest client randomSubsciptionId `shouldReturn` False
    -- check an existed subscriptionId should return True
    createSubscriptionRequest client randomSubsciptionId randomStreamName offset `shouldReturn` True
    checkSubscriptionExistRequest client randomSubsciptionId `shouldReturn` True

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
    Log.debug $ Log.buildString "reqPayloads = " <> Log.buildString (show reqPayloads)
    resp <- fetchRequest client randomSubsciptionId (fromIntegral requestTimeout) 100
    let resPayloads = V.map getReceivedRecordPayload resp
    Log.debug $ Log.buildString "respPayload = " <> Log.buildString (show resPayloads)
    resPayloads `shouldBe` reqPayloads
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

commitOffsetRequest :: Client -> TL.Text -> RecordId -> IO Bool
commitOffsetRequest client subscriptionId recordId = do
  HStreamApi{..} <- hstreamApiClient client
  let req = CommittedOffset subscriptionId $ Just recordId
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
