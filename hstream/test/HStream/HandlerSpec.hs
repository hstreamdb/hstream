{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.HandlerSpec (spec) where

import           Control.Concurrent
import           Control.Concurrent.Async         (race)
import           Control.Monad                    (forM_, replicateM,
                                                   replicateM_, void, when)
import qualified Data.ByteString                  as B
import           Data.Int                         (Int32)
import qualified Data.List                        as L
import qualified Data.Map.Strict                  as Map
import           Data.Maybe                       (fromJust)
import qualified Data.Set                         as Set
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import           Data.Word                        (Word32, Word64)
import           Network.GRPC.HighLevel.Generated
import           Network.GRPC.LowLevel.Call       (clientCallCancel)
import           Proto3.Suite                     (Enumerated (..))
import           Proto3.Suite.Class               (HasDefault (def))
import           System.Random
import           Test.Hspec
import           Z.Foreign                        (toByteString)

import qualified HStream.Logger                   as Log
import           HStream.Server.HStreamApi
import           HStream.SpecUtils
import           HStream.Store.Logger             (pattern C_DBG_ERROR,
                                                   setLogDeviceDbgLevel)
import qualified HStream.ThirdParty.Protobuf      as PB
import           HStream.Utils

spec :: Spec
spec =  describe "HStream.HandlerSpec" $ do
  runIO setupSigsegvHandler
  runIO $ setLogDeviceDbgLevel C_DBG_ERROR

  streamSpec
  subscribeSpec
  consumerSpec

----------------------------------------------------------------------------------------------------------
-- StreamSpec

withRandomStreamName :: ActionWith (HStreamClientApi, TL.Text) -> HStreamClientApi -> IO ()
withRandomStreamName = provideRunTest setup clean
  where
    setup _api = ("StreamSpec_" <>) . TL.fromStrict <$> newRandomText 20
    clean api name = deleteStreamRequest_ api name `shouldReturn` PB.Empty

withRandomStreamNames :: ActionWith (HStreamClientApi, [TL.Text]) -> HStreamClientApi -> IO ()
withRandomStreamNames = provideRunTest setup clean
  where
    setup _api = replicateM 5 $ TL.fromStrict <$> newRandomText 20
    clean api names = forM_ names $ \name -> do
      deleteStreamRequest_ api name `shouldReturn` PB.Empty

streamSpec :: Spec
streamSpec = aroundAll provideHstreamApi $ describe "StreamSpec" $ parallel $ do

  aroundWith withRandomStreamName $ do
    it "test createStream request" $ \(api, name) -> do
      let stream = Stream name 3
      createStreamRequest api stream `shouldReturn` stream
      -- create an existed stream should fail
      createStreamRequest api stream `shouldThrow` anyException

  aroundWith withRandomStreamNames $ do
    it "test listStream request" $ \(api, names) -> do
      let createStreamReqs = zipWith Stream names [1, 2, 3, 3, 2]
      forM_ createStreamReqs $ \stream -> do
        createStreamRequest api stream `shouldReturn` stream

      resp <- listStreamRequest api
      let sortedResp = Set.fromList $ V.toList resp
          sortedReqs = Set.fromList createStreamReqs
      sortedReqs `shouldSatisfy` (`Set.isSubsetOf` sortedResp)

  aroundWith withRandomStreamName $ do
    it "test deleteStream request" $ \(api, name) -> do
      let stream = Stream name 1
      createStreamRequest api stream `shouldReturn` stream
      resp <- listStreamRequest api
      resp `shouldSatisfy` V.elem stream
      deleteStreamRequest api name `shouldReturn` PB.Empty
      resp' <- listStreamRequest api
      resp' `shouldNotSatisfy`  V.elem stream
      -- delete a nonexistent stream without ignoreNonExist set should throw an exception
      deleteStreamRequest api name `shouldThrow` anyException
      -- delete a nonexistent stream with ignoreNonExist set should be okay
      deleteStreamRequest_ api name `shouldReturn` PB.Empty

  aroundWith withRandomStreamName $ do
    it "test append request" $ \(api, name) -> do
      payload1 <- newRandomByteString 5
      payload2 <- newRandomByteString 5
      timeStamp <- getProtoTimestamp
      let stream = Stream name 1
          header = buildRecordHeader HStreamRecordHeader_FlagRAW Map.empty timeStamp TL.empty
          record1 = buildRecord header payload1
          record2 = buildRecord header payload2
      -- append to a nonexistent stream should throw exception
      appendRequest api name (V.fromList [record1, record2]) `shouldThrow` anyException
      createStreamRequest api stream `shouldReturn` stream
      -- FIXME: Even we have called the "syncLogsConfigVersion" method, there is
      -- **no** guarantee that subsequent "append" will have an up-to-date view
      -- of the LogsConfig. For details, see Logdevice::Client::syncLogsConfigVersion
      threadDelay 2000000
      resp <- appendRequest api name (V.fromList [record1, record2])
      appendResponseStreamName resp `shouldBe` name
      recordIdBatchIndex <$> appendResponseRecordIds resp `shouldBe` V.fromList [0, 1]

-------------------------------------------------------------------------------------------------

createStreamRequest :: HStreamClientApi -> Stream -> IO Stream
createStreamRequest HStreamApi{..} stream =
  let req = ClientNormalRequest stream requestTimeout $ MetadataMap Map.empty
  in getServerResp =<< hstreamApiCreateStream req

listStreamRequest :: HStreamClientApi -> IO (V.Vector Stream)
listStreamRequest HStreamApi{..} =
  let req = ClientNormalRequest ListStreamsRequest requestTimeout $ MetadataMap Map.empty
  in listStreamsResponseStreams <$> (getServerResp =<< hstreamApiListStreams req)

deleteStreamRequest :: HStreamClientApi -> TL.Text -> IO PB.Empty
deleteStreamRequest HStreamApi{..} streamName =
  let delReq = def { deleteStreamRequestStreamName = streamName }
      req = ClientNormalRequest delReq requestTimeout $ MetadataMap Map.empty
  in getServerResp =<< hstreamApiDeleteStream req

-- This request is mainly used for cleaning up after testing
deleteStreamRequest_ :: HStreamClientApi -> TL.Text -> IO PB.Empty
deleteStreamRequest_ HStreamApi{..} streamName =
  let delReq = def { deleteStreamRequestStreamName = streamName
                   , deleteStreamRequestIgnoreNonExist = True }
      req = ClientNormalRequest delReq requestTimeout $ MetadataMap Map.empty
  in getServerResp =<< hstreamApiDeleteStream req

appendRequest :: HStreamClientApi -> TL.Text -> V.Vector HStreamRecord -> IO AppendResponse
appendRequest HStreamApi{..} streamName records =
  let appReq = AppendRequest streamName records
      req = ClientNormalRequest appReq requestTimeout $ MetadataMap Map.empty
  in getServerResp =<< hstreamApiAppend req

----------------------------------------------------------------------------------------------------------
-- SubscribeSpec

withSubscription :: ActionWith (HStreamClientApi, (TL.Text, TL.Text)) -> HStreamClientApi -> IO ()
withSubscription = provideRunTest setup clean
  where
    setup _api = do
      stream <- TL.fromStrict <$> newRandomText 5
      subscription <- TL.fromStrict <$> newRandomText 5
      return ("StreamSpec_" <> stream, "SubscriptionSpec_" <> subscription)
    clean api (streamName, subscriptionName) = do
      deleteSubscriptionRequest api subscriptionName `shouldReturn` True
      deleteStreamRequest_ api streamName `shouldReturn` PB.Empty

withSubscriptions :: ActionWith (HStreamClientApi, (V.Vector TL.Text, V.Vector TL.Text))
                  -> HStreamClientApi -> IO ()
withSubscriptions = provideRunTest setup clean
  where
    setup _api = do
      stream <- V.replicateM 5 $ TL.fromStrict <$> newRandomText 5
      subscription <- V.replicateM 5 $ TL.fromStrict <$> newRandomText 5
      return (("StreamSpec_" <>) <$> stream, ("SubscriptionSpec_" <>) <$> subscription)
    clean api (streamNames, subscriptionNames) = do
      forM_ streamNames $ \name -> do
        deleteStreamRequest_ api name `shouldReturn` PB.Empty
      forM_ subscriptionNames $ \name -> do
        deleteSubscriptionRequest api name `shouldReturn` True

subscribeSpec :: Spec
subscribeSpec = aroundAll provideHstreamApi $
  describe "SubscribeSpec" $ parallel $ do

  let offset = SubscriptionOffset . Just . SubscriptionOffsetOffsetSpecialOffset
               . Enumerated . Right $ SubscriptionOffset_SpecialOffsetLATEST

  aroundWith withSubscription $ do
    it "test createSubscribe request" $ \(api, (streamName, subscriptionName)) -> do
      -- createSubscribe with a nonexistent stream should throw an exception
      createSubscriptionRequest api subscriptionName streamName offset `shouldThrow` anyException
      let stream = Stream streamName 1
      createStreamRequest api stream `shouldReturn` stream
      -- createSubscribe with an existing stream should return True
      createSubscriptionRequest api subscriptionName streamName offset `shouldReturn` True
      -- createSubscribe fails if the subscriptionName has been used
      createSubscriptionRequest api subscriptionName streamName offset `shouldThrow` anyException

  aroundWith withSubscription $ do
    it "test subscribe request" $ \(api, (streamName, subscriptionName)) -> do
      let stream = Stream streamName 1
      let subscriptionName' = subscriptionName <> "___"
      createStreamRequest api stream `shouldReturn` stream
      createSubscriptionRequest api subscriptionName streamName offset `shouldReturn` True
      -- subscribe a nonexistent subscriptionId should throw exception
      subscribeRequest api subscriptionName' `shouldThrow` anyException
      -- subscribe an existing subscriptionId should return True
      subscribeRequest api subscriptionName `shouldReturn` True
      -- re-subscribe is okay
      subscribeRequest api subscriptionName `shouldReturn` True
      -- subscribe is okay even though the stream has been deleted
      deleteStreamRequest api streamName `shouldReturn` PB.Empty
      subscribeRequest api subscriptionName `shouldReturn` True

  aroundWith withSubscriptions $ do
    it "test listSubscription request" $ \(api, (streamNames, subscriptionNames)) -> do
      let subscriptions = V.zipWith4 Subscription  subscriptionNames streamNames  (V.replicate 5 (Just offset)) (V.replicate 5 30)
      forM_ subscriptions $ \Subscription{..} -> do
        let stream = Stream subscriptionStreamName 1
        createStreamRequest api stream `shouldReturn` stream
        createSubscriptionRequest api subscriptionSubscriptionId subscriptionStreamName
          (fromJust subscriptionOffset) `shouldReturn` True
        subscribeRequest api subscriptionSubscriptionId `shouldReturn` True
      resp <- listSubscriptionRequest api
      let respSet = Set.fromList $ subscriptionSubscriptionId <$> V.toList resp
          reqsSet = Set.fromList $ subscriptionSubscriptionId <$> V.toList subscriptions
      reqsSet `shouldSatisfy` (`Set.isSubsetOf` respSet)

  aroundWith withSubscription $ do
    it "test deleteSubscription request" $ \(api, (streamName, subscriptionName)) -> do
      let stream = Stream streamName 1
      createStreamRequest api stream `shouldReturn` stream
      createSubscriptionRequest api subscriptionName streamName offset `shouldReturn` True
      subscribeRequest api subscriptionName `shouldReturn` True
      -- delete a subscribed stream should return true
      deleteSubscriptionRequest api subscriptionName `shouldReturn` True
      -- double deletion is okay
      deleteSubscriptionRequest api subscriptionName `shouldReturn` True

  aroundWith withSubscription $ do
    it "deleteSubscription request with removed stream should success" $ \(api, (streamName, subscriptionName)) -> do
      let stream = Stream streamName 1
      createStreamRequest api stream `shouldReturn` stream
      createSubscriptionRequest api subscriptionName streamName offset `shouldReturn` True
      subscribeRequest api subscriptionName `shouldReturn` True
      -- delete a subscription with underlying stream deleted should success
      deleteStreamRequest api streamName `shouldReturn` PB.Empty
      deleteSubscriptionRequest api subscriptionName `shouldReturn` True

  aroundWith withSubscription $ do
    it "test hasSubscription request" $ \(api, (streamName, subscriptionName)) -> do
      void $ createStreamRequest api $ Stream streamName 1
      -- check a nonexistent subscriptionId should return False
      checkSubscriptionExistRequest api subscriptionName `shouldReturn` False
      -- check an existing subscriptionId should return True
      createSubscriptionRequest api subscriptionName streamName offset `shouldReturn` True
      checkSubscriptionExistRequest api subscriptionName `shouldReturn` True

----------------------------------------------------------------------------------------------------------

createSubscriptionRequest :: HStreamClientApi -> TL.Text -> TL.Text -> SubscriptionOffset -> IO Bool
createSubscriptionRequest HStreamApi{..} subscriptionId streamName offset =
  let subscription = Subscription subscriptionId streamName (Just offset) streamingAckTimeout
      req = ClientNormalRequest subscription requestTimeout $ MetadataMap Map.empty
  in True <$ (getServerResp =<< hstreamApiCreateSubscription req)

subscribeRequest :: HStreamClientApi -> TL.Text -> IO Bool
subscribeRequest HStreamApi{..} subscribeId =
  let subReq = SubscribeRequest subscribeId
      req = ClientNormalRequest subReq requestTimeout $ MetadataMap Map.empty
  in True <$ (getServerResp =<< hstreamApiSubscribe req)

listSubscriptionRequest :: HStreamClientApi -> IO (V.Vector Subscription)
listSubscriptionRequest HStreamApi{..} =
  let req = ClientNormalRequest ListSubscriptionsRequest requestTimeout $ MetadataMap Map.empty
  in listSubscriptionsResponseSubscription <$> (getServerResp =<< hstreamApiListSubscriptions req)

deleteSubscriptionRequest :: HStreamClientApi -> TL.Text -> IO Bool
deleteSubscriptionRequest HStreamApi{..} subscribeId =
  let delReq = DeleteSubscriptionRequest subscribeId
      req = ClientNormalRequest delReq requestTimeout $ MetadataMap Map.empty
  in True <$ (getServerResp =<< hstreamApiDeleteSubscription req)

checkSubscriptionExistRequest :: HStreamClientApi -> TL.Text -> IO Bool
checkSubscriptionExistRequest HStreamApi{..} subscribeId =
  let checkReq = CheckSubscriptionExistRequest subscribeId
      req = ClientNormalRequest checkReq requestTimeout $ MetadataMap Map.empty
  in checkSubscriptionExistResponseExists <$> (getServerResp =<< hstreamApiCheckSubscriptionExist req)

----------------------------------------------------------------------------------------------------------
-- ConsumerSpec

withConsumerSpecEnv :: ActionWith (HStreamClientApi, (TL.Text, TL.Text))
                    -> HStreamClientApi -> IO ()
withConsumerSpecEnv = provideRunTest setup clean
  where
    setup api = do
      streamName <- ("ConsumerSpec_" <>) . TL.fromStrict <$> newRandomText 20
      subName <- ("ConsumerSpec_" <>) . TL.fromStrict <$> newRandomText 20

      let offset = SubscriptionOffset . Just . SubscriptionOffsetOffsetSpecialOffset
                   . Enumerated . Right $ SubscriptionOffset_SpecialOffsetLATEST
      let stream = Stream streamName 1
      createStreamRequest api stream `shouldReturn` stream
      createSubscriptionRequest api subName streamName offset `shouldReturn` True
      return (streamName, subName)

    clean api (streamName, subName) = do
      deleteSubscriptionRequest api subName `shouldReturn` True
      deleteStreamRequest_ api streamName `shouldReturn` PB.Empty

consumerSpec :: Spec
consumerSpec = aroundAll provideHstreamApi $ describe "ConsumerSpec" $ do

  aroundWith withConsumerSpecEnv $ do

    timeStamp <- runIO getProtoTimestamp
    let header = buildRecordHeader HStreamRecordHeader_FlagRAW Map.empty timeStamp TL.empty

    it "test streamFetch request" $ \(api, (streamName, subName)) -> do
      originCh <- newChan
      hackerCh <- newChan
      terminate <- newChan
      void $ forkIO (streamFetchRequest api subName originCh hackerCh terminate defaultHacker)

      let numMsg = 300
          batchSize = 5
      reqRids <- replicateM numMsg $ do
        payload <- V.map (buildRecord header) <$> V.replicateM batchSize (newRandomByteString 2)
        appendResponseRecordIds <$> appendRequest api streamName payload
      Log.debug . Log.buildString $ "length of record = " <> show (length reqRids)
      ack <- collectRecord 0 (numMsg * batchSize) [] originCh
      ack `shouldBe` V.concat reqRids
      writeChan terminate ()
      Log.debug "streamFetch test done !!!!!!!!!!!"

    it "test retrans unacked msg" $ \(api, (streamName, subName)) -> do
      originCh <- newChan
      hackerCh <- newChan
      terminate <- newChan
      void $ forkIO (streamFetchRequest api subName originCh hackerCh terminate randomKillHacker)

      replicateM_ 1000 $ do
        payload <- V.map (buildRecord header) <$> V.replicateM 2 (newRandomByteString 2)
        appendResponseRecordIds <$> appendRequest api streamName payload
      originAck <- readChan originCh
      hackedAck <- readChan hackerCh
      retransAck <- collectRetrans 0 originCh (V.length originAck - V.length hackedAck) []
      Log.debug . Log.buildString $ "retransAck length = " <> show (length retransAck)
      let diff = V.toList originAck L.\\ V.toList hackedAck
      retransAck `shouldBe` V.fromList diff
      writeChan terminate ()
      Log.debug "retrans unacked msg test done !!!!!!!!!!!"

    it "test retrans timeout msg" $ \(api, (streamName, subName)) -> do
      originCh <- newChan
      hackerCh <- newChan
      terminate <- newChan
      void $ forkIO (streamFetchRequest api subName originCh hackerCh terminate timeoutHacker)

      replicateM_ 500 $ do
        payload <- V.map (buildRecord header) <$> V.replicateM 5 (newRandomByteString 2)
        appendResponseRecordIds <$> appendRequest api streamName payload
      originAck <- readChan originCh
      let hackedAck = V.empty
      retransAck <- collectRetrans 0 originCh (V.length originAck - V.length hackedAck) []
      Log.debug . Log.buildString $ "retransAck length = " <> show (length retransAck)
      retransAck `shouldBe` originAck
      writeChan terminate ()
      Log.debug "retrans timeout msg done !!!!!!!!!!!"

    -- FIXME:Wrong ack messages cause the server to fail to update the ack window and commit checkpoints,
    -- but the only way to verify the server's behavior now is check the debug logs, so this test is always successful.
    it "test ack wrong msg" $ \(api, (streamName, subName)) -> do
      originCh <- newChan
      hackerCh <- newChan
      terminate <- newChan

      let record = buildRecord header "1"
      RecordId{..} <- V.head . appendResponseRecordIds <$> appendRequest api streamName (V.singleton record)
      let !latesLSN = recordIdBatchId + 1
      void $ forkIO (streamFetchRequest api subName originCh hackerCh terminate (wrongRidHacker latesLSN))

      replicateM_ 5 $ do
        payload <- V.map (buildRecord header) <$> V.replicateM 2 (newRandomByteString 2)
        appendResponseRecordIds <$> appendRequest api streamName payload
      originAck <- readChan originCh
      let hackedAck = V.empty
      retransAck <- collectRetrans 0 originCh (V.length originAck - V.length hackedAck) []
      Log.debug . Log.buildString $ "retransAck length = " <> show (length retransAck)
      retransAck `shouldBe` originAck
      writeChan terminate ()
      Log.debug "ack wrong msg test done !!!!!!!!!!!"

  -- TODO:
  -- test need to add
  --  1. fetch/ack unsubscribed subscription will fail
  --  2. validate commit and checkpoint

----------------------------------------------------------------------------------------------------------

fetchRequest :: HStreamClientApi -> TL.Text -> Word64 -> Word32 -> IO (V.Vector ReceivedRecord)
fetchRequest HStreamApi{..} subscribeId timeout maxSize =
  let fetReq = FetchRequest subscribeId timeout maxSize
      req = ClientNormalRequest fetReq requestTimeout $ MetadataMap Map.empty
  in fetchResponseReceivedRecords <$> (getServerResp =<< hstreamApiFetch req)

streamFetchRequest
  :: HStreamClientApi
  -> TL.Text
  -> Chan (V.Vector RecordId) -- channel use to trans record recevied from server
  -> Chan (V.Vector RecordId) -- channel use to trans record after hacker
  -> Chan ()                  -- channel use to close client request
  -> (V.Vector RecordId -> IO (V.Vector RecordId)) -- hacker function
  -> IO ()
streamFetchRequest HStreamApi{..} subscribeId originCh hackerCh terminate hacker = do
  let req = ClientBiDiRequest streamingReqTimeout (MetadataMap Map.empty) (action True)
  hstreamApiStreamingFetch req >>= \case
    ClientBiDiResponse _meta StatusCancelled detail -> Log.info . Log.buildString $ "request cancel" <> show detail
    ClientBiDiResponse _meta StatusOk _msg -> Log.debug "fetch request done"
    ClientBiDiResponse _meta stats detail -> Log.fatal . Log.buildString $ "abnormal status: " <> show stats <> ", msg: " <> show detail
    ClientErrorResponse err -> Log.e . Log.buildString $ "fetch request err: " <> show err
  where
    action isInit call _meta streamRecv streamSend _done = do
      when isInit $ do
        let initReq = StreamingFetchRequest subscribeId V.empty
        streamSend initReq >>= \case
          Left err -> do
            Log.e . Log.buildString $ "Server error happened when send init streamFetchRequest err: " <> show err
            clientCallCancel call
          Right _ -> return ()
      void $ race (doFetch streamRecv streamSend hacker call) (notifyDone call)

    doFetch streamRecv streamSend hacker' call = do
      streamRecv >>= \case
        Left err -> do
          Log.e . Log.buildString $ "Error happened when recv from server " <> show err
          clientCallCancel call
        Right Nothing -> do
          Log.debug "server close, fetch request end."
          clientCallCancel call
        Right (Just StreamingFetchResponse{..}) -> do
          -- get recordId from response
          let ackIds = V.map (fromJust . receivedRecordRecordId) streamingFetchResponseReceivedRecords
          -- select ackIds
          ackIds' <- hacker' ackIds
          writeChan originCh ackIds
          writeChan hackerCh ackIds'

          -- send ack
          let fetReq = StreamingFetchRequest subscribeId ackIds'
          streamSend fetReq >>= \case
            Left err -> do
              Log.e . Log.buildString $ "Error happened when send ack: " <> show err <> "\n ack context: " <> show fetReq
              clientCallCancel call
            Right _ -> do
              doFetch streamRecv streamSend defaultHacker call

    notifyDone call = do
      void $ readChan terminate
      clientCallCancel call
      Log.d "client cancel fetch request"

collectRetrans :: Int -> Chan (V.Vector RecordId) -> Int -> [V.Vector RecordId]-> IO (V.Vector RecordId)
collectRetrans cnt channel maxCount res
  | maxCount <= cnt = return $ V.concat res
  | otherwise = do
      ids <- readChan channel
      if V.length ids /= 1
         then do
           collectRetrans cnt channel maxCount res
         else do
           let nres = res L.++ [ids]
           collectRetrans (cnt + 1) channel maxCount nres

collectRecord :: Int -> Int -> [V.Vector RecordId] -> Chan (V.Vector RecordId) -> IO (V.Vector RecordId)
collectRecord cnt maxCount res channel
  | cnt >= maxCount = do
      return . V.concat $ res
  | otherwise = do
      rids <- readChan channel
      let nres = res L.++ [rids]
      collectRecord (cnt + V.length rids) maxCount nres channel

defaultHacker :: V.Vector RecordId -> IO (V.Vector RecordId)
defaultHacker = pure

randomKillHacker :: V.Vector RecordId -> IO (V.Vector RecordId)
randomKillHacker rids = do
  let size = V.length rids
  seed <- randomRIO (1, min 10 (size `div` 2))
  let retrans = V.ifilter (\idx _ -> idx `mod` seed == 0) rids
  Log.d $ "length of recordIds which need to retrans: " <> Log.buildInt (V.length retrans)
  return $ V.ifilter (\idx _ -> idx `mod` seed /= 0) rids

timeoutHacker :: V.Vector RecordId -> IO (V.Vector RecordId)
timeoutHacker rids = threadDelay 7000000 >> return rids

wrongRidHacker :: Word64 -> V.Vector RecordId -> IO (V.Vector RecordId)
wrongRidHacker lsn _ = do
  let wrongIdxRecord = RecordId lsn 100005
      wrongLSNRecord = RecordId (lsn + 50000) 99
  return . V.fromList $ [wrongLSNRecord, wrongIdxRecord]

requestTimeout :: Int
requestTimeout = 10

streamingAckTimeout :: Int32
streamingAckTimeout = 3

streamingReqTimeout :: Int
streamingReqTimeout = 10000000

getReceivedRecordPayload :: ReceivedRecord -> B.ByteString
getReceivedRecordPayload ReceivedRecord{..} =
  toByteString . getPayload . decodeByteStringRecord $ receivedRecordRecord
