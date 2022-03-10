{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.HandlerSpec (spec) where

import           Control.Concurrent
import           Control.Concurrent.Async         (race)
import           Control.Monad                    (forM, forM_, replicateM,
                                                   void, when)
import           Data.IORef
import           Data.Int                         (Int32)
import qualified Data.List                        as L
import qualified Data.Map.Strict                  as Map
import           Data.Maybe                       (fromJust)
import           Data.Set                         (Set)
import qualified Data.Set                         as Set
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Data.Word                        (Word64)
import           Network.GRPC.HighLevel.Generated
import           Network.GRPC.LowLevel.Call       (clientCallCancel)
import           Proto3.Suite                     (Enumerated (..))
import           Proto3.Suite.Class               (HasDefault (def))
import           System.Random
import           System.Timeout
import           Test.Hspec

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
  -- subscribeSpec
  -- consumerSpec
  -- consumerGroupSpec

----------------------------------------------------------------------------------------------------------
-- StreamSpec

streamSpec :: Spec
streamSpec = aroundAll provideHstreamApi $ describe "StreamSpec" $ parallel $ do

  aroundWith withRandomStreamName $ do
    it "test createStream request" $ \(api, name) -> do
      let stream = Stream name 3
      createStreamRequest api stream `shouldReturn` stream
      -- create an existed stream should fail
      createStreamRequest api stream `shouldThrow` anyException

  aroundWith (withRandomStreamNames 5) $ do
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
      cleanStreamReq api name `shouldReturn` PB.Empty

  aroundWith withRandomStreamName $ do
    it "test append request" $ \(api, name) -> do
      payload1 <- newRandomByteString 5
      payload2 <- newRandomByteString 5
      timeStamp <- getProtoTimestamp
      let stream = Stream name 1
          header  = buildRecordHeader HStreamRecordHeader_FlagRAW Map.empty timeStamp T.empty
          record1 = buildRecord header payload1
          record2 = buildRecord header payload2
      -- append to a nonexistent stream should throw exception
      appendRequest api name (V.fromList [record1, record2]) `shouldThrow` anyException
      createStreamRequest api stream `shouldReturn` stream
      -- FIXME: Even we have called the "syncLogsConfigVersion" method, there is
      -- __no__ guarantee that subsequent "append" will have an up-to-date view
      -- of the LogsConfig. For details, see Logdevice::Client::syncLogsConfigVersion
      threadDelay 2000000
      resp <- appendRequest api name (V.fromList [record1, record2])
      appendResponseStreamName resp `shouldBe` name
      recordIdBatchIndex <$> appendResponseRecordIds resp `shouldBe` V.fromList [0, 1]
      batchPayload <- readBatchPayload name
      fmap (hstreamRecordPayload . decodeByteStringRecord) batchPayload `shouldBe` V.fromList [payload1, payload2]

-------------------------------------------------------------------------------------------------

createStreamRequest :: HStreamClientApi -> Stream -> IO Stream
createStreamRequest HStreamApi{..} stream =
  let req = ClientNormalRequest stream requestTimeout $ MetadataMap Map.empty
  in getServerResp =<< hstreamApiCreateStream req

listStreamRequest :: HStreamClientApi -> IO (V.Vector Stream)
listStreamRequest HStreamApi{..} =
  let req = ClientNormalRequest ListStreamsRequest requestTimeout $ MetadataMap Map.empty
  in listStreamsResponseStreams <$> (getServerResp =<< hstreamApiListStreams req)

deleteStreamRequest :: HStreamClientApi -> T.Text -> IO PB.Empty
deleteStreamRequest HStreamApi{..} streamName =
  let delReq = def { deleteStreamRequestStreamName = streamName }
      req = ClientNormalRequest delReq requestTimeout $ MetadataMap Map.empty
  in getServerResp =<< hstreamApiDeleteStream req

----------------------------------------------------------------------------------------------------------
-- SubscribeSpec

---- withSubscription :: ActionWith (HStreamClientApi, (T.Text, T.Text)) -> HStreamClientApi -> IO ()
---- withSubscription = provideRunTest setup clean
----   where
----     setup _api = do
----       stream       <- newRandomText 5
----       subscription <- newRandomText 5
----       return ("StreamSpec_" <> stream, "SubscriptionSpec_" <> subscription)
----     clean api (streamName, subscriptionName) = do
----       deleteSubscriptionRequest api subscriptionName `shouldReturn` True
----       cleanStreamReq api streamName `shouldReturn` PB.Empty
----
---- withSubscriptions :: ActionWith (HStreamClientApi, (V.Vector T.Text, V.Vector T.Text))
----                   -> HStreamClientApi -> IO ()
---- withSubscriptions = provideRunTest setup clean
----   where
----     setup _api = do
----       stream       <- V.replicateM 5 $ newRandomText 5
----       subscription <- V.replicateM 5 $ newRandomText 5
----       return (("StreamSpec_" <>) <$> stream, ("SubscriptionSpec_" <>) <$> subscription)
----     clean api (streamNames, subscriptionNames) = do
----       forM_ streamNames $ \name -> do
----         cleanStreamReq api name `shouldReturn` PB.Empty
----       forM_ subscriptionNames $ \name -> do
----         deleteSubscriptionRequest api name `shouldReturn` True
----
---- subscribeSpec :: Spec
---- subscribeSpec = aroundAll provideHstreamApi $
----   xdescribe "SubscribeSpec" $ parallel $ do
----
----   let offset = SubscriptionOffset . Just . SubscriptionOffsetOffsetSpecialOffset
----                . Enumerated . Right $ SubscriptionOffset_SpecialOffsetLATEST
----
----   aroundWith withSubscription $ do
----     it "test createSubscribe request" $ \(api, (streamName, subscriptionName)) -> do
----       -- createSubscribe with a nonexistent stream should throw an exception
----       createSubscriptionRequest api subscriptionName streamName offset `shouldThrow` anyException
----       let stream = Stream streamName 1
----       createStreamRequest api stream `shouldReturn` stream
----       -- createSubscribe with an existing stream should return True
----       createSubscriptionRequest api subscriptionName streamName offset `shouldReturn` True
----       -- createSubscribe fails if the subscriptionName has been used
----       createSubscriptionRequest api subscriptionName streamName offset `shouldThrow` anyException
----
----   aroundWith withSubscriptions $ do
----     it "test listSubscription request" $ \(api, (streamNames, subscriptionNames)) -> do
----       let subscriptions = V.zipWith4 Subscription  subscriptionNames streamNames  (V.replicate 5 (Just offset)) (V.replicate 5 30)
----       forM_ subscriptions $ \Subscription{..} -> do
----         let stream = Stream subscriptionStreamName 1
----         createStreamRequest api stream `shouldReturn` stream
----         createSubscriptionRequest api subscriptionSubscriptionId subscriptionStreamName
----           (fromJust subscriptionOffset) `shouldReturn` True
----       resp <- listSubscriptionRequest api
----       let respSet = Set.fromList $ subscriptionSubscriptionId <$> V.toList resp
----           reqsSet = Set.fromList $ subscriptionSubscriptionId <$> V.toList subscriptions
----       reqsSet `shouldSatisfy` (`Set.isSubsetOf` respSet)
----
----   aroundWith withSubscription $ do
----     it "test deleteSubscription request" $ \(api, (streamName, subscriptionName)) -> do
----       let stream = Stream streamName 1
----       createStreamRequest api stream `shouldReturn` stream
----       createSubscriptionRequest api subscriptionName streamName offset `shouldReturn` True
----       -- delete a subscribed stream should return true
----       deleteSubscriptionRequest api subscriptionName `shouldReturn` True
----       -- double deletion is okay
----       deleteSubscriptionRequest api subscriptionName `shouldReturn` True
----
----   aroundWith withSubscription $ do
----     it "deleteSubscription request with removed stream should success" $ \(api, (streamName, subscriptionName)) -> do
----       let stream = Stream streamName 1
----       createStreamRequest api stream `shouldReturn` stream
----       createSubscriptionRequest api subscriptionName streamName offset `shouldReturn` True
----       -- delete a subscription with underlying stream deleted should success
----       deleteStreamRequest api streamName `shouldReturn` PB.Empty
----       deleteSubscriptionRequest api subscriptionName `shouldReturn` True
----
----   aroundWith withSubscription $ do
----     it "test hasSubscription request" $ \(api, (streamName, subscriptionName)) -> do
----       void $ createStreamRequest api $ Stream streamName 1
----       -- check a nonexistent subscriptionId should return False
----       checkSubscriptionExistRequest api subscriptionName `shouldReturn` False
----       -- check an existing subscriptionId should return True
----       createSubscriptionRequest api subscriptionName streamName offset `shouldReturn` True
----       checkSubscriptionExistRequest api subscriptionName `shouldReturn` True
----
---- ----------------------------------------------------------------------------------------------------------
----
---- createSubscriptionRequest :: HStreamClientApi -> T.Text -> T.Text -> SubscriptionOffset -> IO Bool
---- createSubscriptionRequest HStreamApi{..} subscriptionId streamName offset =
----   let subscription = Subscription subscriptionId streamName (Just offset) streamingAckTimeout
----       req = ClientNormalRequest subscription requestTimeout $ MetadataMap Map.empty
----   in True <$ (getServerResp =<< hstreamApiCreateSubscription req)
----
---- listSubscriptionRequest :: HStreamClientApi -> IO (V.Vector Subscription)
---- listSubscriptionRequest HStreamApi{..} =
----   let req = ClientNormalRequest ListSubscriptionsRequest requestTimeout $ MetadataMap Map.empty
----   in listSubscriptionsResponseSubscription <$> (getServerResp =<< hstreamApiListSubscriptions req)
----
---- deleteSubscriptionRequest :: HStreamClientApi -> T.Text -> IO Bool
---- deleteSubscriptionRequest HStreamApi{..} subscribeId =
----   let delReq = DeleteSubscriptionRequest subscribeId
----       req = ClientNormalRequest delReq requestTimeout $ MetadataMap Map.empty
----   in True <$ (getServerResp =<< hstreamApiDeleteSubscription req)
----
---- checkSubscriptionExistRequest :: HStreamClientApi -> T.Text -> IO Bool
---- checkSubscriptionExistRequest HStreamApi{..} subscribeId =
----   let checkReq = CheckSubscriptionExistRequest subscribeId
----       req = ClientNormalRequest checkReq requestTimeout $ MetadataMap Map.empty
----   in checkSubscriptionExistResponseExists <$> (getServerResp =<< hstreamApiCheckSubscriptionExist req)
----
---- ----------------------------------------------------------------------------------------------------------
---- -- ConsumerSpec
----
---- withConsumerSpecEnv :: ActionWith (HStreamClientApi, (T.Text, T.Text))
----                     -> HStreamClientApi -> IO ()
---- withConsumerSpecEnv = provideRunTest setup clean
----   where
----     setup api = do
----       streamName <- ("ConsumerSpec_" <>) <$> newRandomText 20
----       subName    <- ("ConsumerSpec_" <>) <$> newRandomText 20
----
----       let offset = SubscriptionOffset . Just . SubscriptionOffsetOffsetSpecialOffset
----                    . Enumerated . Right $ SubscriptionOffset_SpecialOffsetLATEST
----       let stream = Stream streamName 1
----       createStreamRequest api stream `shouldReturn` stream
----       createSubscriptionRequest api subName streamName offset `shouldReturn` True
----       return (streamName, subName)
----
----     clean api (streamName, subName) = do
----       deleteSubscriptionRequest api subName `shouldReturn` True
----       cleanStreamReq api streamName `shouldReturn` PB.Empty
----
---- consumerSpec :: Spec
---- consumerSpec = aroundAll provideHstreamApi $ xdescribe "ConsumerSpec" $ do
----
----   aroundWith withConsumerSpecEnv $ do
----
----     timeStamp <- runIO getProtoTimestamp
----     let header = buildRecordHeader HStreamRecordHeader_FlagRAW Map.empty timeStamp T.empty
----
----     it "test streamFetch request" $ \(api, (streamName, subName)) -> do
----       let msgCnt = 30
----           maxPayloadSize = 1300
----       (reqRids, totalSize) <- produceRecords api header streamName msgCnt maxPayloadSize
----       Log.debug $ "length reqRids = " <> Log.buildInt (length reqRids) <> ", totalSize = " <> Log.buildInt totalSize
----
----       verifyConsumer api subName totalSize reqRids [defaultHacker]
----       Log.info "streamFetch test done !!!!!!!!!!!"
----
----     it "test retrans unacked msg" $ \(api, (streamName, subName)) -> do
----       originCh <- newChan
----       hackerCh <- newChan
----       terminate <- newChan
----       void $ forkIO (streamFetchRequestWithChannel api subName originCh hackerCh terminate randomKillHacker)
----
----       (reqRids, totalSize) <- produceRecords api header streamName 200 20
----       Log.debug $ "length reqRids = " <> Log.buildInt (length reqRids) <> ", totalSize = " <> Log.buildInt totalSize
----
----       originAck <- readChan originCh
----       hackedAck <- readChan hackerCh
----       retransAck <- collectRetrans 0 originCh (V.length originAck - V.length hackedAck) []
----       Log.debug . Log.buildString $ "retransAck length = " <> show (length retransAck)
----       let diff = V.toList originAck L.\\ V.toList hackedAck
----       retransAck `shouldBe` V.fromList diff
----       writeChan terminate ()
----       Log.info "retrans unacked msg test done !!!!!!!!!!!"
----
----     it "test retrans timeout msg" $ \(api, (streamName, subName)) -> do
----       originCh <- newChan
----       hackerCh <- newChan
----       terminate <- newChan
----       void $ forkIO (streamFetchRequestWithChannel api subName originCh hackerCh terminate timeoutHacker)
----
----       (reqRids, totalSize) <- produceRecords api header streamName 200 20
----       Log.debug $ "length reqRids = " <> Log.buildInt (length reqRids) <> ", totalSize = " <> Log.buildInt totalSize
----
----       originAck <- readChan originCh
----       let hackedAck = V.empty
----       retransAck <- collectRetrans 0 originCh (V.length originAck - V.length hackedAck) []
----       Log.debug . Log.buildString $ "retransAck length = " <> show (length retransAck)
----       -- delay here to give server some time to complete previous retrans
----       threadDelay 1000000
----       retransAck `shouldBe` originAck
----       writeChan terminate ()
----       Log.info "retrans timeout msg done !!!!!!!!!!!"
----
----     -- FIXME:Wrong ack messages cause the server to fail to update the ack window and commit checkpoints,
----     -- but the only way to verify the server's behavior now is check the debug logs, so this test is always successful.
----     it "test ack wrong msg" $ \(api, (streamName, subName)) -> do
----       originCh <- newChan
----       hackerCh <- newChan
----       terminate <- newChan
----
----       let record = buildRecord header "1"
----       RecordId{..} <- V.head . appendResponseRecordIds <$> appendRequest api streamName (V.singleton record)
----       let !latesLSN = recordIdBatchId + 1
----       void $ forkIO (streamFetchRequestWithChannel api subName originCh hackerCh terminate (wrongRidHacker latesLSN))
----
----       (reqRids, totalSize) <- produceRecords api header streamName 5 2
----       Log.debug $ "length reqRids = " <> Log.buildInt (length reqRids) <> ", totalSize = " <> Log.buildInt totalSize
----
----       originAck <- readChan originCh
----       let hackedAck = V.empty
----       retransAck <- collectRetrans 0 originCh (V.length originAck - V.length hackedAck) []
----       Log.debug . Log.buildString $ "retransAck length = " <> show (length retransAck)
----       retransAck `shouldBe` originAck
----       writeChan terminate ()
----       Log.info "ack wrong msg test done !!!!!!!!!!!"
----
----   -- TODO:
----   -- test need to add
----   --  1. fetch/ack unsubscribed subscription will fail
----   --  2. validate commit and checkpoint
----   --  3. test subscribe from any offset
----
---- ----------------------------------------------------------------------------------------------------------
---- -- ConsumerGroupSpec
----
---- -- TODO:
---- -- 1. what should happen if all consumer exist the consumer group?
---- --    a. if any consumer join the group again, what should happen?
---- -- 2. if a consumer join multi consumer groups, what should happen?
----
---- consumerGroupSpec :: Spec
---- consumerGroupSpec = aroundAll provideHstreamApi $ xdescribe "ConsumerGroupSpec" $ do
----
----   aroundWith withConsumerSpecEnv $ do
----
----      timeStamp <- runIO getProtoTimestamp
----      let header = buildRecordHeader HStreamRecordHeader_FlagRAW Map.empty timeStamp T.empty
----
----      it "test consumerGroup" $ \(api, (streamName, subName)) -> do
----        let msgCnt = 500
----            maxPayloadSize = 20
----        (reqRids, totalSize) <- produceRecords api header streamName msgCnt maxPayloadSize
----        Log.debug $ "length reqRids = " <> Log.buildInt (length reqRids) <> ", totalSize = " <> Log.buildInt totalSize
----
----        let hackers = V.replicate 5 [defaultHacker]
----        verifyConsumerGroup api subName totalSize reqRids hackers
----        Log.info "test consumerGroup done !!!!!!!!"
----
----      it "test consumerGroup with timeout" $ \(api, (streamName, subName)) -> do
----        let msgCnt = 200
----        (reqRids, totalSize) <- produceRecords api header streamName msgCnt 20
----        Log.debug $ "length reqRids = " <> Log.buildInt (length reqRids) <> ", totalSize = " <> Log.buildInt totalSize
----
----        let hackers = V.foldl'
----                       (\acc i -> if even i
----                                    then V.cons [defaultHacker] acc
----                                    else V.cons [timeoutHacker, defaultHacker] acc
----                       )
----                       V.empty
----                       (V.fromList @Int [1..5])
----        verifyConsumerGroup api subName totalSize reqRids hackers
----        Log.info "test consumerGroup with timeout done !!!!!!!!"
----
----      it "test consumerGroup with unacked" $ \(api, (streamName, subName)) -> do
----        let msgCnt = 200
----        (reqRids, totalSize) <- produceRecords api header streamName msgCnt 20
----        Log.debug $ "length reqRids = " <> Log.buildInt (length reqRids) <> ", totalSize = " <> Log.buildInt totalSize
----
----        let hackers = V.foldl'
----                       (\acc i -> if even i
----                                    then V.cons [defaultHacker] acc
----                                    else V.cons [randomKillHacker, defaultHacker] acc
----                       )
----                       V.empty
----                       (V.fromList @Int [1..5])
----        verifyConsumerGroup api subName totalSize reqRids hackers
----        Log.info "test consumerGroup with unacked done !!!!!!!!"
----
----      it "test consumerGroup with multi chaos" $ \(api, (streamName, subName)) -> do
----        let msgCnt = 200
----        (reqRids, totalSize) <- produceRecords api header streamName msgCnt 20
----        Log.debug $ "length reqRids = " <> Log.buildInt (length reqRids) <> ", totalSize = " <> Log.buildInt totalSize
----
----        let hackerList = [defaultHacker, randomKillHacker, timeoutHacker]
----        hackers <- V.forM (V.fromList [1::Int ..5]) $ \_ -> do
----          idx <- randomRIO (0,2)
----          return [hackerList !! idx, defaultHacker]
----        verifyConsumerGroup api subName totalSize reqRids hackers
----        Log.info "test consumerGroup with multi chaos done !!!!!!!!"
----
----      it "test kill consumer" $ \(api, (streamName, subName)) -> do
----        let msgCnt = 200
----        (reqRids, totalSize) <- produceRecords api header streamName msgCnt 20
----        Log.debug $ "length reqRids = " <> Log.buildInt (length reqRids) <> ", totalSize = " <> Log.buildInt totalSize
----        terminate <- newChan
----        condVar <- newEmptyMVar
----        let idxs = [1, 3]
----        sig <- newMVar (Set.empty, condVar)
----        res <- forM (V.fromList @Int [1..5]) $ \i -> do
----          let consumerName = T.pack $ "consumer_" ++ show i
----          responses <- newIORef Set.empty
----          tch <- dupChan terminate
----          tid <- forkIO (streamFetchRequest api consumerName subName responses tch sig totalSize [defaultHacker])
----          return (tid, responses)
----        forM_ idxs $ \index -> do
----          Log.d $ "kill " <> Log.buildInt index
----          killThread . fst $ res V.! (index - 1)
----
----        waitResponse condVar terminate
----        result <- forM res $ readIORef . snd
----        let reqSet = Set.fromList . V.toList . V.concat $ reqRids
----            repSet = Set.unions result
----            reqSize = Set.size reqSet
----            repSize = Set.size repSet
----        repSize `shouldBe` reqSize
----        Set.difference reqSet repSet `shouldBe` Set.empty
----        repSet `shouldBe` reqSet
----        Log.info "test kill consumer done !!!!!!!!"
----
----      it "test add consumer" $ \(api, (streamName, subName)) -> do
----        let msgCnt = 500
----        (reqRids, totalSize) <- produceRecords api header streamName msgCnt 20
----        Log.debug $ "length reqRids = " <> Log.buildInt (length reqRids) <> ", totalSize = " <> Log.buildInt totalSize
----        terminate <- newChan
----        condVar <- newEmptyMVar
----        sig <- newMVar (Set.empty, condVar)
----        res' <- forM (V.fromList @Int [1..5]) $ \i -> do
----          let consumerName = T.pack $ "consumer_" ++ show i
----          responses <- newIORef Set.empty
----          tch <- dupChan terminate
----          void $ forkIO (streamFetchRequest api consumerName subName responses tch sig totalSize [defaultHacker])
----          return responses
----
----        Log.debug "add new consumer"
----        responses <- newIORef Set.empty
----        tch <- dupChan terminate
----        let consumerName = T.pack $ "consumer_" ++ show (length res' + 1)
----        void $ forkIO (streamFetchRequest api consumerName subName responses tch sig totalSize [defaultHacker])
----        let res = res' V.++ V.singleton responses
----
----        waitResponse condVar terminate
----        result <- forM res readIORef
----        let reqSet = Set.fromList . V.toList . V.concat $ reqRids
----            repSet = Set.unions result
----            reqSize = Set.size reqSet
----            repSize = Set.size repSet
----        repSize `shouldBe` reqSize
----        Set.difference reqSet repSet `shouldBe` Set.empty
----        repSet `shouldBe` reqSet
----        Log.info "test add consumer done !!!!!!!!"
----
---- ----------------------------------------------------------------------------------------------------------
----
---- streamFetchRequest
----   :: HStreamClientApi
----   -> T.Text                        -- consumerName
----   -> T.Text                        -- subscriptionID
----   -> IORef (Set RecordId)
----   -> Chan ()                       -- channel use to close client request
----   -> MVar (Set RecordId, CondVar)  -- use as a condition var
----   -> Int                           -- total response need to check
----   -> [Hacker]
----   -> IO ()
---- streamFetchRequest HStreamApi{..} consumerName subscribeId responses terminate conVar total hackerList = do
----   let req = ClientBiDiRequest streamingReqTimeout (MetadataMap Map.empty) action
----   hstreamApiStreamingFetch req >>= \case
----     ClientBiDiResponse _meta StatusCancelled detail -> Log.info . Log.buildString $ "request cancel" <> show detail
----     ClientBiDiResponse _meta StatusOk _msg -> Log.debug "fetch request done"
----     ClientBiDiResponse _meta stats detail -> Log.fatal . Log.buildString $ "abnormal status: " <> show stats <> ", msg: " <> show detail
----     ClientErrorResponse err -> Log.e . Log.buildString $ "fetch request err: " <> show err
----   where
----     action call _meta streamRecv streamSend _done = do
----       let initReq = StreamingFetchRequest subscribeId consumerName V.empty
----       streamSend initReq >>= \case
----         Left err -> do
----           Log.e . Log.buildString $ "Server error happened when send init streamFetchRequest err: " <> show err
----           clientCallCancel call
----         Right _ -> return ()
----       void $ race (doFetch streamRecv streamSend call hackerList) (notifyDone call)
----
----     doFetch streamRecv streamSend call hackers = do
----       streamRecv >>= \case
----         Left err -> do
----           Log.e . Log.buildString $ "Error happened when recv from server " <> show err
----           clientCallCancel call
----         Right Nothing -> do
----           Log.debug $ "consumer " <> Log.buildText consumerName <> ", server close, fetch request end."
----           clientCallCancel call
----         Right (Just StreamingFetchResponse{..}) -> do
----           -- get recordId from response
----           let ackIds = V.map (fromJust . receivedRecordRecordId) streamingFetchResponseReceivedRecords
----           -- Log.debug $ "consumer " <> Log.buildText consumerName <> " get length of response: " <> Log.buildInt (V.length ackIds)
----
----           let curHacker = head hackers
----           let newHackers = if length hackers == 1 then hackers else tail hackers
----
----           ackIds' <- curHacker ackIds
----
----           -- add responsed recordId to set, if recevied enough response, notify the cond_var
----           let newSet = foldr Set.insert Set.empty ackIds'
----           originSet <- readIORef responses
----           -- total records received without duplicate
----           let uSet = Set.union originSet newSet
----           -- new records received this turn
----           let dSet = Set.difference newSet originSet
----           writeIORef responses uSet
----           modifyMVar_ conVar $ \(s, sig) -> do
----             let totalSet = Set.union s dSet
----             -- Log.e $ "consumer is updating total responses, total size before update: " <> Log.buildInt (Set.size s) <> ", after update: " <> Log.buildInt (Set.size totalSet)
----             when (Set.size totalSet == total) $ putMVar sig () >> Log.debug "get all result !!!!!"
----             return (totalSet, sig)
----
----           -- send ack
----           let fetReq = StreamingFetchRequest subscribeId consumerName ackIds'
----           streamSend fetReq >>= \case
----             Left err -> do
----               Log.e . Log.buildString $ "Error happened when send ack: " <> show err <> "\n ack context: " <> show fetReq
----               clientCallCancel call
----             Right _ -> do
----               doFetch streamRecv streamSend call newHackers
----
----     notifyDone call = do
----       void $ readChan terminate
----       res <- readIORef responses
----       Log.debug $ "finally consumer " <> Log.buildText consumerName <> " get length of response: " <> Log.buildInt (Set.size res)
----       clientCallCancel call
----       Log.d "client cancel fetch request"
----
---- streamFetchRequestWithChannel
----   :: HStreamClientApi
----   -> T.Text
----   -> Chan (V.Vector RecordId) -- channel use to trans record recevied from server
----   -> Chan (V.Vector RecordId) -- channel use to trans record after hacker
----   -> Chan ()                  -- channel use to close client request
----   -> (V.Vector RecordId -> IO (V.Vector RecordId)) -- hacker function
----   -> IO ()
---- streamFetchRequestWithChannel HStreamApi{..} subscribeId originCh hackerCh terminate hacker = do
----   consumerName <- newRandomText 5
----   let req = ClientBiDiRequest streamingReqTimeout (MetadataMap Map.empty) (action consumerName)
----   hstreamApiStreamingFetch req >>= \case
----     ClientBiDiResponse _meta StatusCancelled detail -> Log.info . Log.buildString $ "request cancel" <> show detail
----     ClientBiDiResponse _meta StatusOk _msg -> Log.debug "fetch request done"
----     ClientBiDiResponse _meta stats detail -> Log.fatal . Log.buildString $ "abnormal status: " <> show stats <> ", msg: " <> show detail
----     ClientErrorResponse err -> Log.e . Log.buildString $ "fetch request err: " <> show err
----   where
----     action consumerName call _meta streamRecv streamSend _done = do
----       let initReq = StreamingFetchRequest subscribeId consumerName V.empty
----       streamSend initReq >>= \case
----         Left err -> do
----           Log.e . Log.buildString $ "Server error happened when send init streamFetchRequestWithChannel err: " <> show err
----           clientCallCancel call
----         Right _ -> return ()
----       void $ race (doFetch streamRecv streamSend hacker call consumerName) (notifyDone call)
----
----     doFetch streamRecv streamSend hacker' call consumerName = do
----       streamRecv >>= \case
----         Left err -> do
----           Log.e . Log.buildString $ "Error happened when recv from server " <> show err
----           clientCallCancel call
----         Right Nothing -> do
----           Log.debug "server close, fetch request end."
----           clientCallCancel call
----         Right (Just StreamingFetchResponse{..}) -> do
----           -- get recordId from response
----           let ackIds = V.map (fromJust . receivedRecordRecordId) streamingFetchResponseReceivedRecords
----           -- select ackIds
----           ackIds' <- hacker' ackIds
----           writeChan originCh ackIds
----           writeChan hackerCh ackIds'
----
----           -- send ack
----           let fetReq = StreamingFetchRequest subscribeId consumerName ackIds'
----           streamSend fetReq >>= \case
----             Left err -> do
----               Log.e . Log.buildString $ "Error happened when send ack: " <> show err <> "\n ack context: " <> show fetReq
----               clientCallCancel call
----             Right _ -> do
----               doFetch streamRecv streamSend defaultHacker call consumerName
----
----     notifyDone call = do
----       void $ readChan terminate
----       clientCallCancel call
----       Log.d "client cancel fetch request"
----
---- type CondVar = MVar ()
---- type Hacker = V.Vector RecordId -> IO(V.Vector RecordId)
----
---- produceRecords
----   :: HStreamClientApi
----   -> HStreamRecordHeader
----   -> T.Text
----   -> Int -> Int
----   -> IO([V.Vector RecordId], Int)
---- produceRecords api header streamName msgCount maxBatchSize = do
----   batchSize <- newIORef 0
----   reqRids <- replicateM msgCount $ do
----     size <- randomRIO (1, maxBatchSize)
----     payload <- V.map (buildRecord header) <$> V.replicateM size (newRandomByteString 2)
----     modifyIORef' batchSize (+size)
----     appendResponseRecordIds <$> appendRequest api streamName payload
----   sz <- readIORef batchSize
----   return (reqRids, sz)
----
---- doConsume
----   :: HStreamClientApi
----   -> T.Text
----   -> T.Text
----   -> MVar (Set RecordId, CondVar)
----   -> Int
----   -> Chan()
----   -> [Hacker]
----   -> IO (IORef (Set RecordId))
---- doConsume api consumerName subName sig totalSize terminateCh hackers = do
----   responses <- newIORef Set.empty
----   tch <- dupChan terminateCh
----   void $ forkIO (streamFetchRequest api consumerName subName responses tch sig totalSize hackers)
----   return responses
----
---- verifyConsumer
----   :: HStreamClientApi
----   -> T.Text
----   -> Int
----   -> [V.Vector RecordId]
----   -> [Hacker]
----   -> IO ()
---- verifyConsumer api subName totalSize reqRids hackers = do
----   verifyConsumerGroup api subName totalSize reqRids $ V.singleton hackers
----
---- verifyConsumerGroup
----   :: HStreamClientApi
----   -> T.Text
----   -> Int
----   -> [V.Vector RecordId]
----   -> V.Vector [Hacker]
----   -> IO ()
---- verifyConsumerGroup api subName totalSize reqRids hackers = do
----   terminate <- newChan
----   condVar <- newEmptyMVar
----   sig <- newMVar (Set.empty, condVar)
----   res <- V.iforM hackers $ \idx hacker -> do
----     let consumerName = T.pack $ "consumer_" ++ show (idx + 1)
----     doConsume api consumerName subName sig totalSize terminate hacker
----
----   waitResponse condVar terminate
----   result <- forM res readIORef
----   let reqSet = Set.fromList . V.toList . V.concat $ reqRids
----       repSet = Set.unions result
----       reqSize = Set.size reqSet
----       repSize = Set.size repSet
----   repSize `shouldBe` reqSize
----   Set.difference reqSet repSet `shouldBe` Set.empty
----   repSet `shouldBe` reqSet
----
---- waitResponse :: MVar a -> Chan () -> IO ()
---- waitResponse condVar terminate =
----   timeout 25000000 (readMVar condVar) >>= \case
----     Nothing -> do
----       terminateConnect
----       error "STREAMING FETCH TIME OUT ERROR!"
----     Just _ -> do
----       terminateConnect
----   where
----     terminateConnect = threadDelay 1000000 >> writeChan terminate ()
----
---- collectRetrans :: Int -> Chan (V.Vector RecordId) -> Int -> [V.Vector RecordId]-> IO (V.Vector RecordId)
---- collectRetrans cnt channel maxCount res
----   | maxCount <= cnt = return $ V.concat res
----   | otherwise = do
----       ids <- readChan channel
----       if V.length ids /= 1
----          then do
----            collectRetrans cnt channel maxCount res
----          else do
----            let nres = res L.++ [ids]
----            collectRetrans (cnt + 1) channel maxCount nres
----
---- defaultHacker :: Hacker
---- defaultHacker = pure
----
---- randomKillHacker :: Hacker
---- randomKillHacker rids = do
----   let size = V.length rids
----   seed <- randomRIO (1, min 10 (size `div` 2))
----   let retrans = V.ifilter (\idx _ -> idx `mod` seed == 0) rids
----   Log.d $ "length of recordIds which need to retrans: " <> Log.buildInt (V.length retrans)
----   return $ V.ifilter (\idx _ -> idx `mod` seed /= 0) rids
----
---- timeoutHacker :: Hacker
---- timeoutHacker _ = threadDelay 7000000 >> return V.empty
----
---- wrongRidHacker :: Word64 -> Hacker
---- wrongRidHacker lsn _ = do
----   let wrongIdxRecord = RecordId lsn 100005
----       wrongLSNRecord = RecordId (lsn + 50000) 99
----   return . V.fromList $ [wrongLSNRecord, wrongIdxRecord]
----
requestTimeout :: Int
requestTimeout = 10
----
---- streamingAckTimeout :: Int32
---- streamingAckTimeout = 3
----
---- streamingReqTimeout :: Int
---- streamingReqTimeout = 10000000
