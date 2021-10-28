{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Client.Action where

import           Control.Concurrent
import           Control.Monad
import           Data.Bifunctor
import qualified Data.ByteString                  as BS
import           Data.Function
import           Data.Functor
import qualified Data.HashMap.Strict              as HM
import qualified Data.List                        as L
import qualified Data.Map                         as Map
import           Data.Maybe
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import           GHC.Int                          (Int32)
import           HStream.Client.Gadget
import           HStream.Client.Utils
import qualified HStream.Logger                   as Log
import           HStream.SQL.AST                  (RStatsTable (..))
import           HStream.SQL.Codegen              (DropObject (..),
                                                   InsertType (..), StreamName,
                                                   TerminationSelection (..))
import           HStream.Server.HStreamApi        (DeleteSubscriptionRequest (deleteSubscriptionRequestSubscriptionId))
import qualified HStream.Server.HStreamApi        as API
import           HStream.ThirdParty.Protobuf      (Empty (..))
import           HStream.Utils                    (HStreamClientApi,
                                                   buildRecord,
                                                   buildRecordHeader,
                                                   cBytesToLazyText,
                                                   getProtoTimestamp,
                                                   getServerResp)
import           Network.GRPC.HighLevel.Generated (ClientError (..),
                                                   ClientRequest (..),
                                                   ClientResult (..),
                                                   GRPCIOError (..),
                                                   GRPCMethodType (Normal),
                                                   MetadataMap (MetadataMap),
                                                   withGRPCClient)
import           Proto3.Suite                     (Enumerated (Enumerated))
import           Proto3.Suite.Class               (def)
import qualified Text.Layout.Table                as LT

createStream :: HStreamClientApi -> StreamName -> Int -> IO (ClientResult 'Normal API.Stream)
createStream API.HStreamApi{..} sName rFac =
  hstreamApiCreateStream (mkClientNormalRequest def
    { API.streamStreamName        = TL.fromStrict sName
    , API.streamReplicationFactor = fromIntegral rFac})

listStreams :: HStreamClientApi -> IO (ClientResult 'Normal API.ListStreamsResponse)
listStreams API.HStreamApi{..} = hstreamApiListStreams clientDefaultRequest
listViews :: HStreamClientApi -> IO (ClientResult 'Normal API.ListViewsResponse)
listViews API.HStreamApi{..} = hstreamApiListViews clientDefaultRequest
listQueries :: HStreamClientApi -> IO (ClientResult 'Normal API.ListQueriesResponse)
listQueries API.HStreamApi{..} = hstreamApiListQueries clientDefaultRequest
listConnectors :: HStreamClientApi -> IO (ClientResult 'Normal API.ListConnectorsResponse)
listConnectors API.HStreamApi{..} = hstreamApiListConnectors clientDefaultRequest

terminateQueries :: HStreamClientApi
  -> TerminationSelection
  -> IO (ClientResult 'Normal API.TerminateQueriesResponse )
terminateQueries API.HStreamApi{..} (OneQuery qid) =
  hstreamApiTerminateQueries
    (mkClientNormalRequest def{API.terminateQueriesRequestQueryId = V.singleton $ cBytesToLazyText qid})
terminateQueries API.HStreamApi{..} AllQueries =
  hstreamApiTerminateQueries
    (mkClientNormalRequest def{API.terminateQueriesRequestAll = True})
terminateQueries API.HStreamApi{..} (ManyQueries qids) =
  hstreamApiTerminateQueries
    (mkClientNormalRequest
      def {API.terminateQueriesRequestQueryId = V.fromList $ cBytesToLazyText <$> qids})

dropAction :: HStreamClientApi -> Bool -> DropObject -> IO (ClientResult 'Normal Empty)
dropAction API.HStreamApi{..} checkIfExist dropObject = do
  case dropObject of
    DStream    txt -> hstreamApiDeleteStream (mkClientNormalRequest def
                      { API.deleteStreamRequestStreamName     = TL.fromStrict txt
                      , API.deleteStreamRequestIgnoreNonExist = checkIfExist
                      })

    DView      txt -> hstreamApiDeleteView (mkClientNormalRequest def
                      { API.deleteViewRequestViewId = TL.fromStrict txt
                      -- , API.deleteViewRequestIgnoreNonExist = checkIfExist
                      })

    DConnector txt -> hstreamApiDeleteConnector (mkClientNormalRequest def
                      { API.deleteConnectorRequestId = TL.fromStrict txt
                      -- , API.deleteConnectorRequestIgnoreNonExist = checkIfExist
                      })

insertIntoStream :: ClientContext
  -> StreamName -> InsertType -> BS.ByteString
  -> IO (ClientResult 'Normal API.AppendResponse)
insertIntoStream ctx@ClientContext{..} sName insertType payload = do
  curProducers <- readMVar producers
  curNode <- readMVar currentServer
  case Map.lookup sName curProducers of
    Just realNode -> go realNode
    Nothing       -> do
      lookupStream ctx curNode sName >>= \case
        Nothing -> do
          Log.e "Failed to get any avaliable server."
          return $ ClientErrorResponse (ClientIOError GRPCIOUnknownError)
        Just realNode -> do
          modifyMVar_ producers (return . Map.insert sName realNode)
          go realNode
  where
    go node_ = do
      let addr_ = serverNodeToSocketAddr node_
      withGRPCClient (mkGRPCClientConf addr_) $ \client -> do
        API.HStreamApi{..} <- API.hstreamApiClient client
        timestamp <- getProtoTimestamp
        let header = case insertType of
              JsonFormat -> buildRecordHeader API.HStreamRecordHeader_FlagJSON Map.empty timestamp TL.empty
              RawFormat  -> buildRecordHeader API.HStreamRecordHeader_FlagRAW Map.empty timestamp TL.empty
            record = buildRecord header payload
        resp <- hstreamApiAppend (mkClientNormalRequest def
                                  { API.appendRequestStreamName = TL.fromStrict sName
                                  , API.appendRequestRecords    = V.singleton record
                                  })
        case resp of
          (ClientNormalResponse _ _meta1 _meta2 _code _details) -> return resp
          _ -> do
            m_node <- lookupStream ctx addr_ sName
            case m_node of
              Nothing -> do
                Log.e "Failed to get any avaliable server."
                return $ ClientErrorResponse (ClientIOError GRPCIOUnknownError)
              Just newNode -> do
                modifyMVar_ producers (return . Map.insert sName newNode)
                insertIntoStream ctx sName insertType payload

createStreamBySelect :: HStreamClientApi
  -> TL.Text -> Int -> [String]
  -> IO (ClientResult 'Normal API.CreateQueryStreamResponse)
createStreamBySelect API.HStreamApi{..} sName rFac sql =
  hstreamApiCreateQueryStream (mkClientNormalRequest def
    { API.createQueryStreamRequestQueryStream
        = Just def
        { API.streamStreamName        = sName
        , API.streamReplicationFactor = fromIntegral rFac}
    , API.createQueryStreamRequestQueryStatements = extractSelect sql})

--------------------------------------------------------------------------------
sqlStatsAction :: HStreamClientApi -> ([T.Text], RStatsTable, [T.Text]) -> IO ()
sqlStatsAction api (colNames, tableKind, streamNames) = do
  tableRes <- api & case tableKind of
    AppendInBytes -> queryAllAppendInBytes
    RecordBytes   -> queryAllRecordBytes
  putStrLn $ processTable tableRes colNames streamNames

data StatsValue
  = NULL
  | INTEGER Int32
  | REAL    Double
  | TEXT    T.Text
  | BOOL    Bool
  deriving (Eq)

instance Show StatsValue where
  show = \case
    NULL      -> "NULL"
    INTEGER i -> show i
    REAL    f -> show f
    TEXT    s -> show s
    BOOL    b -> show b

queryAllAppendInBytes, queryAllRecordBytes :: HStreamClientApi -> IO (HM.HashMap T.Text (HM.HashMap T.Text StatsValue))
queryAllAppendInBytes api = queryPerStreamTimeSeriesStatsAll api "appends_in"
  ["throughput_1min", "throughput_5min", "throughput_10min"]   . V.fromList $ map (* 1000)
  [60               , 300              , 600]
queryAllRecordBytes   api = queryPerStreamTimeSeriesStatsAll api "reads"
  ["throughput_15min", "throughput_30min", "throughput_60min"] . V.fromList $ map (* 1000)
  [900               , 1800              , 3600]

queryPerStreamTimeSeriesStatsAll :: HStreamClientApi
                                 -> T.Text -> [T.Text] -> V.Vector Int32
                                 -> IO (HM.HashMap T.Text (HM.HashMap T.Text StatsValue))
queryPerStreamTimeSeriesStatsAll API.HStreamApi{..} tableName methodNames intervalVec = do
  let statsRequestTimeOut  = 10
      colNames :: [T.Text] = methodNames
      statsRequest         = API.PerStreamTimeSeriesStatsAllRequest (TL.fromStrict tableName) (Just $ API.StatsIntervalVals intervalVec)
      resRequest           = ClientNormalRequest statsRequest statsRequestTimeOut (MetadataMap Map.empty)
  API.PerStreamTimeSeriesStatsAllResponse respM <- hstreamApiPerStreamTimeSeriesStatsAll resRequest >>= getServerResp
  let resp  = filter (isJust . snd) (Map.toList respM) <&> second (map REAL . V.toList . API.statsDoubleValsVals . fromJust)
      lbled = (map . second) (zip colNames) resp
      named = lbled <&> \(proj0, proj1) ->
        let streamId = TL.toStrict proj0
        in  (proj0, ("stream_id", TEXT streamId) : proj1)
  pure . HM.fromList
    $ (map .  first) TL.toStrict
    $ (map . second) HM.fromList named

processTable :: Show a => HM.HashMap T.Text (HM.HashMap T.Text a)
             -> [T.Text]
             -> [T.Text]
             -> String
processTable adminTable selectNames_ streamNames_
  | HM.size adminTable == 0 = "Empty status table." | otherwise =
    if any (`notElem` inTableSelectNames) selectNames || any (`notElem` inTableStreamNames) streamNames
      then "Col name or stream name not in scope."
      else
        let titles     = map T.unpack selectNames
            tableSiz   = L.length selectNames + 1
            colSpecs   = L.replicate tableSiz $ LT.column LT.expand LT.left LT.noAlign (LT.singleCutMark "...")
            tableSty   = LT.asciiS
            headerSpec = LT.titlesH titles
            rowGrps    = [LT.colsAllG LT.center resTable]
        in LT.tableString colSpecs tableSty headerSpec rowGrps
  where
    inTableSelectNames = let xs = (snd . head) (HM.toList adminTable) & map fst . HM.toList
                         in  "stream_id" : (liftTimeNames "min" . L.sort . filter (/= "stream_id")) xs
    inTableStreamNames = fst <$> HM.toList adminTable & L.sort
    selectNames = case selectNames_ of
      [] -> inTableSelectNames
      _  -> selectNames_
    streamNames = case streamNames_ of
      [] -> inTableStreamNames
      _  -> streamNames_
    processedTable = streamNames <&> \curStreamName ->
      let curLn  = HM.lookup curStreamName adminTable & fromJust
          curCol = selectNames <&> \curSelectName -> fromJust $
            HM.lookup curSelectName curLn
      in map show curCol
    resTable = L.transpose processedTable

liftTimeNames :: T.Text -> [T.Text] -> [T.Text]
liftTimeNames timeStr = \case
  []     -> []
  xs     ->
    let normalNames = filter (not . isEndWith timeStr) xs
        timeLbNames = filter (      isEndWith timeStr) xs
        sortedNames = L.sortBy (cmpByLit `on` takeTime timeStr) timeLbNames
    in  sortedNames <> normalNames
  where
  isEndWith postStr txt
    | T.length txt < T.length postStr = False
    | otherwise = T.reverse postStr == T.take (T.length postStr) (T.reverse txt)
  takeTime timeStr_ txt =
    T.takeWhile (/= '_') (T.reverse txt) & T.reverse & \txt' ->
    T.take (T.length txt' - T.length timeStr_) txt'
  cmpByLit numStr0 numStr1 =
    let num0 :: Int = (read (T.unpack numStr0)) :: Int
        num1 :: Int = (read (T.unpack numStr1)) :: Int
    in  compare num0 num1

--------------------------------------------------------------------------------

callSubscription :: ClientContext -> T.Text -> T.Text -> IO ()
callSubscription ctx subId stream = void $ doAction ctx getRespApp handleRespApp
  where
    getRespApp client = do
      let subReq = API.Subscription
                   { API.subscriptionSubscriptionId = TL.fromStrict subId
                   , API.subscriptionStreamName = TL.fromStrict stream
                   , API.subscriptionOffset = Just $ API.SubscriptionOffset
                     (Just $ API.SubscriptionOffsetOffsetSpecialOffset
                       (Enumerated (Right API.SubscriptionOffset_SpecialOffsetLATEST))
                     )
                   , API.subscriptionAckTimeoutSeconds = 1
                   }
      API.HStreamApi{..} <- API.hstreamApiClient client
      hstreamApiCreateSubscription (mkClientNormalRequest subReq)
    handleRespApp :: ClientResult 'Normal API.Subscription -> IO (Maybe a)
    handleRespApp resp = case resp of
      (ClientNormalResponse resp_ _meta1 _meta2 _code _details) -> do
        print "-----------------"
        print resp_
        print "-----------------"
        return Nothing
      _ -> print "Failed!" >> return Nothing

callDeleteSubscription :: ClientContext -> T.Text -> IO ()
callDeleteSubscription ctx subId = void $ doAction ctx getRespApp handleRespApp
  where
    getRespApp client = do
      let req = API.DeleteSubscriptionRequest
                { deleteSubscriptionRequestSubscriptionId = TL.fromStrict subId
                }
      API.HStreamApi{..} <- API.hstreamApiClient client
      hstreamApiDeleteSubscription (mkClientNormalRequest req)
    handleRespApp resp = case resp of
      (ClientNormalResponse _ _meta1 _meta2 _code _details) -> do
        print "-----------------"
        print "Done."
        print "-----------------"
        return Nothing
      _ -> print "Failed!" >> return Nothing

callListSubscriptions :: ClientContext -> IO ()
callListSubscriptions ctx = void $ doAction ctx getRespApp handleRespApp
  where
    getRespApp client = do
      let req = API.ListSubscriptionsRequest
      API.HStreamApi{..} <- API.hstreamApiClient client
      hstreamApiListSubscriptions (mkClientNormalRequest req)
    handleRespApp :: ClientResult 'Normal API.ListSubscriptionsResponse -> IO (Maybe a)
    handleRespApp resp = case resp of
      (ClientNormalResponse (API.ListSubscriptionsResponse subs) _meta1 _meta2 _code _details) -> do
        print "-----------------"
        mapM_ print subs
        print "-----------------"
        return Nothing
      _ -> print "Failed!" >> return Nothing


callStreamingFetch :: ClientContext -> V.Vector API.RecordId -> T.Text -> T.Text -> IO ()
callStreamingFetch ctx startRecordIds subId clientId = do
  curNode <- readMVar (currentServer ctx)
  m_node <- lookupSubscription ctx curNode subId
  case m_node of
    Nothing   -> print "Subscription not found"
    Just node -> withGRPCClient (mkGRPCClientConf . serverNodeToSocketAddr $ node) $ \client -> do
      API.HStreamApi{..} <- API.hstreamApiClient client
      hstreamApiStreamingFetch (ClientBiDiRequest 10000 mempty action)
      return ()
  where
    action clientCall _meta streamRecv streamSend writeDone = do
      go startRecordIds
      where
        go recordIds = do
          let req = API.StreamingFetchRequest
                    { API.streamingFetchRequestSubscriptionId = TL.fromStrict subId
                    , API.streamingFetchRequestConsumerName = TL.fromStrict clientId
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
              print "Stopped. Redirecting..."
              threadDelay 2000000
              callStreamingFetch ctx recordIds subId clientId
