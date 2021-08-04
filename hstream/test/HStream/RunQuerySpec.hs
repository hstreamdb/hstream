{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.RunQuerySpec (spec) where

import           Control.Concurrent               (forkIO, threadDelay)
import           Control.Monad                    (void)
import qualified Data.Aeson                       as Aeson
import qualified Data.ByteString.Lazy             as BS
import qualified Data.Map.Strict                  as Map
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated
import qualified Proto3.Suite                     as PB
import           Test.Hspec                       (ActionWith, Spec, aroundAll,
                                                   aroundWith, describe, it,
                                                   parallel, runIO, shouldBe,
                                                   shouldReturn)

import qualified HStream.Logger                   as Log
import           HStream.Server.HStreamApi
import           HStream.SpecUtils
import           HStream.Store.Logger
import qualified HStream.ThirdParty.Protobuf      as PB
import           HStream.Utils                    (HStreamClientApi,
                                                   buildRecord,
                                                   buildRecordHeader,
                                                   getProtoTimestamp,
                                                   getServerResp)

{-
  rpc CreateQueryStream(CreateQueryStreamRequest) returns (CreateQueryStreamResponse) {}
  rpc ListQueries(ListQueriesRequest) returns (ListQueriesResponse) {}
  rpc GetQuery(GetQueryRequest) returns (Query) {}
  rpc TerminateQueries(TerminateQueriesRequest) returns (TerminateQueriesResponse) {}
  rpc DeleteQuery(DeleteQueryRequest) returns (google.protobuf.Empty) {}
-}

mkClientRequest :: a -> ClientRequest 'Normal a b
mkClientRequest req =
  ClientNormalRequest req 5 (MetadataMap Map.empty)

spec :: Spec
spec = describe "HStream.RunQuerySpec" $ do
  runIO $ setLogDeviceDbgLevel C_DBG_ERROR
  querySpec

querySpecAround :: ActionWith (HStreamClientApi, (TL.Text, TL.Text, TL.Text))
  -> HStreamClientApi -> IO ()
querySpecAround = provideRunTest setup clean
  where
    setup api = do
      source <- TL.fromStrict <$> newRandomText 20
      sink   <- TL.fromStrict <$> newRandomText 20
      createStream api source
      (sink', qid) <- createQueryStream api sink $ "SELECT * FROM " <> source <> " EMIT CHANGES;"
      sink' `shouldBe` sink'
      return (source, sink', qid)
    clean api (source, sink, _) = do
      deleteStream api source
      queries <- listQueries api
      mapM_ (deleteQuery api) (getRelatedQueries queries sink)
      queries' <- listQueries api
      getRelatedQueries queries' sink `shouldBe` []
      deleteStream api sink

querySpec :: Spec
querySpec = aroundAll provideHstreamApi $ aroundWith querySpecAround $
  describe "StartQuerySpec" $ parallel $ do
    -- stream query means the associated select query when send a CreateQueryStreamRequest

    it "get query info" $ \(api, (_, _, qid)) ->
      getQuery api qid

    it "check if the query is running" $ \(api, (source, sink, _)) -> do
      _ <- forkIO $ do
        timeStamp <- getProtoTimestamp
        let header = buildRecordHeader HStreamRecordHeader_FlagJSON Map.empty timeStamp TL.empty
            records = buildRecord header $ BS.toStrict $
              Aeson.encode $ Map.fromList [("temperature" :: String, 22 :: Int), ("humidity", 80)]
        threadDelay 5000000
        Log.d $ "Insert into " <> Log.buildLazyText source <> " ..."
        insertIntoStream api source $ V.singleton records

      executeCommandPushQuery ("SELECT * FROM " <> sink <> " EMIT CHANGES;")
        `shouldReturn`
          [ mkStruct [("temperature", Aeson.Number 22), ("humidity", Aeson.Number 80)]]

    it "list queries" $ \(api, (_, sink, _)) -> do
      void $ executeCommandPushQuery ("SELECT * FROM " <> sink <> " EMIT CHANGES;")
      void $ executeCommandPushQuery ("SELECT * FROM " <> sink <> " EMIT CHANGES;")
      queries <- listQueries api
      length (getRelatedQueries queries sink) `shouldBe` 2

--------------------------------------------------------------------------------

createStream :: HStreamClientApi -> TL.Text -> IO ()
createStream HStreamApi{..} source = action `grpcShouldReturn` Stream source 3
  where
    action = hstreamApiCreateStream $ mkClientRequest
      PB.def {streamStreamName = source, streamReplicationFactor = 3}

deleteStream :: HStreamClientApi -> TL.Text -> IO ()
deleteStream HStreamApi{..} source = action `grpcShouldReturn` PB.Empty
  where
    action = hstreamApiDeleteStream $ mkClientRequest
      PB.def {deleteStreamRequestStreamName = source}

insertIntoStream :: HStreamClientApi -> TL.Text -> V.Vector HStreamRecord -> IO ()
insertIntoStream HStreamApi{..} sink records = do
  AppendResponse sName _<- getServerResp =<< action
  sName `shouldBe` sink
  where
    action = hstreamApiAppend $ mkClientRequest
      PB.def { appendRequestStreamName = sink
          , appendRequestRecords = records }

getQuery :: HStreamClientApi -> TL.Text -> IO ()
getQuery HStreamApi{..} qid = do
  Query {..}<- getServerResp =<< action
  queryId `shouldBe` qid
  queryStatus `shouldBe` PB.Enumerated (Right Query_StatusRunning) -- Running FIXME:Status should not be just a number
  where
    action = hstreamApiGetQuery $ mkClientRequest PB.def {getQueryRequestId = qid}

createQueryStream :: HStreamClientApi -> TL.Text -> TL.Text -> IO (TL.Text, TL.Text)
createQueryStream HStreamApi{..} sink sql = do
  CreateQueryStreamResponse (Just stream) qid <- getServerResp =<< action
  return (streamStreamName stream, qid)
  where
    action = hstreamApiCreateQueryStream $ mkClientRequest
      PB.def { createQueryStreamRequestQueryStream = Just $
                 PB.def {streamStreamName = sink, streamReplicationFactor = 3}
             , createQueryStreamRequestQueryStatements = sql
             }

listQueries :: HStreamClientApi -> IO [Query]
listQueries HStreamApi{..} = do
  ListQueriesResponse queries <- getServerResp =<<
    hstreamApiListQueries (mkClientRequest PB.def)
  return . V.toList $ queries

deleteQuery :: HStreamClientApi -> TL.Text -> IO ()
deleteQuery HStreamApi{..} qid =
  hstreamApiDeleteQuery (mkClientRequest PB.def {deleteQueryRequestId = qid})
    `grpcShouldReturn` PB.Empty

getRelatedQueries :: [Query] -> TL.Text -> [TL.Text]
getRelatedQueries queries sName =
  [queryId query
    | query@Query{queryQueryType = Just qType} <- queries
    , sName `elem` queryTypeSourceStreamNames qType]
