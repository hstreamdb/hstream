{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
module HStream.Client.Action where

import           Data.Bifunctor
import qualified Data.ByteString                  as BS
import           Data.Function
import           Data.Functor
import qualified Data.List                        as L
import qualified Data.Map                         as Map
import           Data.Maybe
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import           GHC.Int                          (Int32)
import           Network.GRPC.HighLevel.Generated (ClientRequest (ClientNormalRequest),
                                                   ClientResult,
                                                   GRPCMethodType (Normal),
                                                   MetadataMap (MetadataMap))
import           Proto3.Suite.Class               (HasDefault, def)

import           Data.Char                        (toUpper)
import qualified Data.HashMap.Strict              as HM
import           HStream.SQL.AST                  (RStatsTable (..))
import           HStream.SQL.Codegen              (DropObject (..),
                                                   InsertType (..), StreamName,
                                                   TerminationSelection (..))
import qualified HStream.Server.HStreamApi        as API
import           HStream.ThirdParty.Protobuf      (Empty)
import           HStream.Utils                    (HStreamClientApi,
                                                   buildRecord,
                                                   buildRecordHeader,
                                                   cBytesToLazyText,
                                                   getProtoTimestamp,
                                                   getServerResp)
import qualified Text.Layout.Table                as LT

createStream :: HStreamClientApi -> StreamName -> Int -> IO (ClientResult 'Normal API.Stream)
createStream API.HStreamApi{..} sName rFac =
  hstreamApiCreateStream (mkClientNormalRequest def
    { API.streamStreamName        = TL.fromStrict sName
    , API.streamReplicationFactor = fromIntegral rFac})

listStreams :: HStreamClientApi -> IO (ClientResult 'Normal API.ListStreamsResponse)
listStreams API.HStreamApi{..} = hstreamApiListStreams requestDefault
listViews :: HStreamClientApi -> IO (ClientResult 'Normal API.ListViewsResponse)
listViews API.HStreamApi{..} = hstreamApiListViews requestDefault
listQueries :: HStreamClientApi -> IO (ClientResult 'Normal API.ListQueriesResponse)
listQueries API.HStreamApi{..} = hstreamApiListQueries requestDefault
listConnectors :: HStreamClientApi -> IO (ClientResult 'Normal API.ListConnectorsResponse)
listConnectors API.HStreamApi{..} = hstreamApiListConnectors requestDefault

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

insertIntoStream :: HStreamClientApi
  -> StreamName -> InsertType -> BS.ByteString
  -> IO (ClientResult 'Normal API.AppendResponse)
insertIntoStream API.HStreamApi{..} sName insertType payload = do
  timestamp <- getProtoTimestamp
  let header = case insertType of
        JsonFormat -> buildRecordHeader API.HStreamRecordHeader_FlagJSON Map.empty timestamp TL.empty
        RawFormat  -> buildRecordHeader API.HStreamRecordHeader_FlagRAW Map.empty timestamp TL.empty
      record = buildRecord header payload
  hstreamApiAppend (mkClientNormalRequest def
    { API.appendRequestStreamName = TL.fromStrict sName
    , API.appendRequestRecords    = V.singleton record
    })

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

requestDefault :: HasDefault a => ClientRequest 'Normal a b
requestDefault = mkClientNormalRequest def

requestTimeout :: Int
requestTimeout = 1000

mkClientNormalRequest :: a -> ClientRequest 'Normal a b
mkClientNormalRequest x = ClientNormalRequest x requestTimeout (MetadataMap Map.empty)

extractSelect :: [String] -> TL.Text
extractSelect = TL.pack .
  unwords . reverse . ("CHANGES;" :) .
  dropWhile ((/= "EMIT") . map toUpper) .
  reverse .
  dropWhile ((/= "SELECT") . map toUpper)

--------------------------------------------------------------------------------
newtype AdminTable a = AdminTable
  { unAdminTable :: HM.HashMap T.Text (HM.HashMap T.Text a) }
  -- Stream Name, Col Name, Value

selectAT :: AdminTable a
         -> [T.Text] -> [T.Text]
         -> AdminTable a
selectAT (AdminTable adminTable) streamNames colNames
  | streamNames == [] && colNames == [] = AdminTable adminTable
  | streamNames == [] = AdminTable $
      HM.map (HM.filterWithKey \colName _ -> colName `elem` colNames) adminTable
  | colNames == [] = AdminTable $
      (HM.filterWithKey \streamName _ -> streamName `elem` streamNames) adminTable
  | otherwise = AdminTable $
      (HM.filterWithKey \streamName _ -> streamName `elem` streamNames) $ HM.map
      (HM.filterWithKey \colName    _ -> colName    `elem`    colNames) adminTable

fmtAdminTable :: Show a => AdminTable a -> String
fmtAdminTable (AdminTable adminTable)
  | HM.size adminTable == 0 = "no result." | otherwise =
  let titles     = ["stream_id"] <> getTitles
      colSpecs   = L.replicate tableSize $ LT.column LT.expand LT.left LT.noAlign (LT.singleCutMark "...")
      tableSty   = LT.asciiS
      headerSpec = LT.titlesH titles
      rowGrps    = [LT.colsAllG LT.center $
        L.transpose . L.sortBy (compare `on` head) $ processedTable]
  in LT.tableString colSpecs tableSty headerSpec rowGrps
  where
    streamNames = HM.toList adminTable <&> fst
    rawTitles :: [T.Text] = (snd . head) (HM.toList adminTable) & map fst . HM.toList & L.nub . L.sort
    getTitles :: [String] = T.unpack <$> rawTitles
    tableSize = HM.size adminTable
    processedTable = streamNames <&> \curName ->
      let curLn  = HM.lookup curName adminTable & fromJust
          curCol = rawTitles <&> \curTitle -> fromJust $
            HM.lookup curTitle curLn
      in T.unpack curName : map show curCol

queryAllAppendInBytes, queryAllRecordBytes :: HStreamClientApi -> IO (AdminTable Double)
queryAllAppendInBytes api = queryAdminTable api "appends_in"
  ["throughput_1min", "throughput_5min", "throughput_10min"]   . V.fromList $ map (* 1000)
  [60               , 300              , 600]
queryAllRecordBytes   api = queryAdminTable api "reads"
  ["throughput_15min", "throughput_30min", "throughput_60min"] . V.fromList $ map (* 1000)
  [900               , 1800              , 3600]

queryAdminTable :: HStreamClientApi
                -> T.Text -> [T.Text] -> V.Vector Int32
                -> IO (AdminTable Double)
queryAdminTable API.HStreamApi{..} tableName methodNames intervalVec = do
  let statsRequestTimeOut  = 10
      colNames :: [T.Text] = methodNames
      statsRequest         = API.PerStreamTimeSeriesStatsAllRequest (TL.fromStrict tableName) (Just $ API.StatsIntervalVals intervalVec)
      resRequest           = ClientNormalRequest statsRequest statsRequestTimeOut (MetadataMap Map.empty)
  API.PerStreamTimeSeriesStatsAllResponse respM <- hstreamApiPerStreamTimeSeriesStatsAll resRequest >>= getServerResp
  let resp  = filter (isJust . snd) (Map.toList respM) <&> second (V.toList . API.statsDoubleValsVals . fromJust)
      lbled = (map . second) (zip colNames) resp
  pure . AdminTable . HM.fromList
    $ (map .  first) TL.toStrict
    $ (map . second) HM.fromList lbled

sqlStatsAction :: HStreamClientApi -> ([T.Text], RStatsTable, [T.Text]) -> IO ()
sqlStatsAction api (colNames, tableKind, streamNames) = do
  tableRes <- api & case tableKind of
    AppendInBytes -> queryAllAppendInBytes
    RecordBytes   -> queryAllRecordBytes
  let processedTable = selectAT tableRes streamNames colNames
  putStrLn $ fmtAdminTable processedTable
