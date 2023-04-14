{-# LANGUAGE CPP                 #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module HStream.Client.SQL where

import           Control.Concurrent               (forkFinally, myThreadId,
                                                   readMVar, threadDelay,
                                                   throwTo)
import           Control.Exception                (SomeException, handle, try)
import           Control.Monad                    (forM_, forever, void, (>=>))
import           Control.Monad.IO.Class           (liftIO)
import           Data.Char                        (toUpper)
import qualified Data.Map                         as M
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Client    (ClientRequest (..),
                                                   ClientResult (..))
import           Network.GRPC.HighLevel.Generated (withGRPCClient)
import qualified System.Console.Haskeline         as RL
import qualified System.Console.Haskeline.History as RL
import           Text.RawString.QQ                (r)

import           HStream.Client.Action            (createConnector,
                                                   createStream,
                                                   createStreamBySelect,
                                                   createStreamBySelectWithCustomQueryName,
                                                   dropAction, executeViewQuery,
                                                   insertIntoStream, listShards,
                                                   pauseConnector, pauseQuery,
                                                   resumeConnector, resumeQuery,
                                                   retry, terminateQuery)
import           HStream.Client.Execute           (execute, executeShowPlan,
                                                   executeWithLookupResource_,
                                                   execute_, updateClusterInfo)
import           HStream.Client.Internal          (cliFetch)
import           HStream.Client.Types             (HStreamCliContext (..),
                                                   HStreamSqlContext (..),
                                                   Resource (..))
import           HStream.Client.Utils             (calculateShardId,
                                                   dropPlanToResType)
import           HStream.Server.HStreamApi        (CommandQuery (..),
                                                   CommandQueryResponse (..),
                                                   HStreamApi (..),
                                                   hstreamApiClient)
import qualified HStream.Server.HStreamApi        as API
import           HStream.SQL                      (HStreamPlan (..),
                                                   PauseObject (..),
                                                   RCreate (..), RSQL (..),
                                                   ResumeObject (..),
                                                   TerminateObject (..),
                                                   hstreamCodegen,
                                                   parseAndRefine)
#ifdef HStreamUseV2Engine
import           HStream.SQL.Codegen              (DropObject (..))
#else
import           HStream.SQL.Codegen.V1           (DropObject (..))
#endif
import qualified Data.Aeson.Text                  as J
import qualified Data.Text.Lazy                   as TL
import           HStream.SQL.Exception            (SomeSQLException,
                                                   formatSomeSQLException,
                                                   isEOF)
import           HStream.Utils                    (HStreamClientApi,
                                                   ResourceType (..),
                                                   formatCommandQueryResponse,
                                                   formatResult,
                                                   mkGRPCClientConfWithSSL,
                                                   newRandomText)

-- and this needs to be optimized. This could be done with a grpc client pool.
interactiveSQLApp :: HStreamSqlContext -> Maybe FilePath -> IO ()
interactiveSQLApp sqlCtx@HStreamSqlContext{hstreamCliContext = cliCtx@HStreamCliContext{..}, ..} historyFile = do
  putStrLn helpInfo
  tid <- myThreadId
  void $ forkFinally maintainAvailableNodes (\case Left err -> throwTo tid err; _ -> return ())
  RL.runInputT settings loop
  where
    settings :: RL.Settings IO
    settings = RL.Settings RL.noCompletion historyFile False

    maintainAvailableNodes = forever $ do
      readMVar availableServers >>= \case
        []     -> return ()
        node:_ -> void $ updateClusterInfo cliCtx node
      threadDelay $ updateInterval * 1000 * 1000

    loop :: RL.InputT IO ()
    loop = RL.withInterrupt . RL.handleInterrupt loop $ do
      RL.getInputLine "> " >>= \case
        Nothing -> pure ()
        Just str
          | null . take 1 . words $ str                  -> loop
          | take 1 (words str)                 == [":q"] -> pure ()
          | (head . head . take 1 . words) str == ':'    -> liftIO (commandExec sqlCtx str) >> loop
          | otherwise -> do
              RL.getHistory >>= RL.putHistory . RL.addHistoryUnlessConsecutiveDupe str
              str' <- readToSQL $ T.pack str
              case str' of
                Just str'' -> liftIO (handle (\(e :: SomeException) -> print e) $ commandExec sqlCtx str'')
                Nothing    -> pure ()
              loop

commandExec :: HStreamSqlContext -> String -> IO ()
commandExec HStreamSqlContext{hstreamCliContext = cliCtx@HStreamCliContext{..},..} xs = case words xs of
  [] -> return ()

  -- -- The Following commands are for testing only
  -- -- {
  -- ":sub":subId:stream:_ -> callSubscription cliCtx (T.pack subId) (T.pack stream)
  -- ":delSub":subId:_     -> callDeleteSubscription cliCtx (T.pack subId)
  -- ":delAllSubs":_       -> callDeleteSubscriptionAll cliCtx
  -- ":fetch":subId:_      -> genClientId >>= callStreamingFetch cliCtx V.empty (T.pack subId)
  -- ":listSubs":_         -> callListSubscriptions cliCtx
  -- -- }

  ":h": _     -> putStrLn helpInfo
  [":help"]   -> putStr groupedHelpInfo
  ":help":x:_ -> forM_ (M.lookup (map toUpper x) helpInfos) putStrLn

  (_:_)       -> liftIO $ handle (\(e :: SomeSQLException) -> putStrLn . formatSomeSQLException $ e) $ do
    rSQL <- parseAndRefine $ T.pack xs
    case rSQL of
      RQPushSelect{} -> cliFetch cliCtx xs
      RQCreate RCreateAs {} -> do
        qName <-  ("cli_generated_" <>) <$> newRandomText 10
        executeWithLookupResource_ cliCtx (Resource ResQuery qName) (createStreamBySelectWithCustomQueryName xs qName)
      rSql' -> hstreamCodegen rSql' >>= \case
        ShowPlan showObj      -> executeShowPlan cliCtx showObj
        DropPlan checkIfExists dropObj -> executeWithLookupResource_ cliCtx (dropPlanToResType dropObj) $ dropAction checkIfExists dropObj
        CreatePlan sName rFac -> execute_ cliCtx $ createStream sName rFac
        TerminatePlan (TQuery qName) -> executeWithLookupResource_ cliCtx (Resource ResQuery qName) $ terminateQuery qName
        InsertPlan sName insertType payload -> do
            result <- execute cliCtx $ listShards sName
            case result of
              Just (API.ListShardsResponse shards) -> do
                case calculateShardId "" (V.toList shards) of
                  Nothing  -> putStrLn "Failed to calculate shard id"
                  Just sid -> executeWithLookupResource_ cliCtx (Resource ResShard (T.pack $ show sid)) (retry retryLimit retryInterval $ insertIntoStream sName sid insertType payload)
              Nothing -> putStrLn "No shards found"
        CreateConnectorPlan cType cName cTarget _ cfg  -> do
          let cfgText = TL.toStrict (J.encodeToLazyText cfg)
          executeWithLookupResource_ cliCtx (Resource ResConnector cName) (createConnector cName cType cTarget cfgText)
        PausePlan  (PauseObjectConnector cName) -> executeWithLookupResource_ cliCtx (Resource ResConnector cName) (pauseConnector cName)
        ResumePlan (ResumeObjectConnector cName) -> executeWithLookupResource_ cliCtx (Resource ResConnector cName) (resumeConnector cName)
        PausePlan  (PauseObjectQuery qName) -> executeWithLookupResource_ cliCtx (Resource ResQuery qName) (pauseQuery qName)
        ResumePlan (ResumeObjectQuery qName) -> executeWithLookupResource_ cliCtx (Resource ResQuery qName) (resumeQuery qName)
        SelectPlan sources _ _ _ _ -> executeWithLookupResource_ cliCtx (Resource ResView (head sources)) (executeViewQuery xs)
        _ -> do
          addr <- readMVar currentServer
          withGRPCClient (HStream.Utils.mkGRPCClientConfWithSSL addr sslConfig)
            (hstreamApiClient >=> \api -> sqlAction api (T.pack xs))

readToSQL :: T.Text -> RL.InputT IO (Maybe String)
readToSQL acc = do
    x <- liftIO $ try @SomeSQLException $ parseAndRefine acc
    case x of
      Left err ->
        if isEOF err
            then do
              line <- RL.getInputLine "| "
              case line of
                Nothing   -> pure . Just $ T.unpack acc
                Just line -> readToSQL $ acc <> " " <> T.pack line
            else do
              RL.outputStrLn . formatSomeSQLException $ err
              pure Nothing
      Right _ -> pure . Just $ T.unpack acc

sqlAction :: HStream.Utils.HStreamClientApi -> T.Text -> IO ()
sqlAction HStreamApi{..} sql = do
  let commandQuery = CommandQuery{ commandQueryStmtText = sql }
  resp <- hstreamApiExecuteQuery (ClientNormalRequest commandQuery 100 mempty)
  case resp of
    ClientNormalResponse x@CommandQueryResponse{} _meta1 _meta2 _status _details -> do
      putStr $ HStream.Utils.formatCommandQueryResponse x
    ClientErrorResponse _ -> putStr $ HStream.Utils.formatResult resp

helpInfo :: String
helpInfo =
  [r|
Command
  :h                           To show these help info
  :q                           To exit command line interface
  :help [sql_operation]        To show full usage of sql statement

SQL STATEMENTS:
  To create a simplest stream:
    CREATE STREAM stream_name;

  To create a query select all fields from a stream:
    SELECT * FROM stream_name EMIT CHANGES;

  To insert values to a stream:
    INSERT INTO stream_name (field1, field2) VALUES (1, 2);
  |]

helpInfos :: M.Map String String
helpInfos = M.fromList [
  ("CREATE",[r|
  CREATE STREAM <stream_name> [AS <select_query>] [ WITH ( {stream_options} ) ];
  CREATE {SOURCE|SINK} CONNECTOR <connector_name> [IF NOT EXIST] WITH ( {connector_options} ) ;
  CREATE VIEW <stream_name> AS <select_query> ;
  |]),
  ("INSERT",[r|
  INSERT INTO <stream_name> ( {field_name} ) VALUES ( {field_value} );
  INSERT INTO <stream_name> VALUES '<json_value>';
  INSERT INTO <stream_name> VALUES "<binary_value>";
  |]),
  ("SELECT", [r|
  SELECT <* | {expression [ AS field_alias ]}>
  FROM stream_name_1
       [ join_type JOIN stream_name_2
         WITHIN (some_interval)
         ON stream_name_1.field_1 = stream_name_2.field_2 ]
  [ WHERE search_condition ]
  [ GROUP BY field_name [, window_type] ]
  EMIT CHANGES;
  |]),
  ("SHOW", [r|
  SHOW <CONNECTORS|STREAMS|QUERIES|VIEWS>;
  |]),
  ("TERMINATE", [r|
  TERMINATE <QUERY <query_id>|ALL>;
  |]),
  ("DROP", [r|
  DROP <STREAM <stream_name>|VIEW <view_name>> [IF EXISTS];
  |])
  ]

groupedHelpInfo :: String
groupedHelpInfo = ("SQL Statements\n" <> ) . unlines . map (\(x, y) -> x <> "  " <> y) . M.toList $ helpInfos

runActionWithGrpc :: HStreamCliContext
  -> (HStream.Utils.HStreamClientApi -> IO b) -> IO b
runActionWithGrpc HStreamCliContext{..} action= do
  addr <- readMVar currentServer
  withGRPCClient (HStream.Utils.mkGRPCClientConfWithSSL addr sslConfig)
    (hstreamApiClient >=> action)
