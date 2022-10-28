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
import           Control.Exception                (finally, handle, try)
import           Control.Monad                    (forM_, forever, void, (>=>))
import           Control.Monad.IO.Class           (liftIO)
import           Data.Char                        (toUpper)
import qualified Data.Map                         as M
import qualified Data.Text                        as T
import qualified Data.Text.Encoding               as T
import qualified Data.Text.IO                     as T
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated (ClientError (ClientIOError),
                                                   ClientRequest (..),
                                                   ClientResult (..),
                                                   GRPCIOError (GRPCIOBadStatusCode),
                                                   StatusDetails (unStatusDetails),
                                                   withGRPCClient)
import           Network.GRPC.LowLevel.Call       (clientCallCancel)
import qualified System.Console.Haskeline         as RL
import qualified System.Console.Haskeline.History as RL
import           System.Posix                     (Handler (Catch),
                                                   installHandler,
                                                   keyboardSignal)
import           Text.RawString.QQ                (r)

import           HStream.Client.Action            (createConnector,
                                                   createStream,
                                                   createStreamBySelect,
                                                   dropAction, insertIntoStream,
                                                   listShards, pauseConnector,
                                                   resumeConnector,
                                                   terminateQueries)
import           HStream.Client.Execute           (execute, executeShowPlan,
                                                   executeWithLookupResource_,
                                                   execute_, updateClusterInfo)
import           HStream.Client.Internal          (callDeleteSubscription,
                                                   callDeleteSubscriptionAll,
                                                   callListSubscriptions,
                                                   callStreamingFetch,
                                                   callSubscription)
import           HStream.Client.Types             (HStreamSqlContext (..),
                                                   ResourceType (..))
import           HStream.Client.Utils             (calculateShardId,
                                                   dropPlanToResType)
import           HStream.Server.HStreamApi        (CommandPushQuery (..),
                                                   CommandQuery (..),
                                                   CommandQueryResponse (..),
                                                   HStreamApi (..),
                                                   hstreamApiClient)
import qualified HStream.Server.HStreamApi        as API
import           HStream.SQL                      (DropObject (..),
                                                   HStreamPlan (..),
                                                   PauseObject (..),
                                                   RCreate (..), RSQL (..),
                                                   ResumeObject (..),
                                                   hstreamCodegen,
                                                   parseAndRefine)
import           HStream.SQL.Exception            (SomeSQLException,
                                                   formatSomeSQLException,
                                                   isEOF)
import           HStream.Utils                    (HStreamClientApi,
                                                   formatCommandQueryResponse,
                                                   formatResult, genUnique,
                                                   mkGRPCClientConfWithSSL,
                                                   serverNodeToSocketAddr)

-- FIXME: Currently, every new command will create a new connection to a server,
-- and this needs to be optimized. This could be done with a grpc client pool.
interactiveSQLApp :: HStreamSqlContext -> Maybe FilePath -> IO ()
interactiveSQLApp ctx@HStreamSqlContext{..} historyFile = do
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
        node:_ -> void $ updateClusterInfo ctx node
      threadDelay $ updateInterval * 1000 * 1000

    loop :: RL.InputT IO ()
    loop = RL.withInterrupt . RL.handleInterrupt loop $ do
      RL.getInputLine "> " >>= \case
        Nothing -> pure ()
        Just str
          | null . take 1 . words $ str                  -> loop
          | take 1 (words str)                 == [":q"] -> pure ()
          | (head . head . take 1 . words) str == ':'    -> liftIO (commandExec ctx str) >> loop
          | otherwise -> do
              str' <- readToSQL $ T.pack str
              case str' of
                Nothing  -> loop
                Just str'' -> do
                  RL.getHistory >>= RL.putHistory . RL.addHistoryUnlessConsecutiveDupe str
                  liftIO (commandExec ctx str'')
                  loop

commandExec :: HStreamSqlContext -> String -> IO ()
commandExec ctx@HStreamSqlContext{..} xs = case words xs of
  [] -> return ()

  -- The Following commands are for testing only
  -- {
  ":sub":subId:stream:_ -> callSubscription ctx (T.pack subId) (T.pack stream)
  ":delSub":subId:_     -> callDeleteSubscription ctx (T.pack subId)
  ":delAllSubs":_       -> callDeleteSubscriptionAll ctx
  ":fetch":subId:_      -> HStream.Utils.genUnique >>= callStreamingFetch ctx V.empty (T.pack subId) . T.pack . show
  ":listSubs":_         -> callListSubscriptions ctx
  -- }

  ":h": _     -> putStrLn helpInfo
  [":help"]   -> putStr groupedHelpInfo
  ":help":x:_ -> forM_ (M.lookup (map toUpper x) helpInfos) putStrLn

  (_:_)       -> liftIO $ handle (\(e :: SomeSQLException) -> putStrLn . formatSomeSQLException $ e) $ do
    rSQL <- parseAndRefine $ T.pack xs
    case rSQL of
      RQPushSelect{} -> runActionWithGrpc ctx (\api -> sqlStreamAction api (T.pack xs))
      RQCreate RCreateAs {} ->
        execute_ ctx $ createStreamBySelect xs
      rSql' -> hstreamCodegen rSql' >>= \case
        ShowPlan showObj      -> executeShowPlan ctx showObj
        -- FIXME: add lookup after supporting lookup stream and lookup view
        DropPlan checkIfExists dropObj@DStream{} -> execute_ ctx $ dropAction checkIfExists dropObj
        DropPlan checkIfExists dropObj@DView{} -> execute_ ctx $ dropAction checkIfExists dropObj
        DropPlan checkIfExists dropObj -> executeWithLookupResource_ ctx (dropPlanToResType dropObj) $ dropAction checkIfExists dropObj
        CreatePlan sName rFac -> execute_ ctx $ createStream sName rFac
        -- FIXME: requires lookup
        TerminatePlan termSel -> execute_ ctx $ terminateQueries termSel
        InsertPlan sName insertType payload -> do
            result <- execute ctx $ listShards sName
            case result of
              Just (API.ListShardsResponse shards) -> do
                case calculateShardId "" (V.toList shards) of
                  Nothing  -> putStrLn "Failed to calculate shard id"
                  Just sid -> executeWithLookupResource_ ctx (ResShard sid) (insertIntoStream sName sid insertType payload)
              Nothing -> putStrLn "No shards found"
        CreateConnectorPlan _ cName _ _ _  -> executeWithLookupResource_ ctx (ResConnector cName) (createConnector xs)
        PausePlan  (PauseObjectConnector cName) -> executeWithLookupResource_ ctx (ResConnector cName) (pauseConnector cName)
        ResumePlan (ResumeObjectConnector cName) -> executeWithLookupResource_ ctx (ResConnector cName) (resumeConnector cName)
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

sqlStreamAction :: HStream.Utils.HStreamClientApi -> T.Text -> IO ()
sqlStreamAction HStreamApi{..} sql = do
  let commandPushQuery = CommandPushQuery{ commandPushQueryQueryText = sql }
  resp <- hstreamApiExecutePushQuery (ClientReaderRequest commandPushQuery 10000000 mempty action)
  case resp of
    ClientReaderResponse {} -> return ()
    ClientErrorResponse (ClientIOError (GRPCIOBadStatusCode _ details)) -> do
      T.putStrLn $ "Error: " <> T.decodeUtf8 (unStatusDetails details)
    ClientErrorResponse err -> do
      putStrLn $ "Error: " <> (case err of ClientIOError ge -> show ge; _ -> show err) <> ", please try a different server node"
  where
    action call _meta recv = do
      msg <- withInterrupt (clientCallCancel call) recv
      case msg of
        Left err            -> print err
        Right Nothing       -> putStrLn ("\x1b[32m" <> "Terminated" <> "\x1b[0m")
        Right (Just result) -> do
          putStr $ HStream.Utils.formatResult result
          action call _meta recv

sqlAction :: HStream.Utils.HStreamClientApi -> T.Text -> IO ()
sqlAction HStreamApi{..} sql = do
  let commandQuery = CommandQuery{ commandQueryStmtText = sql }
  resp <- hstreamApiExecuteQuery (ClientNormalRequest commandQuery 100 mempty)
  case resp of
    ClientNormalResponse x@CommandQueryResponse{} _meta1 _meta2 _status _details -> do
      putStr $ HStream.Utils.formatCommandQueryResponse x
    ClientErrorResponse _ -> putStr $ HStream.Utils.formatResult resp

withInterrupt :: IO () -> IO a -> IO a
withInterrupt interruptHandle act = do
  old_handler <- installHandler keyboardSignal (Catch interruptHandle) Nothing
  act `finally` installHandler keyboardSignal old_handler Nothing

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

runActionWithGrpc :: HStreamSqlContext
  -> (HStream.Utils.HStreamClientApi -> IO b) -> IO b
runActionWithGrpc HStreamSqlContext{..} action= do
  addr <- readMVar currentServer
  withGRPCClient (HStream.Utils.mkGRPCClientConfWithSSL addr sslConfig)
    (hstreamApiClient >=> action)
