{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module Main where

import           Control.Concurrent               (forkFinally, myThreadId,
                                                   newMVar, readMVar,
                                                   threadDelay, throwTo)
import           Control.Exception                (finally, handle, try)
import           Control.Monad                    (forever, void, when, (>=>))
import           Control.Monad.IO.Class           (liftIO)
import           Data.Aeson                       as Aeson
import           Data.Char                        (toUpper)
import qualified Data.Char                        as Char
import qualified Data.HashMap.Strict              as HM
import qualified Data.List                        as L
import qualified Data.Map                         as M
import           Data.Maybe                       (maybeToList)
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
import qualified Options.Applicative              as O
import qualified System.Console.Haskeline         as RL
import qualified System.Console.Haskeline.History as RL
import           System.Exit                      (exitFailure)
import           System.Posix                     (Handler (Catch),
                                                   installHandler,
                                                   keyboardSignal)
import           Text.RawString.QQ                (r)

import qualified HStream.Admin.Server.Command     as Admin
import           HStream.Client.Action            (createStream,
                                                   createStreamBySelect,
                                                   dropAction, insertIntoStream,
                                                   listShards, terminateQueries)
import           HStream.Client.Execute           (execute, executeShowPlan,
                                                   execute_)
import           HStream.Client.Gadget            (describeCluster,
                                                   lookupConnector,
                                                   waitForServerToStart)
import           HStream.Client.Internal          (callDeleteSubscription,
                                                   callDeleteSubscriptionAll,
                                                   callListSubscriptions,
                                                   callStreamingFetch,
                                                   callSubscription)
import           HStream.Client.Types             (CliConnOpts (..),
                                                   Command (..),
                                                   HStreamCommand (..),
                                                   HStreamInitOpts (..),
                                                   HStreamNodes (..),
                                                   HStreamSqlContext (..),
                                                   HStreamSqlOpts (..),
                                                   commandParser)
import           HStream.Client.Utils             (mkClientNormalRequest')
import           HStream.Server.HStreamApi        (CommandPushQuery (..),
                                                   CommandQuery (..),
                                                   CommandQueryResponse (..),
                                                   DescribeClusterResponse (..),
                                                   HStreamApi (..),
                                                   ServerNode (serverNodeId),
                                                   hstreamApiClient)
import qualified HStream.Server.HStreamApi        as API
import           HStream.SQL                      (HStreamPlan (..),
                                                   RCreate (..), RSQL (..),
                                                   RStreamOptions (..),
                                                   hstreamCodegen,
                                                   parseAndRefine,
                                                   pattern ConnectorWritePlan)
import           HStream.SQL.Exception            (SomeSQLException,
                                                   formatSomeSQLException,
                                                   isEOF)
import           HStream.ThirdParty.Protobuf      (Empty (Empty))
import           HStream.Utils                    (HStreamClientApi,
                                                   SocketAddr (..),
                                                   fillWithJsonString',
                                                   formatCommandQueryResponse,
                                                   formatResult, genUnique,
                                                   mkGRPCClientConf,
                                                   serverNodeToSocketAddr,
                                                   setupSigsegvHandler)
import           System.Timeout                   (timeout)

main :: IO ()
main = runCommand =<<
  O.customExecParser (O.prefs (O.showHelpOnEmpty <> O.showHelpOnError))
                     (O.info (commandParser O.<**> O.helper)
                             (O.fullDesc <> O.header "======= HStream CLI =======")
                     )

runCommand :: HStreamCommand -> IO ()
runCommand HStreamCommand {..} =
  case cliCommand of
    HStreamSql opts   -> hstreamSQL cliConnOpts opts
    HStreamNodes opts -> hstreamNodes cliConnOpts opts
    HStreamInit opts  -> hstreamInit cliConnOpts opts

hstreamSQL :: CliConnOpts -> HStreamSqlOpts -> IO ()
hstreamSQL CliConnOpts{..} HStreamSqlOpts{_updateInterval = updateInterval, .. } = do
  let addr = SocketAddr _serverHost _serverPort
  availableServers <- newMVar []
  currentServer    <- newMVar addr
  let ctx = HStreamSqlContext {..}
  setupSigsegvHandler
  connected <- waitForServerToStart (_retryTimeout * 1000000) addr
  case connected of
    Nothing -> errorWithoutStackTrace "Connection timed out. Please check the server URI and try again."
    Just _  -> pure ()
  void $ describeCluster ctx addr
  case _execute of
    Nothing        -> showHStream *> interactiveSQLApp ctx _historyFile
    Just statement -> do
      when (Char.isSpace `all` statement) $ do putStrLn "Empty statement" *> exitFailure
      commandExec ctx statement
  where
    showHStream = putStrLn [r|
      __  _________________  _________    __  ___
     / / / / ___/_  __/ __ \/ ____/   |  /  |/  /
    / /_/ /\__ \ / / / /_/ / __/ / /| | / /|_/ /
   / __  /___/ // / / _, _/ /___/ ___ |/ /  / /
  /_/ /_//____//_/ /_/ |_/_____/_/  |_/_/  /_/
  |]

hstreamNodes :: CliConnOpts -> HStreamNodes -> IO ()
hstreamNodes connOpts HStreamNodesList =
  getNodes connOpts >>= putStrLn . formatResult . L.sort . V.toList . API.describeClusterResponseServerNodes
hstreamNodes connOpts (HStreamNodesStatus Nothing) =
  getNodes connOpts >>= putStrLn . formatResult . V.toList . API.describeClusterResponseServerNodesStatus
hstreamNodes connOpts (HStreamNodesStatus (Just sid)) =
  getNodes connOpts
  >>= putStrLn
    . formatResult
    . maybeToList . L.find (compareServerId sid)
    . V.toList . API.describeClusterResponseServerNodesStatus
  where
    compareServerId x API.ServerNodeStatus{..} =
      case serverNodeStatusNode of
        Just API.ServerNode{..} -> serverNodeId == x
        Nothing                 -> False

-- TODO: Init should have it's own rpc call
hstreamInit :: CliConnOpts -> HStreamInitOpts -> IO ()
hstreamInit CliConnOpts{..} HStreamInitOpts{..} = do
  ready <- timeout (_timeoutSec * 1000000) $
    withGRPCClient (mkGRPCClientConf $ SocketAddr _serverHost _serverPort) $ \client -> do
      api <- hstreamApiClient client
      Admin.sendAdminCommand "server init" api >>= Admin.formatCommandResponse >>= putStrLn
      loop api
  case ready of
    Just s  -> putStrLn s
    Nothing -> putStrLn "Time out waiting for cluster ready" >> exitFailure
  where
    loop api = do
      threadDelay 1000000
      resp <- Admin.sendAdminCommand "server ready" api
      case Aeson.eitherDecodeStrict (T.encodeUtf8 resp) of
        Left errmsg              -> pure $ "Decode json error: " <> errmsg
        Right (Aeson.Object obj) -> do
          let m_type = HM.lookup "type" obj
          case m_type of
            Just (Aeson.String "plain") -> pure $ fillWithJsonString' "content" obj
            _                           -> loop api
        _ -> loop api

getNodes :: CliConnOpts -> IO DescribeClusterResponse
getNodes CliConnOpts{..} =
  withGRPCClient (mkGRPCClientConf $ SocketAddr _serverHost _serverPort) $ \client -> do
    HStreamApi{..} <- hstreamApiClient client
    res <- hstreamApiDescribeCluster (mkClientNormalRequest' Empty)
    case res of
      ClientNormalResponse resp _ _ _ _ -> return resp
      ClientErrorResponse (ClientIOError (GRPCIOBadStatusCode x details))
        -> putStrLn (show x <> " Error: "  <> show (unStatusDetails details))
           >> exitFailure
      ClientErrorResponse err -> error $ "Server Error: " <> show err <> "\n"

--------------------------------------------------------------------------------

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
        node:_ -> void $ describeCluster ctx node
      threadDelay $ updateInterval * 1000 * 1000

    loop :: RL.InputT IO ()
    loop = RL.withInterrupt . RL.handleInterrupt loop $ do
      RL.getInputLine "> " >>= \case
        Nothing -> pure ()
        Just str
          | take 1 (words str)             == [":q"] -> pure ()
          | (take 1 (words str)) !! 0 !! 0 == ':'    -> liftIO (commandExec ctx str) >> loop
          | otherwise -> do
              str <- readToSQL $ T.pack str
              case str of
                Nothing  -> loop
                Just str -> do
                  RL.getHistory >>= RL.putHistory . RL.addHistoryUnlessConsecutiveDupe str
                  liftIO (commandExec ctx str)
                  loop

commandExec :: HStreamSqlContext -> String -> IO ()
commandExec ctx@HStreamSqlContext{..} xs = case words xs of
  [] -> return ()

  -- The Following commands are for testing only
  -- {
  ":sub":subId:stream:_ -> callSubscription ctx (T.pack subId) (T.pack stream)
  ":delSub":subId:_     -> callDeleteSubscription ctx (T.pack subId)
  ":delAllSubs":_       -> callDeleteSubscriptionAll ctx
  ":fetch":subId:_      -> genUnique >>= callStreamingFetch ctx V.empty (T.pack subId) . T.pack . show
  ":listSubs":_         -> callListSubscriptions ctx
  -- }

  ":h": _     -> putStrLn helpInfo
  [":help"]   -> putStr groupedHelpInfo
  ":help":x:_ -> case M.lookup (map toUpper x) helpInfos of Just infos -> putStrLn infos; Nothing -> pure ()

  (_:_)       -> liftIO $ handle (\(e :: SomeSQLException) -> putStrLn . formatSomeSQLException $ e) $ do
    rSQL <- parseAndRefine $ T.pack xs
    case rSQL of
      RQSelect{} -> runActionWithGrpc ctx (\api -> sqlStreamAction api (T.pack xs))
      RQCreate (RCreateAs stream _ rOptions) ->
        execute_ ctx $ createStreamBySelect stream (rRepFactor rOptions) xs
      rSql' -> hstreamCodegen rSql' >>= \case
        CreatePlan sName rFac
          -> execute_ ctx $ createStream sName rFac
        ShowPlan showObj
          -> executeShowPlan ctx showObj
        TerminatePlan termSel
          -> execute_ ctx $ terminateQueries termSel
        DropPlan checkIfExists dropObj
          -> execute_ ctx $ dropAction checkIfExists dropObj
        InsertPlan sName insertType payload
          -> do
            result <- execute ctx $ listShards sName
            case result of
              Just (API.ListShardsResponse shards) -> do
                let API.Shard{..}:_ = V.toList shards
                execute_ ctx $ insertIntoStream sName shardShardId insertType payload
              Nothing -> return ()
        ConnectorWritePlan name -> do
          addr <- readMVar currentServer
          lookupConnector ctx addr name >>= \case
            Nothing -> putStrLn "lookupConnector failed"
            Just node -> do
              withGRPCClient (mkGRPCClientConf (serverNodeToSocketAddr node))
                (hstreamApiClient >=> \api -> sqlAction api (T.pack xs))
        _ -> do
          addr <- readMVar currentServer
          withGRPCClient (mkGRPCClientConf addr)
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

sqlStreamAction :: HStreamClientApi -> T.Text -> IO ()
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
          putStr $ formatResult result
          action call _meta recv

sqlAction :: HStreamClientApi -> T.Text -> IO ()
sqlAction HStreamApi{..} sql = do
  let commandQuery = CommandQuery{ commandQueryStmtText = sql }
  resp <- hstreamApiExecuteQuery (ClientNormalRequest commandQuery 100 mempty)
  case resp of
    ClientNormalResponse x@CommandQueryResponse{} _meta1 _meta2 _status _details -> do
      putStr $ formatCommandQueryResponse x
    ClientErrorResponse _ -> putStr $ formatResult resp

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
  -> (HStreamClientApi -> IO b) -> IO b
runActionWithGrpc HStreamSqlContext{..} action= do
  addr <- readMVar currentServer
  withGRPCClient (mkGRPCClientConf addr)
    (hstreamApiClient >=> action)
