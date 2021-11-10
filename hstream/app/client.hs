{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import           Control.Concurrent
import           Control.Exception                (finally, handle)
import           Control.Monad
import           Control.Monad.IO.Class           (liftIO)
import           Data.Char                        (toLower, toUpper)
import qualified Data.Map                         as M
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated
import           Network.GRPC.LowLevel.Call       (clientCallCancel)
import           Network.Socket                   (PortNumber)
import qualified Options.Applicative              as O
import qualified System.Console.Haskeline         as H
import           System.Posix                     (Handler (Catch),
                                                   installHandler,
                                                   keyboardSignal)
import           Text.RawString.QQ                (r)
import qualified Z.Data.CBytes                    as CB
import           Z.IO.Network.SocketAddr          (ipv4)

import           HStream.Client.Action
import           HStream.Client.Execute           (execute, executeInsert,
                                                   executeShowPlan)
import           HStream.Client.Gadget
import           HStream.Client.Internal
import           HStream.Client.Type              (ClientContext (..))
import           HStream.Client.Utils             (mkGRPCClientConf)
import qualified HStream.Common.Query             as Query
import qualified HStream.Logger                   as Log
import           HStream.SQL
import           HStream.SQL.Exception            (SomeSQLException,
                                                   formatSomeSQLException)
import           HStream.Server.HStreamApi
import           HStream.Store.Logger
import           HStream.Utils                    (HStreamClientApi,
                                                   formatCommandQueryResponse,
                                                   formatResult,
                                                   setupSigsegvHandler)

data UserConfig = UserConfig
  { _serverHost                     :: String
  , _serverPort                     :: PortNumber
  , _clientId                       :: String
  , _availableServersUpdateInterval :: Int
  }

parseConfig :: O.Parser UserConfig
parseConfig =
  UserConfig
    <$> O.strOption (O.long "host" <> O.metavar "HOST" <> O.showDefault <> O.value "127.0.0.1" <> O.help "server host value")
    <*> O.option O.auto (O.long "port" <> O.metavar "INT" <> O.showDefault <> O.value 6570 <> O.short 'p' <> O.help "server port value")
    <*> O.strOption (O.long "client-id" <> O.metavar "ID" <> O.help "unique id for the client")
    <*> O.option O.auto (O.long "update-interval" <> O.metavar "INT" <> O.showDefault <> O.value 30 <> O.help "interval to update available servers in second")

main :: IO ()
main = do
  UserConfig{..} <- O.execParser $ O.info (parseConfig O.<**> O.helper) (O.fullDesc <> O.progDesc "HStream-Client")
  putStrLn [r|
      __  _________________  _________    __  ___
     / / / / ___/_  __/ __ \/ ____/   |  /  |/  /
    / /_/ /\__ \ / / / /_/ / __/ / /| | / /|_/ /
   / __  /___/ // / / _, _/ /___/ ___ |/ /  / /
  /_/ /_//____//_/ /_/ |_/_____/_/  |_/_/  /_/
  |]
  let _addr = ipv4 (CB.pack _serverHost) (fromIntegral _serverPort)
  available <- newMVar []
  current <- newMVar _addr
  producers_ <- newMVar mempty
  let ctx = ClientContext { cctxServerHost = _serverHost
                          , cctxServerPort = _serverPort
                          , availableServers = available
                          , currentServer    = current
                          , producers        = producers_
                          , clientId         = _clientId
                          , availableServersUpdateInterval = _availableServersUpdateInterval
                          }
  setupSigsegvHandler
  setLogDeviceDbgLevel C_DBG_ERROR
  m_desc <- describeCluster ctx _addr
  case m_desc of
    Nothing -> Log.e "Connection timed out. Please check the server URI and try again."
    Just _  -> app ctx

-- FIXME: Currently, every new command will create a new connection to a server,
-- and this needs to be optimized. This could be done with a grpc client pool.
app :: ClientContext -> IO ()
app ctx@ClientContext{..} = do
  query <- Query.newHStreamQuery (CB.pack $ cctxServerHost <> ":" <> show cctxServerPort)
  putStrLn helpInfo
  void $ forkIO maintainAvailableNodes
  H.runInputT H.defaultSettings (loop query)
  where
    maintainAvailableNodes = forever $ do
      readMVar availableServers >>= \case
        []     -> return ()
        node:_ -> void $ describeCluster ctx node
      threadDelay $ availableServersUpdateInterval * 1000 * 1000
    loop :: Query.HStreamQuery -> H.InputT IO ()
    loop query = H.getInputLine "> " >>= \case
      Nothing   -> return ()
      Just str
        | take 1 (words str) == [":q"] -> return ()
        | take 3 (map toUpper <$> words str) == ["USE", "ADMIN", ";"] ||
          take 2 (map toUpper <$> words str) == ["USE", "ADMIN;"]     ->
            loopAdmin query
        | otherwise -> liftIO (commandExec ctx str) >> loop query
    loopAdmin query = H.getInputLine "ADMIN> " >>= \case
      Nothing   -> return ()
      Just str
        | take 1 (words str) == [":q"] -> return ()
        | take 3 (map toUpper <$> words str) == ["USE", "STREAM", ";"] ||
          take 2 (map toUpper <$> words str) == ["USE", "STREAM;"]     -> loop query
        | otherwise -> liftIO (adminCommandExec query str) >> loopAdmin query

commandExec :: ClientContext -> String -> IO ()
commandExec ctx@ClientContext{..} xs = case words xs of
  [] -> return ()

  -- The Following commands are for testing only
  -- {
  ":sub":subId:stream:_ -> callSubscription ctx (T.pack subId) (T.pack stream)
  ":delSub":subId:_     -> callDeleteSubscription ctx (T.pack subId)
  ":fetch":subId:_      -> callStreamingFetch ctx V.empty (T.pack subId) (T.pack clientId)
  ":listSubs":_         -> callListSubscriptions ctx
  -- }

  ":h": _     -> putStrLn helpInfo
  [":help"]   -> putStr groupedHelpInfo
  ":help":x:_ -> case M.lookup (map toUpper x) helpInfos of Just infos -> putStrLn infos; Nothing -> pure ()
  xs'@(_:_)   -> liftIO $ handle (\(e :: SomeSQLException) -> putStrLn . formatSomeSQLException $ e) $ do
    (parseAndRefine . T.pack) xs >>= \case
      RQSelect{} -> runActionWithGrpc ctx (\api -> sqlStreamAction api (T.pack xs))
      RQCreate (RCreateAs stream _ rOptions) ->
        execute ctx $ createStreamBySelect stream (rRepFactor rOptions) xs'
      rSql' -> hstreamCodegen rSql' >>= \case
        CreatePlan sName rFac
          -> execute ctx $ createStream sName rFac
        ShowPlan showObj
          -> executeShowPlan ctx showObj
        TerminatePlan termSel
          -> execute ctx $ terminateQueries termSel
        DropPlan checkIfExists dropObj
          -> execute ctx $ dropAction checkIfExists dropObj
        InsertPlan sName insertType payload
          -> executeInsert ctx sName $ insertIntoStream sName insertType payload
        _ -> do
          addr <- readMVar currentServer
          withGRPCClient (mkGRPCClientConf addr)
            (hstreamApiClient >=> \api -> sqlAction api (T.pack xs))

adminCommandExec :: Query.HStreamQuery -> String -> IO ()
adminCommandExec q str =
  case words (map toLower str) of
    [] -> return ()
    ["show", "tables"]      -> putStrLn =<< Query.showTables q
    ["show", "tables", ";"] -> putStrLn =<< Query.showTables q
    ["show", "tables;"]     -> putStrLn =<< Query.showTables q
    ["describe", name_]     -> if last name_ == ';'
                                  then putStrLn =<< Query.showTableColumns q (CB.pack $ init name_)
                                  else putStrLn =<< Query.showTableColumns q (CB.pack name_)
    ["describe", name, ";"] -> putStrLn =<< Query.showTableColumns q (CB.pack name)
    "select" : _ -> either putStrLn (mapM_ putStrLn) =<< Query.runQuery q (CB.pack str)
    _            -> putStrLn $ "Unknown statement: " <> str

sqlStreamAction :: HStreamClientApi -> T.Text -> IO ()
sqlStreamAction HStreamApi{..} sql = do
  let commandPushQuery = CommandPushQuery{ commandPushQueryQueryText = sql }
  ClientReaderResponse _meta _status _details <-
    hstreamApiExecutePushQuery (ClientReaderRequest commandPushQuery 10000000 [] action)
  return ()
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
  resp <- hstreamApiExecuteQuery (ClientNormalRequest commandQuery 100 [])
  case resp of
    ClientNormalResponse x@CommandQueryResponse{} _meta1 _meta2 _status _details -> do
      putStr $ formatCommandQueryResponse x
    ClientErrorResponse clientError -> putStrLn $ "Client Error: " <> show clientError

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
  CREATE {SOURCE|SINK} CONNECTOR <stream_name> [IF NOT EXIST] WITH ( {connector_options} ) ;
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

runActionWithGrpc :: ClientContext
  -> (HStreamClientApi -> IO b) -> IO b
runActionWithGrpc ClientContext{..} action= do
  addr <- readMVar currentServer
  withGRPCClient (mkGRPCClientConf addr)
    (hstreamApiClient >=> action)
