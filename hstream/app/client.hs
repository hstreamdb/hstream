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
import           Control.Monad.IO.Class           (liftIO)
import           Data.ByteString                  (ByteString)
import qualified Data.ByteString.Char8            as BSC
import           Data.Char                        (toUpper)
import qualified Data.Map                         as M
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import           Network.GRPC.HighLevel.Generated
import           Network.GRPC.LowLevel.Call       (clientCallCancel)
import qualified Options.Applicative              as O
import           System.Console.ANSI              (getTerminalSize)
import qualified System.Console.Haskeline         as H
import           System.Posix                     (Handler (Catch),
                                                   installHandler,
                                                   keyboardSignal)
import           Text.RawString.QQ                (r)

import           Data.Functor                     ((<&>))
import           HStream.Client.Action
import           HStream.Client.Gadget
import qualified HStream.Logger                   as Log
import           HStream.SQL
import           HStream.SQL.Exception            (SomeSQLException,
                                                   formatSomeSQLException)
import           HStream.Server.HStreamApi
import           HStream.Utils                    (Format, HStreamClientApi,
                                                   formatCommandQueryResponse,
                                                   formatResult,
                                                   setupSigsegvHandler)

data UserConfig = UserConfig
  { _serverHost :: ByteString
  , _serverPort :: Int
  , _clientId   :: String
  }

parseConfig :: O.Parser UserConfig
parseConfig =
  UserConfig
    <$> O.strOption (O.long "host" <> O.metavar "HOST" <> O.showDefault <> O.value "127.0.0.1" <> O.help "server host value")
    <*> O.option O.auto (O.long "port" <> O.metavar "INT" <> O.showDefault <> O.value 6570 <> O.short 'p' <> O.help "server port value")
    <*> O.strOption (O.long "client-id" <> O.metavar "ID" <> O.help "unique id for the client")

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
  let _serverUri = _serverHost <> ":" <> (BSC.pack . show $ _serverPort)
  available <- newMVar []
  current <- newMVar _serverUri
  producers_ <- newMVar mempty
  let ctx = ClientContext
        { availableServers = available
        , currentServer    = current
        , producers        = producers_
        , clientId         = _clientId
        }
      clientConfig = ClientConfig { clientServerHost = Host _serverHost
                                  , clientServerPort = Port _serverPort
                                  , clientArgs       = []
                                  , clientSSLConfig  = Nothing
                                  , clientAuthority  = Nothing
                                  }
  setupSigsegvHandler
  app ctx clientConfig
  m_uri <- connect ctx _serverUri ByLoad
  case m_uri of
    Just _  -> app ctx clientConfig
    Nothing -> do
      Log.e "Connection timed out. Please check the server URI and try again."

app :: ClientContext -> ClientConfig -> IO ()
app ctx config@ClientConfig{..} = withGRPCClient config $ \client -> do
  api@HStreamApi{..} <- hstreamApiClient client
  putStrLn helpInfo
  H.runInputT H.defaultSettings (loop api)
  where
    loop :: HStreamClientApi -> H.InputT IO ()
    loop api = H.getInputLine "> " >>= \case
      Nothing   -> return ()
      Just str
        | take 1 (words str) == [":q"] -> return ()
        | take 3 (map toUpper <$> words str) == ["USE", "ADMIN", ";"] ||
          take 2 (map toUpper <$> words str) == ["USE", "ADMIN;"]     ->
            loopAdmin api
        | otherwise -> liftIO (commandExec ctx api str) >> loop api
    loopAdmin api = H.getInputLine "ADMIN> " >>= \case
      Nothing   -> return ()
      Just str
        | take 1 (words str) == [":q"] -> return ()
        | take 3 (map toUpper <$> words str) == ["USE", "STREAM", ";"] ||
          take 2 (map toUpper <$> words str) == ["USE", "STREAM;"]     ->
            loop api
        | otherwise -> liftIO (commandExec ctx api $ "ADMIN:: " <> str) >> loopAdmin api

commandExec :: ClientContext -> HStreamClientApi -> String -> IO ()
commandExec ctx api xs = case words xs of
  ":h": _     -> putStrLn helpInfo
  [":help"]   -> putStr groupedHelpInfo
  ":help":x:_ -> case M.lookup (map toUpper x) helpInfos of Just infos -> putStrLn infos; Nothing -> pure ()
  xs'@(_:_)   -> liftIO $ handle (\(e :: SomeSQLException) -> putStrLn . formatSomeSQLException $ e) $ do
    (parseAndRefine . T.pack) xs >>= \case
      RQSelect{} -> sqlStreamAction api (TL.pack xs)
      RQCreate (RCreateAs stream _ rOptions) ->
        createStreamBySelect api (TL.fromStrict stream) (rRepFactor rOptions) xs'
        >>= printResult
      RQSelectStats (RSelectStats colNames tableKind streamNames) ->
        sqlStatsAction api (colNames, tableKind, streamNames)
      rSql' -> hstreamCodegen rSql' >>= \case
        CreatePlan sName rFac
          -> createStream api sName rFac >>= printResult
        ShowPlan showObj
          -> executeShowPlan api showObj
        TerminatePlan termSel
          -> terminateQueries api termSel >>= printResult
        DropPlan checkIfExists dropObj
          -> dropAction api checkIfExists dropObj >>= printResult
        InsertPlan sName insertType payload
          -> insertIntoStream ctx sName insertType payload >>= printResult
        _ -> sqlAction api (TL.pack xs)

  [] -> return ()

sqlStreamAction :: HStreamClientApi -> TL.Text -> IO ()
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
          width <- getTerminalSize
          putStr $ formatResult (case width of Nothing -> 80; Just (_, w) -> w) result
          action call _meta recv

sqlAction :: HStreamClientApi -> TL.Text -> IO ()
sqlAction HStreamApi{..} sql = do
  let commandQuery = CommandQuery{ commandQueryStmtText = sql }
  resp <- hstreamApiExecuteQuery (ClientNormalRequest commandQuery 100 [])
  case resp of
    ClientNormalResponse x@CommandQueryResponse{} _meta1 _meta2 _status _details -> do
      width <- getTerminalSize
      putStr $ formatCommandQueryResponse (case width of Nothing -> 80; Just (_, w) -> w) x
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

executeShowPlan :: HStreamClientApi -> ShowObject -> IO ()
executeShowPlan api showObject =
  case showObject of
    SStreams    -> listStreams    api >>= printResult
    SViews      -> listViews      api >>= printResult
    SQueries    -> listQueries    api >>= printResult
    SConnectors -> listConnectors api >>= printResult

printResult :: Format a => a -> IO ()
printResult resp = getWidth >>= putStr . flip formatResult resp

getWidth :: IO Int
getWidth = getTerminalSize <&> (\case Nothing -> 80; Just (_, w) -> w)
