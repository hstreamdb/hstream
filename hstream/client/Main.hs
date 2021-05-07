{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import           Control.Exception                (SomeException, finally, try)
import           Control.Monad.IO.Class           (liftIO)
import           Data.ByteString                  (ByteString)
import qualified Data.List                        as L
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import           HStream.Format                   (renderJSONObjectToTable,
                                                   renderTable)
import           HStream.SQL
import           HStream.Server.HStreamApi
import           HStream.Server.Utils             (structToJsonObject)
import           Network.GRPC.HighLevel.Generated
import           Network.GRPC.LowLevel.Call       (clientCallCancel)
import           Options.Applicative
import qualified System.Console.Haskeline         as H
import           System.Posix                     (Handler (Catch),
                                                   installHandler,
                                                   keyboardSignal)
import           Text.RawString.QQ                (r)

helpInfo :: String
helpInfo =
  unlines
    [ "Command ",
      "  :h                        help command",
      "  :q                        quit cli",
      "  show queries              list all queries",
      "  terminate query <taskid>  terminate query by id",
      "  terminate query all       terminate all queries",
      "  <sql>                     run sql"
    ]

def :: H.Settings IO
def = H.setComplete compE H.defaultSettings

compE :: H.CompletionFunc IO
compE = H.completeWord Nothing [] compword

wordTable :: [[String]]
wordTable =
  [ ["show", "queries"],
    ["terminate", "query"],
    ["terminate", "query", "all"],
    [":h"],
    [":q"]
  ]

generalComplete :: [[String]] -> [String] -> [String]
generalComplete t [] = L.nub (map head t)
generalComplete t [x] = case L.nub (filter (L.isPrefixOf x) (map head t)) of
  [w]
    | x == w ->
      map (\z -> x ++ " " ++ z) (generalComplete (filter (/= []) (map tail (filter (\z -> head z == x) t))) [])
  ws -> ws
generalComplete t (x : xs) =
  map (\z -> x ++ " " ++ z) (generalComplete (filter (/= []) (map tail (filter (\z -> head z == x) t))) xs)

specificComplete :: Monad m => [String] -> m [String]
specificComplete _ = return []

compword :: Monad m => String -> m [H.Completion]
compword s = do
  let gs = generalComplete wordTable (words s)
  cs <- specificComplete (words s)
  return $ map H.simpleCompletion (gs <> cs)

--------------------------------------------------------------------------------
data UserConfig = UserConfig
  { _serverHost :: ByteString
  , _serverPort :: Int
  }

parseConfig :: Parser UserConfig
parseConfig =
  UserConfig
    <$> strOption (long "host" <> metavar "HOST" <> showDefault <> value "127.0.0.1" <> help "server host value")
    <*> option auto (long "port" <> metavar "INT" <> showDefault <> value 50051 <> short 'p' <> help "server port value")

main :: IO ()
main = do
  UserConfig{..} <- execParser $ info (parseConfig <**> helper) (fullDesc <> progDesc "HStream-Client")
  putStrLn [r|
      __  _________________  _________    __  ___
     / / / / ___/_  __/ __ \/ ____/   |  /  |/  /
    / /_/ /\__ \ / / / /_/ / __/ / /| | / /|_/ /
   / __  /___/ // / / _, _/ /___/ ___ |/ /  / /
  /_/ /_//____//_/ /_/ |_/_____/_/  |_/_/  /_/
  |]
  let clientConfig = ClientConfig { clientServerHost = Host _serverHost
                                  , clientServerPort = Port . fromIntegral $ _serverPort
                                  , clientArgs = []
                                  , clientSSLConfig = Nothing
                                  , clientAuthority = Nothing
                                  }
   in app clientConfig

app :: ClientConfig -> IO ()
app clientConfig = do
  putStrLn helpInfo
  H.runInputT def loop
  where
    loop :: H.InputT IO ()
    loop = do
        input <- H.getInputLine "> "
        case input of
          Nothing   -> return ()
          Just ":q" -> return ()
          Just xs   -> do
            case words xs of
              ":h" : _                          -> liftIO $ putStrLn helpInfo
              "show" : "queries" : _            -> undefined
              "terminate" : "query" : "all" : _ -> undefined
              "terminate" : "query" : dbid      -> undefined
              val@(_ : _)                       -> do
                let sql = T.pack (unwords val)
                (liftIO . try . parseAndRefine $ sql) >>= \case
                  Left (e :: SomeException) -> liftIO . print $ e
                  Right rsql                -> case rsql of
                    RQSelect _ -> liftIO $ sqlStreamAction clientConfig (TL.fromStrict sql)
                    _          -> liftIO $ sqlAction       clientConfig (TL.fromStrict sql)
              [] -> return ()
            loop

sqlStreamAction :: ClientConfig -> TL.Text -> IO ()
sqlStreamAction clientConfig sql = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let commandPushQuery = CommandPushQuery{ commandPushQueryQueryText = sql }
  ClientReaderResponse _meta _status _details <-
    hstreamApiExecutePushQuery (ClientReaderRequest commandPushQuery 10000 [] action)
  return ()
  where
    action call _meta recv =
      let go = do
            msg <- withInterrupt (clientCallCancel call) recv
            case msg of
              Left err            -> print err
              Right Nothing       -> putStrLn "terminated"
              Right (Just result) -> do
                let object = structToJsonObject result
                putStrLn (unlines $ renderTable $ renderJSONObjectToTable object)
                go
      in go

sqlAction :: ClientConfig -> TL.Text -> IO ()
sqlAction clientConfig sql = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let commandQuery = CommandQuery{ commandQueryStmtText = sql }
  resp <- hstreamApiExecuteQuery (ClientNormalRequest commandQuery 100 [])
  case resp of
    ClientNormalResponse x@CommandQueryResponse{..} _meta1 _meta2 _status _details -> do
      print x
    ClientErrorResponse clientError -> print $ "client error: " <> show clientError

withInterrupt :: IO () -> IO a -> IO a
withInterrupt handle act = do
  old_handler <- installHandler keyboardSignal (Catch handle) Nothing
  act `finally` installHandler keyboardSignal old_handler Nothing
