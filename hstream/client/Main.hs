{-# LANGUAGE DeriveAnyClass      #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module Main where

import           Conduit
import           Control.Exception        (SomeException, try)
import           Data.Aeson               (FromJSON, ToJSON, eitherDecode')
import           Data.ByteString          (ByteString)
import qualified Data.ByteString.Lazy     as BL
import           Data.Data                (Typeable)
import qualified Data.List                as L
import           Data.Proxy               (Proxy (..))
import           Data.Text                (pack)
import           GHC.Generics             (Generic)
import           HStream.SQL
import           HStream.Server.Type
import           Network.HTTP.Simple      (Request, Response, getResponseBody,
                                           httpBS, httpSink, parseRequest,
                                           setRequestBodyJSON, setRequestMethod)
import           Options.Applicative      (Parser, auto, execParser, fullDesc,
                                           help, helper, info, long, metavar,
                                           option, progDesc, short, showDefault,
                                           strOption, value, (<**>))
import           System.Console.Haskeline (Completion, CompletionFunc, InputT,
                                           Settings, completeWord,
                                           defaultSettings, getInputLine,
                                           handleInterrupt, runInputT,
                                           setComplete, simpleCompletion,
                                           withInterrupt)
import           Text.Pretty.Simple       (pPrint)

data Config = Config
  { chttp :: String,
    cport :: Int
  }
  deriving (Show, Eq, Generic, Typeable, FromJSON, ToJSON)

parseConfig :: Parser Config
parseConfig =
  Config
    <$> strOption (long "url" <> metavar "string" <> showDefault <> value "http://localhost" <> short 'b' <> help "base url valur")
    <*> option auto (long "port" <> showDefault <> value 8081 <> short 'p' <> help "port value" <> metavar "INT")

def :: Settings IO
def = setComplete compE defaultSettings

compE :: CompletionFunc IO
compE = completeWord Nothing [] compword

-- should make sure there is no empty command
wordTable :: [[String]]
wordTable =
  [ ["show", "tasks"],
    ["query", "task"],
    ["delete", "task"],
    ["delete", "task", "all"],
    [":h"],
    [":q"]
  ]

-- for complete wordTable command
generalComplete :: [[String]] -> [String] -> [String]
generalComplete t [] = L.nub (map head t)
generalComplete t [x] = case L.nub (filter (L.isPrefixOf x) (map head t)) of
  [w]
    | x == w ->
      map (\z -> x ++ " " ++ z) (generalComplete (filter (/= []) (map tail (filter (\z -> head z == x) t))) [])
  ws -> ws
generalComplete t (x : xs) =
  --                    remove empty    remove head       filter prefix
  map (\z -> x ++ " " ++ z) (generalComplete (filter (/= []) (map tail (filter (\z -> head z == x) t))) xs)

-- for complete dbid & tbid
specificComplete :: Monad m => [String] -> m [String]
specificComplete _ = return []

compword :: Monad m => String -> m [Completion]
compword s = do
  let gs = generalComplete wordTable (words s)
  cs <- specificComplete (words s)
  return $ map simpleCompletion (gs <> cs)

main :: IO ()
main = do
  putStrLn "Start Hstream-CLI!"
  cf <- execParser $ info (parseConfig <**> helper) (fullDesc <> progDesc "start hstream-cli")
  putStrLn helpInfo
  runInputT def $ loop cf
  where
    loop :: Config -> InputT IO ()
    loop c@Config {..} = handleInterrupt ((liftIO $ putStrLn "Interrupt") >> loop c) $
      withInterrupt $ do
        minput <- getInputLine ":> "
        case minput of
          Nothing -> return ()
          Just ":q" -> return ()
          Just xs -> do
            case words xs of
              ":h" : _ -> do
                liftIO $ putStrLn helpInfo
              "show" : "tasks" : _ ->
                liftIO $ parseRequest (chttp ++ ":" ++ show cport ++ "/show/tasks") >>= handleReq @[TaskInfo] Proxy
              "query" : "task" : tbid ->
                liftIO $ parseRequest (chttp ++ ":" ++ show cport ++ "/query/task/" ++ unwords tbid) >>= handleReq @(Maybe TaskInfo) Proxy
              "delete" : "task" : "all" : _ -> do
                liftIO $ parseRequest (chttp ++ ":" ++ show cport ++ "/delete/task/all") >>= handleReq @Resp Proxy
              "delete" : "task" : dbid ->
                liftIO $ parseRequest (chttp ++ ":" ++ show cport ++ "/delete/task/" ++ unwords dbid) >>= handleReq @Resp Proxy
              a : sql -> do
                case parseAndRefine $ pack $ unwords $ a : sql of
                  Left err -> liftIO $ putStrLn $ show err
                  Right s -> case s of
                    RQSelect _ -> do
                      re <- liftIO $ parseRequest (chttp ++ ":" ++ show cport ++ "/create/stream/task")
                      liftIO $
                        handleStreamReq $
                          setRequestBodyJSON (ReqSQL (pack $ unwords $ a : sql)) $
                            setRequestMethod "POST" re
                    _ -> do
                      re <- liftIO $ parseRequest (chttp ++ ":" ++ show cport ++ "/create/task")
                      liftIO $
                        handleReq @(Either String TaskInfo) Proxy $
                          setRequestBodyJSON (ReqSQL (pack $ unwords $ a : sql)) $
                            setRequestMethod "POST" re
              [] -> return ()
            loop c

helpInfo :: String
helpInfo =
  unlines
    [ "Command ",
      "  :h                     help command",
      "  :q                     quit cli",
      "  show tasks             list all tasks",
      "  query task  taskid     query task by id",
      "  delete task taskid     delete task by id",
      "  deletet task all        delete all task",
      "  sql                    run sql"
    ]

handleReq :: forall a. (Show a, FromJSON a) => Proxy a -> Request -> IO ()
handleReq Proxy req = do
  (v :: Either SomeException ((Response ByteString))) <- try $ httpBS req
  case v of
    Left e -> print e
    Right a -> do
      case getResponseBody a of
        "" -> putStrLn "invalid command"
        ot -> case eitherDecode' (BL.fromStrict ot) of
          Left e           -> print e
          Right (rsp :: a) -> pPrint rsp

handleStreamReq :: Request -> IO ()
handleStreamReq req = do
  (v :: Either SomeException ()) <- try $ httpSink req (\_ -> mapM_C print)
  case v of
    Left e  -> print e
    Right _ -> return ()
