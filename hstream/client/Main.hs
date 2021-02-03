{-# LANGUAGE DeriveAnyClass      #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module Main where

import           Conduit
import           Control.Exception        (SomeException, try)
import           Data.Aeson               (FromJSON, eitherDecode')
import qualified Data.ByteString.Char8    as BC
import qualified Data.ByteString.Lazy     as BL
import qualified Data.List                as L
import           Data.Proxy               (Proxy (..))
import           Data.Text                (pack)
import           HStream.SQL
import           HStream.Server.Type
import           Network.HTTP.Simple      (Request, getResponseBody, httpBS,
                                           httpSink, parseRequest,
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

parseConfig :: Parser ClientConfig
parseConfig =
  ClientConfig
    <$> strOption (long "url" <> metavar "string" <> showDefault <> value "http://localhost" <> short 'b' <> help "base url valur")
    <*> option auto (long "port" <> showDefault <> value 8081 <> short 'p' <> help "port value" <> metavar "INT")

def :: Settings IO
def = setComplete compE defaultSettings

compE :: CompletionFunc IO
compE = completeWord Nothing [] compword

-- should make sure there is no empty command
wordTable :: [[String]]
wordTable =
  [ ["show", "querys"],
    ["delete", "query"],
    ["delete", "query", "all"],
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
    loop :: ClientConfig -> InputT IO ()
    loop c@ClientConfig {..} = handleInterrupt ((liftIO $ putStrLn "Interrupt") >> loop c) $
      withInterrupt $ do
        minput <- getInputLine "> "
        let createRequest api = liftIO $ parseRequest (cHttpUrl ++ ":" ++ show cServerPort ++ api)
        case minput of
          Nothing -> return ()
          Just ":q" -> return ()
          Just xs -> do
            case words xs of
              ":h" : _ -> liftIO $ putStrLn helpInfo
              "show" : "querys" : _ -> createRequest "/show/querys" >>= handleReq @[TaskInfo] Proxy
              "delete" : "query" : "all" : _ -> createRequest ("/delete/query/all") >>= handleReq @Resp Proxy
              "delete" : "query" : dbid -> createRequest ("/delete/query/" ++ unwords dbid) >>= handleReq @Resp Proxy
              "replicate" : times : sql -> do
                re <- createRequest $ "/replicate/" ++ times
                handleReq @Resp Proxy $
                  setRequestBodyJSON (ReqSQL (pack $ unwords sql)) $
                    setRequestMethod "POST" re
              a : sql -> do
                let val = a : sql
                (liftIO $ try $ parseAndRefine $ pack $ unwords val) >>= \case
                  Left (err :: SomeException) -> liftIO $ putStrLn $ show err
                  Right s -> case s of
                    RQSelect _ -> do
                      re <- createRequest ("/create/stream/query")
                      liftIO $
                        handleStreamReq $
                          setRequestBodyJSON (ReqSQL (pack $ unwords val)) $
                            setRequestMethod "POST" re
                    _ -> do
                      re <- createRequest "/create/query"
                      handleReq @(Either String TaskInfo) Proxy $
                        setRequestBodyJSON (ReqSQL (pack $ unwords val)) $
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
      "  delete query taskid    delete query by id",
      "  deletet query all      delete all query",
      "  sql                    run sql"
    ]

handleReq :: forall a. (Show a, FromJSON a) => Proxy a -> Request -> InputT IO ()
handleReq _ req = do
  (liftIO $ try $ httpBS req) >>= \case
    Left (e :: SomeException) -> liftIO $ print e
    Right a -> do
      case getResponseBody a of
        "" -> liftIO $ putStrLn "invalid command"
        ot -> case eitherDecode' (BL.fromStrict ot) of
          Left e           -> liftIO $ print e
          Right (rsp :: a) -> liftIO $ pPrint rsp

handleStreamReq :: Request -> IO ()
handleStreamReq req = do
  (try $ httpSink req (\_ -> mapM_C BC.putStrLn)) >>= \case
    Left (e :: SomeException) -> print e
    Right _                   -> return ()
