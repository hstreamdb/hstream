{-# LANGUAGE CPP                 #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module HStream.Client.SQLNew where

#ifdef HStreamEnableSchema
import           Control.Concurrent               (forkFinally, myThreadId,
                                                   readMVar, threadDelay,
                                                   throwTo)
import           Control.Exception                (SomeException, handle, try)
import           Control.Monad                    (forM_, forever, void, (>=>))
import           Control.Monad.IO.Class           (liftIO)
import qualified Data.Aeson.Text                  as J
import qualified Data.ByteString.Lazy             as BL
import           Data.Char                        (toUpper)
import qualified Data.Map                         as M
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Client    (ClientRequest (..),
                                                   ClientResult (..))
import           Network.GRPC.HighLevel.Generated (withGRPCClient)
import qualified Proto3.Suite                     as PB
import qualified System.Console.Haskeline         as RL
import qualified System.Console.Haskeline.History as RL

import           HStream.Client.Action            (createConnector,
                                                   createStream,
                                                   createStreamBySelect,
                                                   createStreamBySelectWithCustomQueryName,
                                                   dropAction, executeViewQuery,
                                                   getSchema, insertIntoStream,
                                                   insertIntoStream',
                                                   listShards, pauseConnector,
                                                   pauseQuery, registerSchema,
                                                   resumeConnector, resumeQuery,
                                                   retry, terminateQuery)
import           HStream.Client.Execute           (execute, executeShowPlan,
                                                   executeWithLookupResource,
                                                   executeWithLookupResource_,
                                                   execute_, updateClusterInfo)
import           HStream.Client.Internal          (cliFetch)
import           HStream.Client.Types             (HStreamCliContext (..),
                                                   HStreamSqlContext (..),
                                                   Resource (..))
import           HStream.Client.Utils             (calculateShardId,
                                                   dropPlanToResType)
import           HStream.Common.Types             (hashShardKey)
import           HStream.RawString                (cliSqlHelpInfo,
                                                   cliSqlHelpInfos)
import           HStream.Server.HStreamApi        (CommandQuery (..),
                                                   CommandQueryResponse (..),
                                                   HStreamApi (..),
                                                   hstreamApiClient)
import qualified HStream.Server.HStreamApi        as API
import qualified HStream.Server.MetaData.Types    as P
import           HStream.SQL                      (BoundCreate (..),
                                                   BoundSQL (..), BoundSelect,
                                                   BoundStreamOptions (..),
                                                   DropObject (..),
                                                   HStreamPlan (..),
                                                   InsertType (..),
                                                   PauseObject (..),
                                                   ResumeObject (..),
                                                   TerminateObject (..),
                                                   hstreamCodegen, parseAndBind)
import qualified HStream.SQL                      as SQL
import           HStream.SQL.Exception            (SomeSQLException,
                                                   formatSomeSQLException,
                                                   isEOF)
import           HStream.Utils                    (HStreamClientApi,
                                                   ResourceType (..),
                                                   formatCommandQueryResponse,
                                                   formatResult, getServerResp,
                                                   mkGRPCClientConfWithSSL,
                                                   newRandomText)

-- and this needs to be optimized. This could be done with a grpc client pool.
interactiveSQLApp :: HStreamSqlContext -> Maybe FilePath -> IO ()
interactiveSQLApp sqlCtx@HStreamSqlContext{hstreamCliContext = cliCtx@HStreamCliContext{..}, ..} historyFile = do
  putStrLn cliSqlHelpInfo
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
              str' <- readToSQL sqlCtx (T.pack str)
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

  ":h": _     -> putStrLn cliSqlHelpInfo
  [":help"]   -> putStr groupedHelpInfo
  ":help":x:_ -> forM_ (M.lookup (map toUpper x) cliSqlHelpInfos) putStrLn

  (_:_)       -> liftIO $ handle (\(e :: SomeSQLException) -> putStrLn . formatSomeSQLException $ e) $ do
    bSQL <- parseAndBind (T.pack xs) (cliGetSchema cliCtx)
    case bSQL of
      BoundQPushSelect{} -> cliFetch cliCtx xs
      BoundQCreate BoundCreateAs {} -> do
        qName <-  ("cli_generated_" <>) <$> newRandomText 10
        executeWithLookupResource_ cliCtx (Resource ResQuery qName) (createStreamBySelectWithCustomQueryName xs qName)
      BoundQCreate BoundCreateView {} -> do
        qName <-  ("cli_generated_" <>) <$> newRandomText 10
        executeWithLookupResource_ cliCtx (Resource ResQuery qName) (createStreamBySelectWithCustomQueryName xs qName)
      bSql' -> hstreamCodegen bSql' (cliGetSchema cliCtx) >>= \case
        ShowPlan showObj      -> executeShowPlan cliCtx showObj
        DropPlan checkIfExists dropObj -> executeWithLookupResource_ cliCtx (dropPlanToResType dropObj) $ dropAction checkIfExists dropObj
        CreatePlan sName schema rOptions -> do
          execute_ cliCtx (registerSchema schema)
          execute_ cliCtx $ createStream sName (bRepFactor rOptions) (bBacklogDuration rOptions)
        TerminatePlan (TQuery qName) -> executeWithLookupResource_ cliCtx (Resource ResQuery qName) $ terminateQuery qName
        InsertPlan sName insertType payload -> do
            result <- execute cliCtx $ listShards sName
            case result of
              Just (API.ListShardsResponse shards) -> do
                let shardKey = hashShardKey ""
                case calculateShardId shardKey (V.toList shards) of
                  Nothing  -> putStrLn "Failed to calculate shard id"
                  Just sid -> executeWithLookupResource_ cliCtx (Resource ResShard (T.pack $ show sid)) (retry retryLimit retryInterval $ insertIntoStream sName sid (insertType == JsonFormat) payload)
              Nothing -> putStrLn "No shards found"
        InsertBySelectPlan {} -> do
          qName <-  ("cli_generated_" <>) <$> newRandomText 10
          executeWithLookupResource_ cliCtx (Resource ResQuery qName) (createStreamBySelectWithCustomQueryName xs qName)
        CreateConnectorPlan cType cName cTarget _ cfg  -> do
          let cfgText = TL.toStrict (J.encodeToLazyText cfg)
          executeWithLookupResource_ cliCtx (Resource ResConnector cName) (createConnector cName cType cTarget cfgText)
        PausePlan  (PauseObjectConnector cName) -> executeWithLookupResource_ cliCtx (Resource ResConnector cName) (pauseConnector cName)
        ResumePlan (ResumeObjectConnector cName) -> executeWithLookupResource_ cliCtx (Resource ResConnector cName) (resumeConnector cName)
        PausePlan  (PauseObjectQuery qName) -> executeWithLookupResource_ cliCtx (Resource ResQuery qName) (pauseQuery qName)
        ResumePlan (ResumeObjectQuery qName) -> executeWithLookupResource_ cliCtx (Resource ResQuery qName) (resumeQuery qName)
        SelectPlan sources _ _ _ -> executeWithLookupResource_ cliCtx (Resource ResView (head sources)) (executeViewQuery xs)
        -- NOTE: EXPLAIN PLAN still uses the following path
        _ -> do
          addr <- readMVar currentServer
          withGRPCClient (HStream.Utils.mkGRPCClientConfWithSSL addr sslConfig)
            (hstreamApiClient >=> \api -> sqlAction api (T.pack xs))

readToSQL :: HStreamSqlContext -> T.Text -> RL.InputT IO (Maybe String)
readToSQL sqlCtx acc = do
    x <- liftIO $ try @SomeSQLException $ parseAndBind acc (cliGetSchema (hstreamCliContext sqlCtx))
    case x of
      Left err ->
        if isEOF err
            then do
              line <- RL.getInputLine "| "
              case line of
                Nothing   -> pure . Just $ T.unpack acc
                Just line -> readToSQL sqlCtx (acc <> " " <> T.pack line)
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

groupedHelpInfo :: String
groupedHelpInfo = ("SQL Statements\n" <> ) . unlines . map (\(x, y) -> x <> "  " <> y) . M.toList $ cliSqlHelpInfos

runActionWithGrpc :: HStreamCliContext
  -> (HStream.Utils.HStreamClientApi -> IO b) -> IO b
runActionWithGrpc HStreamCliContext{..} action= do
  addr <- readMVar currentServer
  withGRPCClient (HStream.Utils.mkGRPCClientConfWithSSL addr sslConfig)
    (hstreamApiClient >=> action)

cliGetSchema :: HStreamCliContext -> T.Text -> IO (Maybe SQL.Schema)
cliGetSchema cliCtx schemaOwner = do
  m <- execute cliCtx (getSchema schemaOwner)
  return (P.schemaToHStreamSchema <$> m)
#endif
