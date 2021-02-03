{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE LambdaCase                #-}
{-# LANGUAGE NoImplicitPrelude         #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE PackageImports            #-}
{-# LANGUAGE RecordWildCards           #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeOperators             #-}

module HStream.Server.Handler where

------------------------------------------------------------------

import           Control.Concurrent           hiding (threadDelay)
import           Data.Aeson                   (Value (..), encode)
import qualified Data.ByteString.Char8        as B
import qualified Data.ByteString.Lazy         as BL
import qualified Data.HashMap.Strict          as HM
import qualified Data.List                    as L
import qualified Data.Map                     as M
import           Data.Text                    (unpack)
import qualified Data.Text                    as T
import           Data.Time
import           Data.Time.Clock.POSIX
import           HStream.Processing.Processor
import           HStream.SQL.Codegen
import           HStream.Server.Api
import           HStream.Server.Type
import           HStream.Store
import qualified HStream.Store.Stream         as S
import qualified Prelude                      as P
import           RIO                          hiding (Handler)
import           Servant
import           Servant.Types.SourceT
import           System.Random
import           Z.Data.CBytes                (fromBytes, pack)
import           Z.Foreign

-------------------------------------------------------------------------

app :: ServerConfig -> IO Application
app ServerConfig {..} = do
  setLogDeviceDbgLevel C_DBG_CRITICAL
  s <-
    State
      <$> newIORef M.empty
      <*> newIORef M.empty
      <*> newIORef []
      <*> newIORef 0
      <*> return (pack sLogDeviceConfigPath)
      <*> mkAdminClient (AdminClientConfig $ pack sLogDeviceConfigPath)
      <*> mkProducer (ProducerConfig $ pack sLogDeviceConfigPath)
      <*> return sTopicRepFactor
      <*> return sConsumBuffSize
  _ <- async $ waitThread s
  return $ app' s

app' :: State -> Application
app' s = serve server1API $ hoistServer server1API (liftH s) server1

server1API :: Proxy ServerApi
server1API = Proxy

server1 :: ServerT ServerApi HandlerM
server1 = handleTask

handleTask ::
  HandlerM [TaskInfo]
    :<|> (ReqSQL -> HandlerM (Either String TaskInfo))
    :<|> (TaskID -> HandlerM Resp)
    :<|> (ReqSQL -> HandlerM (SourceIO RecordStream))
    :<|> (HandlerM Resp)
    :<|> (Int -> ReqSQL -> HandlerM Resp)
handleTask =
  handleShowTasks
    :<|> handleCreateTask
    :<|> handleDeleteTask
    :<|> handleCreateStreamTask
    :<|> handleDeleteTaskAll
    :<|> handleReplicateTask

handleShowTasks :: HandlerM [TaskInfo]
handleShowTasks = do
  State {..} <- ask
  v <- liftIO $ readIORef taskMap
  return $ fmap snd $ M.elems v

handleQueryTask :: TaskID -> HandlerM (Maybe TaskInfo)
handleQueryTask t = do
  State {..} <- ask
  v <- liftIO $ readIORef taskMap
  return $ fmap snd $ M.lookup t v

handleDeleteTaskAll :: HandlerM Resp
handleDeleteTaskAll = do
  State {..} <- ask
  ls <- (liftIO $ readIORef waitList)
  forM_ ls $ \w -> liftIO (cancel w)
  return $ OK "delete all querys"

handleDeleteTask :: TaskID -> HandlerM Resp
handleDeleteTask tid = do
  State {..} <- ask
  tm <- readIORef taskMap
  case M.lookup tid tm of
    Nothing           -> return $ OK "not found the query"
    Just (Nothing, _) -> return $ OK "query deleted"
    Just (Just w, _)  -> liftIO (cancel w) >> (return $ OK "delete query")

handleCreateStreamTask :: ReqSQL -> HandlerM (SourceIO RecordStream)
handleCreateStreamTask (ReqSQL seqValue) = do
  State {..} <- ask
  plan' <- liftIO $ try $ streamCodegen seqValue
  case plan' of
    Left (e :: SomeException) -> do
      return $ source [B.pack $ "streamCodegen error: " <> show e]
    Right plan -> do
      case plan of
        SelectPlan sou sink query -> do
          eAll <- liftIO $ mapM (doesTopicExists adminClient) (fmap (pack . T.unpack) sou)
          case all id eAll of
            False -> return $ source [B.pack $ "topic not exist: " ++ show sou]
            True -> do
              (liftIO $ try $ createTopics adminClient (M.fromList [(pack $ unpack sink, S.TopicAttrs topicRepFactor)])) >>= \case
                Left (e :: SomeException) -> return $ source [B.pack $ "create consumer error: " <> show e]
                Right _ -> do
                  tid <- getTaskid
                  time <- liftIO $ getCurrentTime
                  let ti = CreateTmpStream tid seqValue sou sink Starting time

                  atomicModifyIORef' taskMap (\t -> (M.insert tid (Nothing, ti) t, ()))

                  logOptions <- liftIO $ logOptionsHandle stderr True

                  let producerConfig =
                        ProducerConfig
                          { producerConfigUri = logDeviceConfigPath
                          }

                  name <- liftIO randomName
                  let consumerConfig =
                        ConsumerConfig
                          { consumerConfigUri = logDeviceConfigPath,
                            consumerName = pack name ,
                            consumerBufferSize = fromIntegral consumBufferSize,
                            consumerCheckpointUri = pack $  "/tmp/checkpoint/" ++ name,
                            consumerCheckpointRetries = 3
                          }

                  res <- liftIO $
                    async $
                      withLogFunc logOptions $ \lf -> do
                        let taskConfig =
                              TaskConfig
                                { tcMessageStoreType = LogDevice producerConfig consumerConfig,
                                  tcLogFunc = lf
                                }
                        runTask taskConfig query
                        return Finished

                  atomicModifyIORef' waitList (\ls -> (res : ls, ()))
                  atomicModifyIORef' asyncMap (\t -> (M.insert res tid t, ()))
                  atomicModifyIORef' taskMap (\t -> (M.insert tid (Just res, ti {taskState = Running}) t, ()))

                  liftIO $ do
                    cname <- randomName
                    cons <-
                      try $
                        mkConsumer
                          (ConsumerConfig logDeviceConfigPath (pack cname) (fromIntegral consumBufferSize) (pack $ "/tmp/checkpoint/" ++ cname) 3)
                          (fmap (pack . unpack) [sink])
                    case cons of
                      Left (e :: SomeException) -> return $ source [B.pack $ "create consumer error: " <> show e]
                      Right cons' ->
                        return $
                          fromAction
                            (\_ -> False)
                            $ do
                              ms <- pollMessages cons' 1 100000
                              return $ B.concat $ map (B.cons '\n' . toByteString . dataOutValue) ms
        _ -> error "Not supported"

posixTimeToMilliSeconds :: POSIXTime -> Int64
posixTimeToMilliSeconds =
  floor . (* 1000) . nominalDiffTimeToSeconds

-- return millisecond timestamp
getCurrentTimestamp :: IO Int64
getCurrentTimestamp = posixTimeToMilliSeconds <$> getPOSIXTime

handleReplicateTask :: Int -> ReqSQL -> HandlerM Resp
handleReplicateTask rs (ReqSQL seqValue) = do
  State {..} <- ask
  plan' <- liftIO $ try $ streamCodegen seqValue
  case plan' of
    Left (e :: SomeException) -> do
      return $ OK $ T.pack $ "streamCodegen error: " <> show e
    Right plan -> do
      case plan of
        InsertPlan topic bs -> do
          liftIO
            ( try $ do
                time <- getCurrentTimestamp
                sendMessages producer $
                  replicate rs $
                    ProducerRecord
                      (pack $ unpack topic)
                      (Just $ fromBytes $ fromByteString $ BL.toStrict $ encode $ HM.fromList [ ("key" :: Text, String "demoKey")])
                      (fromByteString $ BL.toStrict bs)
                      time
            )
            >>= \case
              Left (e :: SomeException) -> return $ OK $ T.pack $ "insert topic " <> show topic <> " error: " <> show e
              Right () -> do
                return $ OK "finished"
        _ -> error "Not supported"

handleCreateTask :: ReqSQL -> HandlerM (Either String TaskInfo)
handleCreateTask (ReqSQL seqValue) = do
  State {..} <- ask
  plan' <- liftIO $ try $ streamCodegen seqValue
  case plan' of
    Left (e :: SomeException) -> do
      return $ Left $ "streamCodegen error: " <> show e
    Right plan -> do
      case plan of
        CreateBySelectPlan sou sink query -> do
          eAll <- liftIO $ mapM (doesTopicExists adminClient) (fmap (pack . T.unpack) (sink : sou))
          case all id eAll of
            False -> return $ Left $ "topic not exist: " ++ show sou
            True -> do
              tid <- getTaskid
              time <- liftIO $ getCurrentTime
              let ti = CreateStream tid seqValue sou sink Starting time

              atomicModifyIORef' taskMap (\t -> (M.insert tid (Nothing, ti) t, ()))
              -----------------------------------

              logOptions <- liftIO $ logOptionsHandle stderr True

              let producerConfig =
                    ProducerConfig
                      { producerConfigUri = logDeviceConfigPath
                      }

              name <- liftIO randomName
              let consumerConfig =
                    ConsumerConfig
                      { consumerConfigUri = logDeviceConfigPath,
                        consumerName = pack name ,
                        consumerBufferSize = fromIntegral consumBufferSize,
                        consumerCheckpointUri = pack $ "/tmp/checkpoint/" ++ name,
                        consumerCheckpointRetries = 3
                      }

              res <- liftIO $
                async $
                  withLogFunc logOptions $ \lf -> do
                    let taskConfig =
                          TaskConfig
                            { tcMessageStoreType = LogDevice producerConfig consumerConfig,
                              tcLogFunc = lf
                            }
                    -----------------------------------
                    runTask taskConfig query >> return Finished

              atomicModifyIORef' waitList (\ls -> (res : ls, ()))
              atomicModifyIORef' asyncMap (\t -> (M.insert res tid t, ()))
              atomicModifyIORef' taskMap (\t -> (M.insert tid (Just res, ti {taskState = Running}) t, ()))

              return $ Right ti {taskState = Running}
        CreatePlan topic -> do
          liftIO
            (try $ createTopics adminClient (M.fromList [(pack $ unpack topic, (S.TopicAttrs topicRepFactor))]))
            >>= \case
              Left (e :: SomeException) -> return $ Left $ "create topic " <> show topic <> " error: " <> show e
              Right () -> do
                tid <- getTaskid
                time <- liftIO $ getCurrentTime
                let ti = CreateTopic tid seqValue topic Finished time
                return $ Right $ ti
        InsertPlan topic bs -> do
          liftIO
            ( try $ do
                time <- getCurrentTimestamp
                sendMessage producer $
                  ProducerRecord
                    (pack $ unpack topic)
                    (Just $ fromBytes $ fromByteString $ BL.toStrict $ encode $ HM.fromList [ ("key" :: Text, String "demoKey")])
                    (fromByteString $ BL.toStrict bs)
                    time
            )
            >>= \case
              Left (e :: SomeException) -> return $ Left $ "insert topic " <> show topic <> " error: " <> show e
              Right () -> do
                tid <- getTaskid
                time <- liftIO $ getCurrentTime
                let ti = InsertTopic tid seqValue topic Finished time
                return $ Right $ ti
        _ -> error "Not supported"

waitThread :: State -> IO ()
waitThread State {..} = do
  forever $ do
    li <- readIORef waitList
    case li of
      [] -> threadDelay 1000000
      ls -> do
        (a, r) <- waitAnyCatch ls
        case r of
          Left e -> do
            ths <- readIORef asyncMap
            tks <- readIORef taskMap
            case M.lookup a ths >>= flip M.lookup tks of
              Nothing -> error "error happened"
              Just (_, ts) -> do
                atomicModifyIORef' taskMap (\t -> (M.insert (taskid ts) (Nothing, ts {taskState = ErrorHappened $ show e}) t, ()))
          Right v -> do
            ths <- readIORef asyncMap
            tks <- readIORef taskMap
            case M.lookup a ths >>= flip M.lookup tks of
              Nothing -> error "error happened"
              Just (_, ts) -> do
                atomicModifyIORef' taskMap (\t -> (M.insert (taskid ts) (Nothing, ts {taskState = v}) t, ()))
        atomicModifyIORef' waitList (\t -> (L.delete a t, ()))
        atomicModifyIORef' asyncMap (\t -> (M.delete a t, ()))

type HandlerM = ReaderT State Handler

getTaskid :: HandlerM Int
getTaskid = do
  State {..} <- ask
  v <- liftIO $ readIORef taskIndex
  liftIO $ atomicModifyIORef' taskIndex (\i -> (i + 1, ()))
  return v

liftH :: State -> HandlerM a -> Handler a
liftH s h = runReaderT h s

randomName :: IO String
randomName = do
  mapM (\_ -> randomRIO ('A', 'z')) [1..10 :: Int]
