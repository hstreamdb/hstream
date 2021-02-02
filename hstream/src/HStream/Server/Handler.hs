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

import qualified Data.ByteString.Lazy         as BL
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
import           Z.Data.CBytes                (pack)
import           Z.Foreign

-------------------------------------------------------------------------

app :: String -> IO Application
app cpath = do
  s <-
    State
      <$> newIORef M.empty
      <*> newIORef M.empty
      <*> newIORef []
      <*> newIORef 0
      <*> return (pack cpath)
      <*> mkAdminClient (AdminClientConfig $ pack cpath)
      <*> (mkProducer $ ProducerConfig $ pack cpath)
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
    :<|> (TaskID -> HandlerM (Maybe TaskInfo))
    :<|> (TaskID -> HandlerM Resp)
    :<|> (ReqSQL -> HandlerM (SourceIO [RecordVal]))
    :<|> (HandlerM Resp)
handleTask =
  handleShowTasks
    :<|> handleCreateTask
    :<|> handleQueryTask
    :<|> handleDeleteTask
    :<|> handleCreateStreamTask
    :<|> handleDeleteTaskAll

handleShowTasks :: HandlerM [TaskInfo]
handleShowTasks = do
  State {..} <- ask
  v <- liftIO $ readIORef tasks
  return $ M.elems v

handleQueryTask :: TaskID -> HandlerM (Maybe TaskInfo)
handleQueryTask t = do
  State {..} <- ask
  v <- liftIO $ readIORef tasks
  return $ M.lookup t v

handleDeleteTaskAll :: HandlerM Resp
handleDeleteTaskAll = do
  State {..} <- ask
  ls <- (liftIO $ readIORef waits)
  forM_ ls $ \w -> liftIO (cancel w)
  return $ OK "delete all task"

handleDeleteTask :: TaskID -> HandlerM Resp
handleDeleteTask tid = do
  State {..} <- ask
  ls <- M.toList <$> readIORef thids
  case filter ((== tid) . snd) ls of
    []       -> return $ OK "not found the task"
    [(w, _)] -> liftIO (cancel w) >> (return $ OK "delete the task")
    _        -> return $ OK "strange happened"

handleCreateStreamTask :: ReqSQL -> HandlerM (SourceIO [RecordVal])
handleCreateStreamTask (ReqSQL seqValue) = do
  State {..} <- ask
  plan' <- liftIO $ try $ streamCodegen seqValue
  case plan' of
    Left (e :: SomeException) -> do
      return $ error $ show e
    Right plan -> do
      case plan of
        SelectPlan sou sink task ->
          do
            -----------------------------------
            tid <- getTaskid
            time <- liftIO $ getCurrentTime

            let ti = CreateTmpStream tid seqValue sou sink Starting time
            atomicModifyIORef' tasks (\t -> (M.insert tid ti t, ()))
            -----------------------------------
            mockStore <- liftIO $ mkMockTopicStore

            logOptions <- liftIO $ logOptionsHandle stderr True
            res <- liftIO $
              withLogFunc logOptions $ \lf -> do
                let taskConfig =
                      TaskConfig
                        { tcMessageStoreType = Mock mockStore,
                          tcLogFunc = lf
                        }
                -----------------------------------
                async $ runTask taskConfig task >> return Finished
            -----------------------------------
            atomicModifyIORef' waits (\ls -> (res : ls, ()))
            atomicModifyIORef' thids (\t -> (M.insert res tid t, ()))
            atomicModifyIORef' tasks (\t -> (M.insert tid ti {taskState = Running} t, ()))

            -----------------------------------

            cons <-
              liftIO $ do
                mkConsumer
                  ( ConsumerConfig
                      lpath
                      "consumer"
                      4096
                      "consumer"
                      10
                  )
                  (fmap (pack . unpack) sou)

            return $
              fromAction
                (\_ -> False)
                $ do
                  ms1 <- pollMessages cons 1 1000000
                  return $ map (RecordVal . T.pack . show) ms1
        _ -> error "Not supported"

posixTimeToMilliSeconds :: POSIXTime -> Int64
posixTimeToMilliSeconds =
  floor . (* 1000) . nominalDiffTimeToSeconds

-- return millisecond timestamp
getCurrentTimestamp :: IO Int64
getCurrentTimestamp = posixTimeToMilliSeconds <$> getPOSIXTime

handleCreateTask :: ReqSQL -> HandlerM (Either String TaskInfo)
handleCreateTask (ReqSQL seqValue) = do
  State {..} <- ask
  plan' <- liftIO $ try $ streamCodegen seqValue
  case plan' of
    Left (e :: SomeException) -> do
      return $ Left $ show e
    Right plan -> do
      case plan of
        CreateBySelectPlan sou sink task -> do
          -----------------------------------
          tid <- getTaskid
          time <- liftIO $ getCurrentTime
          let ti = CreateStream tid seqValue sou sink Starting time

          atomicModifyIORef' tasks (\t -> (M.insert tid ti t, ()))
          -----------------------------------
          mockStore <- liftIO $ mkMockTopicStore

          logOptions <- liftIO $ logOptionsHandle stderr True
          res <- liftIO $
            withLogFunc logOptions $ \lf -> do
              let taskConfig =
                    TaskConfig
                      { tcMessageStoreType = Mock mockStore,
                        tcLogFunc = lf
                      }
              -----------------------------------
              async $ runTask taskConfig task >> return Finished
          -----------------------------------
          atomicModifyIORef' waits (\ls -> (res : ls, ()))
          atomicModifyIORef' thids (\t -> (M.insert res tid t, ()))
          atomicModifyIORef' tasks (\t -> (M.insert tid ti {taskState = Running} t, ()))

          return $ Right ti {taskState = Running}
        CreatePlan topic -> do
          liftIO
            ( try $ do
                createTopics admin (M.fromList [(pack $ unpack topic, (S.TopicAttrs 3))])
                return ()
            )
            >>= \case
              Left (e :: SomeException) -> return $ Left $ show e
              Right () -> do
                tid <- getTaskid
                time <- liftIO $ getCurrentTime
                let ti = CreateTopic tid seqValue topic Finished time
                atomicModifyIORef' tasks (\t -> (M.insert tid ti t, ()))
                return $ Right $ ti
        InsertPlan topic bs -> do
          liftIO
            ( try $ do
                t <- getCurrentTimestamp
                sendMessage produ $
                  ProducerRecord
                    (pack $ unpack topic)
                    Nothing
                    (fromByteString $ BL.toStrict bs)
                    t
            )
            >>= \case
              Left (e :: SomeException) -> return $ Left $ show e
              Right () -> do
                tid <- getTaskid
                time <- liftIO $ getCurrentTime
                let ti = InsertTopic tid seqValue topic Finished time
                atomicModifyIORef' tasks (\t -> (M.insert tid ti t, ()))
                return $ Right $ ti
        _ -> error "Not supported"

waitThread :: State -> IO ()
waitThread State {..} = do
  forever $ do
    li <- readIORef waits
    case li of
      [] -> threadDelay 1000000
      ls -> do
        (a, r) <- waitAnyCatch ls
        atomicModifyIORef' waits (\t -> (L.delete a t, ()))
        case r of
          Left e -> do
            ths <- readIORef thids
            tks <- readIORef tasks
            case M.lookup a ths >>= flip M.lookup tks of
              Nothing -> error "error happened"
              Just ts -> do
                atomicModifyIORef' tasks (\t -> (M.insert (taskid ts) ts {taskState = ErrorHappened $ show e} t, ()))
          Right v -> do
            ths <- readIORef thids
            tks <- readIORef tasks
            case M.lookup a ths >>= flip M.lookup tks of
              Nothing -> error "error happened"
              Just ts -> do
                atomicModifyIORef' tasks (\t -> (M.insert (taskid ts) ts {taskState = v} t, ()))

type HandlerM = ReaderT State Handler

getTaskid :: HandlerM Int
getTaskid = do
  State {..} <- ask
  v <- liftIO $ readIORef index
  liftIO $ atomicModifyIORef' index (\i -> (i + 1, ()))
  return v

liftH :: State -> HandlerM a -> Handler a
liftH s h = runReaderT h s
