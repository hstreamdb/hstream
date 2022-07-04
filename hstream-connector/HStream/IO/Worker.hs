{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE LambdaCase         #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE TupleSections      #-}

module HStream.IO.Worker where

import           Control.Concurrent        (MVar, modifyMVar_, newMVar,
                                            readMVar)
import           Control.Monad             (unless)
import qualified Data.Aeson                as J
import qualified Data.HashMap.Strict       as HM
import qualified Data.Text                 as T
import qualified Data.UUID                 as UUID
import qualified Data.UUID.V4              as UUID
import qualified HStream.IO.IOTask         as IOTask
import qualified HStream.IO.Storage        as S
import           HStream.IO.Types
import qualified HStream.Logger            as Log
import qualified HStream.Server.HStreamApi as API
import qualified HStream.SQL.Codegen       as CG

data Worker
  = Worker {
    kvConfig :: KvConfig,
    hsConfig :: HStreamConfig,
    ioTasksM :: MVar (HM.HashMap T.Text IOTask.IOTask),
    storage  :: S.Storage
  }

newWorker :: KvConfig -> HStreamConfig -> IO Worker
newWorker kvCfg hsConfig = do
  let (ZkKvConfig zk _ _) = kvCfg
  Log.info $ "new Worker with hsConfig:" <> Log.buildString (show hsConfig)
  Worker kvCfg hsConfig <$> newMVar HM.empty <*> S.newZkStorage zk

createIOTaskFromSql :: Worker -> T.Text -> IO API.Connector
createIOTaskFromSql worker@Worker{..} sql = do
  (CG.CreateConnectorPlan cType cName ifNotExist cfg) <- CG.streamCodegen sql
  Log.info $ "CreateConnector CodeGen"
           <> ", connector type: " <> Log.buildText cType
           <> ", connector name: " <> Log.buildText cName
           <> ", config: "         <> Log.buildString (show cfg)
  taskId <- UUID.toText <$> UUID.nextRandom
  let (Just (J.String image)) = HM.lookup "task.image" cfg
      connectorConfig =
        J.object
          [ "hstream" J..= toTaskJson hsConfig taskId
          , "kv" J..= toTaskJson kvConfig taskId
          , "connector" J..= J.toJSON (HM.filterWithKey (\k _ -> k /= "task.image") cfg)
          ]
      taskInfo = TaskInfo
        { taskName = cName
        , taskType = if cType == "SOURCE" then SOURCE else SINK
        , taskConfig = TaskConfig image
        , connectorConfig = connectorConfig
        , originSql = sql
        }
  S.createIOTask storage cName taskId taskInfo
  createIOTask worker taskId taskInfo
  return $ API.Connector cName (ioTaskStatusToText NEW)

createIOTask :: Worker -> T.Text -> TaskInfo -> IO ()
createIOTask Worker{..} taskId taskInfo = do
  task <- IOTask.newIOTask taskId storage taskInfo
  IOTask.startIOTask task
  modifyMVar_ ioTasksM $ pure . HM.insert taskId task

showIOTask :: Worker -> T.Text -> IO (Maybe API.Connector)
showIOTask Worker{..} name = do
  S.getIdFromName storage name >>= \case
    Nothing  -> return Nothing
    Just tid -> S.showIOTask storage tid

listIOTasks :: Worker -> IO [API.Connector]
listIOTasks Worker{..} = S.listIOTasks storage

stopIOTask :: Worker -> T.Text -> Bool -> IO ()
stopIOTask worker@Worker{..} name ifExist = do
  S.getIdFromName storage name >>= \case
    Nothing   -> unless ifExist $ fail ("invalid task name: " ++ T.unpack name)
    Just taskId -> do
      getIOTask worker taskId >>= \case
        Nothing   -> pure ()
        Just task -> IOTask.stopIOTask task ifExist

-- restartIOTask :: Worker -> T.Text -> IO ()
-- restartIOTask worker taskName = do
--   stopIOTask worker taskName True
--   startIOTask worker taskName

startIOTask :: Worker -> T.Text -> IO ()
startIOTask worker@Worker{..} name = do
  S.getIdFromName storage name >>= \case
    Nothing   -> fail $ "invalid task name: " ++ T.unpack name
    Just taskId -> do
      getIOTask worker taskId >>= \case
        Nothing   -> pure ()
        Just task -> IOTask.startIOTask task

getIOTask :: Worker -> T.Text -> IO (Maybe IOTask.IOTask)
getIOTask Worker{..} taskId = HM.lookup taskId <$> readMVar ioTasksM

deleteIOTask :: Worker -> T.Text -> IO ()
deleteIOTask worker@Worker{..} taskName = do
  stopIOTask worker taskName True
  S.deleteIOTask storage taskName
  modifyMVar_ ioTasksM $ return . HM.delete taskName

