{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GADTs              #-}
{-# LANGUAGE LambdaCase         #-}
{-# LANGUAGE OverloadedStrings  #-}

module HStream.IO.Storage where

import qualified Data.Aeson                as J
import qualified Data.Text                 as T
import qualified Data.Text.Lazy            as LT
import qualified Data.Text.Lazy.Encoding   as LTE
import           HStream.IO.Types
import           HStream.IO.ZkKv           (ZkKv (..))
import qualified HStream.Server.HStreamApi as API
import           ZooKeeper.Types
import           Data.Foldable             (foldrM)

data Storage where
    Storage :: Kv kv => {
      -- Id -> Task information
      tasksKv :: kv,
      -- Id -> status
      statusKv :: kv,
      -- Name -> Id
      namesKv :: kv
    } -> Storage


newZkStorage :: ZHandle -> IO Storage
newZkStorage zk = do
  let rootPath = "/hstream/io"
      tasksKv = ZkKv zk $ rootPath <> "/tasks"
      statusKv = ZkKv zk $ rootPath <> "/status"
      namesKv = ZkKv zk $ rootPath <> "/names"
  return Storage {..}

createIOTask :: Storage -> T.Text -> T.Text -> TaskInfo -> IO ()
createIOTask Storage{..} taskName taskId taskInfo = do
  insert tasksKv taskId $ J.encode taskInfo
  insert statusKv taskId $ ioTaskStatusToBS NEW
  insert namesKv taskName $ LTE.encodeUtf8 (LT.fromStrict taskId)

getIdFromName :: Storage -> T.Text -> IO (Maybe T.Text)
getIdFromName Storage{..} name = do
  get namesKv name >>= traverse (return . LT.toStrict . LTE.decodeUtf8)

listIOTasks :: Storage -> IO [API.Connector]
listIOTasks s@Storage{..} = do
  names <- keys namesKv
  foldrM toItem [] names
  where
    toItem :: T.Text -> [API.Connector] -> IO [API.Connector]
    toItem name res = do
      get namesKv name >>= \case
        Nothing -> return res
        Just taskIdBS -> do
          item <- showIOTaskFromId s (LT.toStrict $ LTE.decodeUtf8 taskIdBS)
          return (item : res)

showIOTask :: Storage -> T.Text -> IO (Maybe API.Connector)
showIOTask s@Storage{..} name = do
  get namesKv name >>= mapM (showIOTaskFromId s . LT.toStrict . LTE.decodeUtf8)

showIOTaskFromId :: Storage -> T.Text -> IO API.Connector
showIOTaskFromId Storage{..} taskId = do
  Just infoBS <- get tasksKv taskId
  Just statusBS <- get statusKv taskId
  let status = LT.toStrict . LTE.decodeUtf8 $ statusBS
      Just TaskInfo {..} = J.decode infoBS
  return $ mkConnector taskName status

updateStatus :: Storage -> T.Text -> IOTaskStatus -> IO ()
updateStatus Storage{..} taskId status = update statusKv taskId $ ioTaskStatusToBS status

deleteIOTask :: Storage -> T.Text -> IO ()
deleteIOTask Storage{..} name = do
  taskId <- LT.toStrict . LTE.decodeUtf8 <$> delete namesKv name
  update statusKv taskId (ioTaskStatusToBS DELETED)

