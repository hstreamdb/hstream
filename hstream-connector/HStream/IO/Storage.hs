{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GADTs              #-}
{-# LANGUAGE LambdaCase         #-}
{-# LANGUAGE OverloadedStrings  #-}

module HStream.IO.Storage where

import           Control.Monad             (forM)
import qualified Data.Aeson                as J
import qualified Data.Text                 as T
import qualified Data.Text.Lazy            as LT
import qualified Data.Text.Lazy.Encoding   as LTE
import           HStream.IO.Types
import           HStream.IO.ZkKv           (ZkKv (..))
import qualified HStream.Server.HStreamApi as API
import           ZooKeeper.Types

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
  insert namesKv taskName $ LTE.encodeUtf8 (LT.fromStrict taskId)
  insert tasksKv taskId $ J.encode taskInfo
  insert statusKv taskId $ ioTaskStatusToBS NEW

getIdFromName :: Storage -> T.Text -> IO (Maybe T.Text)
getIdFromName Storage{..} name =
  get namesKv name >>= \case
    Nothing -> return Nothing
    Just v  -> return . Just . LT.toStrict . LTE.decodeUtf8 $ v

listIOTasks :: Storage -> IO [API.Connector]
listIOTasks Storage{..} = do
  names <- keys namesKv
  forM names toItem
  where
    toItem name = do
      -- TODO: atomically read
      Just taskIdBS <- get namesKv name
      let taskId = LT.toStrict $ LTE.decodeUtf8 taskIdBS
      Just statusBS <- get statusKv taskId
      Just infoBS <- get tasksKv taskId
      let Just TaskInfo {..} = J.decode infoBS
      let status = LT.toStrict . LTE.decodeUtf8 $ statusBS
      return $ API.Connector taskName status

showIOTask :: Storage -> T.Text -> IO (Maybe API.Connector)
showIOTask Storage{..} taskId = do
  get tasksKv taskId >>= \case
    Nothing -> return Nothing
    Just infoBS -> do
      Just statusBS <- get statusKv taskId
      let status = LT.toStrict . LTE.decodeUtf8 $ statusBS
          Just TaskInfo {..} = J.decode infoBS
      return . Just $ API.Connector taskName status

updateStatus :: Storage -> T.Text -> IOTaskStatus -> IO ()
updateStatus Storage{..} taskId status = update statusKv taskId $ ioTaskStatusToBS status

deleteIOTask :: Storage -> T.Text -> IO ()
deleteIOTask Storage{..} name = do
  taskId <- LT.toStrict . LTE.decodeUtf8 <$> delete namesKv name
  update statusKv taskId (ioTaskStatusToBS DELETED)

