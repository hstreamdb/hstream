{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
module HStream.Server.Leader where


import           Control.Concurrent         (forkIO, isEmptyMVar, putMVar,
                                             swapMVar)
import           Control.Monad              (void)
import qualified Data.UUID                  as UUID
import           Data.UUID.V4               (nextRandom)
import qualified Z.Data.CBytes              as CB
import           ZooKeeper                  (zooGet, zooSet)
import           ZooKeeper.Recipe.Election  (election)
import           ZooKeeper.Types            (DataCompletion (..))

import           HStream.Server.Persistence (leaderPath)
import           HStream.Server.Types       (ServerContext (..))

selectLeader :: ServerContext -> IO ()
selectLeader ServerContext{..} = do
  uuid <- nextRandom
  void . forkIO $ election zkHandle "/election" (CB.pack . UUID.toString $ uuid)
    (do
      void $ zooSet zkHandle leaderPath (Just $ CB.toBytes serverName) Nothing
      updateLeader serverName
    )
    (\_ -> do
      DataCompletion v _ <- zooGet zkHandle leaderPath
      case v of
        Just x  -> updateLeader (CB.fromBytes x)
        Nothing -> pure ()
    )
  where
    updateLeader new = do
      noLeader <- isEmptyMVar leaderName
      case () of
        _ | noLeader  -> putMVar leaderName new
          | otherwise -> void $ swapMVar leaderName new
