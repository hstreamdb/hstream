{-# OPTIONS_GHC -Wno-incomplete-uni-patterns #-}
{-# OPTIONS_GHC -Wno-missing-fields #-}
{-# LANGUAGE OverloadedStrings #-}

module HStream.BootstrapSpec where

import           Control.Concurrent       (MVar, forkIO, newEmptyMVar, putMVar,
                                           takeMVar, tryReadMVar)
import           GHC.IO                   (unsafePerformIO)
import           System.Environment       (lookupEnv)
import           Test.Hspec
import qualified Z.Data.CBytes            as CB
import           ZooKeeper                (zookeeperClose, zookeeperInit)
import           ZooKeeper.Types          (ZHandle)

import           Control.Monad            (replicateM, void)
import           HStream.Server.Bootstrap (startServer)
import           HStream.Server.Types

spec :: Spec
spec = beforeAll zkUri $
  describe "HStream.BootstrapSpec" $ do
    bootstrapBaseSpec
    bootstrapConfigSpec

bootstrapConfigSpec :: SpecWith CB.CBytes
bootstrapConfigSpec = beforeWith getZHandles $ after (mapM_ zookeeperClose) $
  describe "Bootstrap test on different configuration" $ do
    it "bootstrap with inconsistent configuration should fail" $ \(zkHandle:zkHandle2:_) -> do
      isFailed <- newEmptyMVar
      let tryStart action = forkFinally action (\(Left _) -> putMVar isFailed ())
      void . tryStart $ startServer zkHandle defaultServerOpts {_serverMinNum = 2} (pure ())
      void . tryStart $ startServer zkHandle2 defaultServerOpts {_serverName = "hserver-2", _serverMinNum = 1} (pure ())
      takeMVar isFailed `shouldReturn` ()
      threadDelay 10000
    it "bootstrap with same server names should fail" $ \(zkHandle:zkHandle2:_) -> do
      isFailed <- newEmptyMVar
      let tryStart action = forkFinally action (\(Left _) -> putMVar isFailed ())
      void . tryStart $ startServer zkHandle defaultServerOpts {_serverMinNum = 2} (pure ())
      void . tryStart $ startServer zkHandle2 defaultServerOpts {_serverMinNum = 2} (pure ())
      takeMVar isFailed `shouldReturn` ()
      threadDelay 10000

bootstrapBaseSpec :: SpecWith CB.CBytes
bootstrapBaseSpec = beforeWith initTest $ after (mapM_ zookeeperClose) $
  describe "Basic bootstrap test with simple app" $ do
    it "bootstrap one single node cluster" $ \(zkHandle:_) -> do
      takeMVar isAppRunning `shouldReturn` False
      startServer zkHandle defaultServerOpts (putMVar isAppRunning True)
      takeMVar isAppRunning `shouldReturn` True
    it "bootstrap two nodes cluster" $ \(zkHandle:zkHandle2:_) -> do
      takeMVar isAppRunning `shouldReturn` False
      void . forkIO $ startServer zkHandle defaultServerOpts {_serverMinNum = 2} (putMVar isAppRunning True)
      tryReadMVar isAppRunning `shouldReturn` Nothing
      void . forkIO $ startServer zkHandle2 defaultServerOpts {_serverName = "hserver-2", _serverMinNum = 2} (pure ())
      takeMVar isAppRunning `shouldReturn` True
  where
    initTest uri =
      putMVar isAppRunning False >> getZHandles uri

zkUri :: IO CB.CBytes
zkUri = do
  zkPort <- maybe (error "no env") CB.pack <$> lookupEnv "ZOOKEEPER_LOCAL_PORT"
  return $ "127.0.0.1:" <> zkPort

getZHandles :: CB.CBytes -> IO [ZHandle]
getZHandles uri = replicateM 2 (getZHandle uri)

getZHandle :: CB.CBytes -> IO ZHandle
getZHandle uri = zookeeperInit uri 1000 Nothing 0

defaultServerOpts :: ServerOpts
defaultServerOpts = ServerOpts {
    _serverName    = "hserver-1"
  , _serverMinNum  = 1
  , _serverAddress = ""
  , _serverPort    = 0
  }

isAppRunning :: MVar Bool
isAppRunning = unsafePerformIO newEmptyMVar
{-# NOINLINE isAppRunning #-}
