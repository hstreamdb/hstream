{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

import           Control.Concurrent               (MVar, forkIO, modifyMVar_,
                                                   threadDelay, withMVar)
import           Control.Monad                    (forever, unless, void)
import           Data.List                        (sort)
import qualified Data.Text                        as T
import           Network.GRPC.HighLevel           (ServiceOptions (..))
import           Network.GRPC.HighLevel.Client    (Port (unPort))
import           Text.RawString.QQ                (r)
import           ZooKeeper                        (withResource, zooState,
                                                   zooWatchGetChildren)
import           ZooKeeper.Exception
import           ZooKeeper.Types

import           Control.Exception                (handle)
import           HStream.Common.ConsistentHashing (HashRing, constructHashRing)
import qualified HStream.Logger                   as Log
import           HStream.Server.Config            (getConfig)
import           HStream.Server.HStreamApi        (hstreamApiServer)
import           HStream.Server.HStreamInternal
import           HStream.Server.Handler           (handlers, routineForSubs)
import           HStream.Server.Initialization    (initNodePath,
                                                   initializeServer)
import           HStream.Server.InternalHandler
import           HStream.Server.Persistence       (defaultHandle,
                                                   getServerNode',
                                                   initializeAncestors,
                                                   serverRootPath)
import           HStream.Server.Types             (ServerContext (..),
                                                   ServerOpts (..))
import qualified HStream.Store.Logger             as Log
import           HStream.Utils                    (setupSigsegvHandler)

app :: ServerOpts -> IO ()
app config@ServerOpts{..} = do
  setupSigsegvHandler
  Log.setLogDeviceDbgLevel' _ldLogLevel
  withResource (defaultHandle _zkUri) $ \zk -> do
    initializeAncestors zk
    (options, options', serverContext) <- initializeServer config zk
    initNodePath zk _serverID (T.pack _serverAddress) (fromIntegral _serverPort) (fromIntegral _serverInternalPort)
    serve options options' serverContext

serve :: ServiceOptions -> ServiceOptions -> ServerContext -> IO ()
serve options@ServiceOptions{..} optionsInternal sc@ServerContext{..} = do
  void . forkIO $ updateHashRing zkHandle loadBalanceHashRing
  -- FIXME: now every server should be responsible for monitor zk partition path periodically,
  -- even no subscription req will redirect to current server. This is probably not a good idea
  void . forkIO $ forever $ do
    threadDelay 2000000
    routineForSubs sc
  -- GRPC service
  Log.i "************************"
  putStrLn [r|
   _  _   __ _____ ___ ___  __  __ __
  | || |/' _/_   _| _ \ __|/  \|  V  |
  | >< |`._`. | | | v / _|| /\ | \_/ |
  |_||_||___/ |_| |_|_\___|_||_|_| |_|

  |]
  Log.i $ "Server is starting on port " <> Log.buildInt (unPort serverPort)
  Log.i "*************************"
  api <- handlers sc
  internalApi <- internalHandlers sc
  void . forkIO $ hstreamInternalServer internalApi optionsInternal
  hstreamApiServer api options

main :: IO ()
main = do
  config <- getConfig
  app config

--------------------------------------------------------------------------------

-- However, reconstruct hashRing every time can be expensive
-- when we have a large number of nodes in the cluster.
-- TODO: Instead of reconstruction, we should use the operation insert/delete.
updateHashRing :: ZHandle -> MVar HashRing -> IO ()
updateHashRing zk mhr = handle connLossHandler $
  zooWatchGetChildren zk serverRootPath
    callback action
  where
    callback HsWatcherCtx {..} =
      updateHashRing watcherCtxZHandle mhr
    action (StringsCompletion (StringVector children)) = do
      modifyMVar_ mhr $ \_ -> do
        serverNodes <- mapM (getServerNode' zk) children
        let hr = constructHashRing . sort $ serverNodes
        Log.debug . Log.buildString $ show hr
        return hr
    connLossHandler = \(_ :: ZCONNECTIONLOSS) -> do
      Log.warning "Connection with zookeeper lost"
      void $ withMVar mhr $ \_ -> waitForConnection zk
      Log.warning "Connection with zookeeper re-established"
      updateHashRing zk mhr

waitForConnection :: ZHandle -> IO ()
waitForConnection zk = do
  state <- zooState zk
  unless (state == ZooConnectedState)
    $ waitForConnection zk
