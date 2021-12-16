{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

import           Control.Concurrent             (forkIO)
import           Control.Monad                  (void)
import           Network.GRPC.HighLevel         (ServiceOptions (..))
import           Network.GRPC.HighLevel.Client  (Port (unPort))
import           Text.RawString.QQ              (r)
import           ZooKeeper                      (withResource)

import qualified HStream.Logger                 as Log
import           HStream.Server.Bootstrap       (startServer)
import           HStream.Server.Config          (getConfig)
import           HStream.Server.HStreamApi      (hstreamApiServer)
import           HStream.Server.HStreamInternal (hstreamInternalServer)
import           HStream.Server.Handler         (handlers)
import           HStream.Server.Initialization  (initializeServer)
import           HStream.Server.InternalHandler (internalHandlers)
import           HStream.Server.Leader          (selectLeader)
import           HStream.Server.LoadBalance     (startWritingLoadReport)
import           HStream.Server.Persistence     (defaultHandle,
                                                 initializeAncestors)
import           HStream.Server.Types           (LoadManager,
                                                 ServerContext (..),
                                                 ServerOpts (..))
import qualified HStream.Store.Logger           as Log
import           HStream.Utils                  (setupSigsegvHandler)

app :: ServerOpts -> IO ()
app config@ServerOpts{..} = do
  setupSigsegvHandler
  Log.setLogDeviceDbgLevel' _ldLogLevel
  withResource (defaultHandle _zkUri) $ \zk -> do
    (options, options', serverContext, lm) <- initializeServer config zk
    initializeAncestors zk
    startServer zk config (serve options options' serverContext lm)

serve :: ServiceOptions -> ServiceOptions -> ServerContext -> LoadManager -> IO ()
serve options@ServiceOptions{..} optionsInternal sc@ServerContext{..} lm = do
  startWritingLoadReport zkHandle lm

  selectLeader sc lm

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
