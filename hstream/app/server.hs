{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DeriveAnyClass      #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

import           Control.Concurrent
import           Control.Concurrent.STM.TChan
import           Control.Exception
import           Control.Monad
import           Control.Monad.STM                (atomically)
import           Data.Aeson                       (FromJSON (..), ToJSON (..))
import qualified Data.Aeson                       as Aeson
import           Data.ByteString                  (ByteString)
import           Data.IORef
import           Data.Int                         (Int64)
import qualified Data.Map.Strict                  as Map
import           Data.Set                         (Set)
import qualified Data.Set                         as Set
import           GHC.Generics
import           Network.GRPC.HighLevel.Generated
import           Options.Applicative
import           System.Exit                      (exitFailure)
import           System.IO.Unsafe                 (unsafePerformIO)
import           Text.RawString.QQ                (r)
import qualified Z.Data.Builder                   as Builder
import           Z.Data.CBytes                    (CBytes, toBytes, unpack)
import qualified Z.Data.CBytes                    as CBytes
import           Z.Foreign                        (toByteString)
import           Z.IO.Network
import           ZooKeeper
import           ZooKeeper.Exception
import           ZooKeeper.Types

import qualified HStream.Logger                   as Log
import           HStream.Server.HStreamApi
import           HStream.Server.Handler
import           HStream.Server.Persistence
import           HStream.Store
import qualified HStream.Store.Admin.API          as AA
import           HStream.Utils                    (bytesToLaztByteString,
                                                   lazyByteStringToBytes,
                                                   setupSigsegvHandler)
import           Z.Data.Vector.Base               (Bytes)

--------------------------------------------------------------------------------

retryCount :: IORef Int
retryCount = unsafePerformIO $ newIORef 1
{-# NOINLINE retryCount #-}

prevServers :: IORef (Set CBytes)
prevServers = unsafePerformIO $ newIORef Set.empty
{-# NOINLINE prevServers #-}

serverEvents :: TChan ZooEvent
serverEvents = unsafePerformIO newTChanIO
{-# NOINLINE serverEvents #-}

isSelfRunning :: IORef Bool
isSelfRunning = unsafePerformIO $ newIORef False
{-# NOINLINE isSelfRunning #-}

data NodeStatus = Starting | Ready | Working deriving (Show, Eq, Generic, FromJSON, ToJSON)

data NodeInfo = NodeInfo
  { nodeStatus :: NodeStatus
  } deriving (Show, Eq, Generic, FromJSON, ToJSON)

data HServerConfig = HServerConfig
  { hserverMinServers :: Int
  } deriving (Eq, Show, Generic, FromJSON, ToJSON)

--------------------------------------------------------------------------------

-- TODO
-- 1. config file for the Server
-- 2. log options

data ServerOpts = ServerOpts
  { _serverHost         :: CBytes
  , _serverPort         :: PortNumber
  , _serverName         :: CBytes
  , _serverMinNum       :: Int
  , _persistent         :: Bool
  , _zkUri              :: CBytes
  , _ldConfigPath       :: CBytes
  , _topicRepFactor     :: Int
  , _ckpRepFactor       :: Int
  , _heartbeatTimeout   :: Int64
  , _compression        :: Compression
  , _ldAdminHost        :: ByteString
  , _ldAdminPort        :: Int
  , _ldAdminProtocolId  :: AA.ProtocolId
  , _ldAdminConnTimeout :: Int
  , _ldAdminSendTimeout :: Int
  , _ldAdminRecvTimeout :: Int
  , _serverLogLevel     :: Log.Level
  , _serverLogWithColor :: Bool
  } deriving (Show)

printBanner :: IO ()
printBanner = do
  putStrLn [r|
   _  _   __ _____ ___ ___  __  __ __
  | || |/' _/_   _| _ \ __|/  \|  V  |
  | >< |`._`. | | | v / _|| /\ | \_/ |
  |_||_||___/ |_| |_|_\___|_||_|_| |_|

  |]

parseConfig :: Parser ServerOpts
parseConfig =
  ServerOpts
    <$> strOption ( long "host" <> metavar "HOST"
                 <> showDefault <> value "127.0.0.1"
                 <> help "server host value"
                  )
    <*> option auto ( long "port" <> short 'p' <> metavar "INT"
                   <> showDefault <> value 6570
                   <> help "server port value"
                    )
    <*> strOption ( long "name" <> metavar "NAME"
                 <> showDefault <> value "hserver-1"
                 <> help "name of the hstream server node"
                  )
    <*> option auto ( long "min-servers" <> metavar "INT"
                   <> showDefault <> value 1
                   <> help "minimal hstream servers")
    <*> flag False True ( long "persistent"
                       <> help "set flag to store queries in zookeeper"
                        )
    <*> strOption ( long "zkuri" <> metavar "STR"
                 <> showDefault
                 <> value "127.0.0.1:2181"
                 <> help ( "comma separated host:port pairs, each corresponding"
                      <> "to a zk zookeeper server, only meaningful when"
                      <> "persistent flag is set. "
                      <> "e.g. \"127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183\""
                         )
                  )
    <*> strOption ( long "store-config" <> metavar "PATH"
                 <> showDefault <> value "/data/store/logdevice.conf"
                 <> help "logdevice config path"
                  )
    <*> option auto ( long "replicate-factor" <> metavar "INT"
                   <> showDefault <> value 3
                   <> help "topic replicate factor"
                    )
    <*> option auto ( long "ckp-replicate-factor" <> metavar "INT"
                   <> showDefault <> value 1
                   <> help "checkpoint replicate factor"
                    )
    <*> option auto ( long "timeout" <> metavar "INT"
                   <> showDefault <> value 1000
                   <> help "the timer timeout in milliseconds"
                    )
    <*> option auto ( long "compression" <> metavar "none|lz4|lz4hc"
                   <> showDefault <> value CompressionLZ4
                   <> help "Specify the compression policy for gdevice"
                    )
    <*> strOption   ( long "hadmin-host" <> metavar "HOST"
                   <> showDefault <> value "127.0.0.1" <> help "logdevice admin host"
                    )
    <*> option auto ( long "hadmin-port" <> metavar "INT"
                   <> showDefault <> value 6440 <> help "logdevice admin port"
                    )
    <*> option auto ( long "hadmin-protocol-id" <> metavar "ProtocolId"
                   <> showDefault <> value AA.binaryProtocolId <> help "logdevice admin thrift protocol id"
                    )
    <*> option auto ( long "hadmin-conn-timeout" <> metavar "INT"
                   <> showDefault <> value 5000
                   <> help "logdevice admin thrift connection timeout in milliseconds"
                    )
    <*> option auto ( long "hadmin-send-timeout" <> metavar "INT"
                   <> showDefault <> value 5000
                   <> help "logdevice admin thrift sending timeout in milliseconds"
                    )
    <*> option auto ( long "hadmin-recv-timeout" <> metavar "INT"
                   <> showDefault <> value 5000
                   <> help "logdevice admin thrift receiving timeout in milliseconds"
                    )
    <*> option auto ( long "log-level" <> metavar "[critical|fatal|warning|info|debug]"
                   <> showDefault <> value (Log.Level Log.INFO)
                   <> help "server log level"
                    )
    <*> switch ( long "log-with-color"
              <> help "print logs with color or not" )

app :: ServerOpts -> ZHandle -> IO ()
app config@ServerOpts{..} zk = do
  Log.setLogLevel _serverLogLevel _serverLogWithColor
  setupSigsegvHandler
  ldclient <- newLDClient _ldConfigPath
  _ <- initCheckpointStoreLogID ldclient (LogAttrs $ HsLogAttrs _ckpRepFactor Map.empty)
  if _persistent
     then serve config ldclient (Just zk)
     else serve config ldclient Nothing

serve :: ServerOpts -> LDClient -> Maybe ZHandle -> IO ()
serve ServerOpts{..} ldclient zk = do
  let options = defaultServiceOptions
                { serverHost = Host . toByteString . toBytes $ _serverHost
                , serverPort = Port . fromIntegral $ _serverPort
                }
  let headerConfig = AA.HeaderConfig _ldAdminHost _ldAdminPort _ldAdminProtocolId _ldAdminConnTimeout _ldAdminSendTimeout _ldAdminRecvTimeout
  api <- handlers ldclient headerConfig _topicRepFactor zk _heartbeatTimeout _compression
  Log.i $ "Server started on "
       <> CBytes.toBuilder _serverHost <> ":" <> Builder.int _serverPort
  hstreamApiServer api options

initZooKeeper :: ZHandle -> IO ()
initZooKeeper zk = catch (initializeAncestors zk) (\(_ :: ZNODEEXISTS) -> pure ())

main :: IO ()
main = do
  config@ServerOpts{..} <- execParser $ info (parseConfig <**> helper) (fullDesc <> progDesc "HStream-Server")
  let rootPath         = "/hserver"
      serverRootPath   = rootPath <> "/servers"
      configPath       = rootPath <> "/config"
      serverPath       = serverRootPath <> "/" <> _serverName
  withResource (defaultHandle _zkUri) $ \zk -> do
    initZooKeeper zk

    -- 1. Check persistent paths
    rootExists <- zooExists zk rootPath
    serverRootExists <- zooExists zk serverRootPath
    case rootExists of
      Just _  -> return ()
      Nothing -> void $ zooCreate zk rootPath Nothing zooOpenAclUnsafe ZooPersistent
    case serverRootExists of
      Just _  -> return ()
      Nothing -> void $ zooCreate zk serverRootPath Nothing zooOpenAclUnsafe ZooPersistent

    -- 2. Check the consistence of the server config
    configExists <- zooExists zk configPath
    case configExists of
      Just _  -> do
        (DataCompletion val _) <- zooGet zk configPath
        case Aeson.decode' . bytesToLaztByteString =<< val of
          Just (HServerConfig minServers)
            | minServers == _serverMinNum -> return ()
            | otherwise  -> do
                Log.fatal . Log.buildString $
                  "Server config min-servers is set to "
                  <> show _serverMinNum <> ", which does not match "
                  <> show minServers <> " in zookeeper"
                exitFailure
          Nothing -> Log.fatal "Server error: broken config is found"
      Nothing -> do
        let serverConfig = HServerConfig { hserverMinServers = _serverMinNum }
        void $ zooCreate zk configPath (Just $ valueToBytes serverConfig) zooOpenAclUnsafe ZooEphemeral

    -- 3. Run the monitoring service
    void . forkIO $ watchChildren' zk serverRootPath
    threadDelay 1000000
    let myApp = do
          printBanner
          app config zk
    void . forkIO $ watcher zk serverRootPath _serverName _serverMinNum myApp

    -- 4. Create initial server node (Starting)
    let nodeInfo = NodeInfo { nodeStatus = Starting }
    e' <- try $ zooCreate zk serverPath (Just $ valueToBytes nodeInfo) zooOpenAclUnsafe ZooEphemeral
    case e' of
      Left (e :: SomeException) -> do
        Log.fatal . Log.buildString $ "Server failed to start: " <> show e
        exitFailure
      Right _ -> return ()

    -- 5. Start
    setNodeStatus zk _serverName Ready
    threadDelay 10000000000

--------------------------------------------------------------------------------

watchChildren' :: ZHandle -> CBytes -> IO()
watchChildren' zk path = do
  zooWatchGetChildren zk path callback ret
  where
    callback HsWatcherCtx{..} = do
      (StringsCompletion (StringVector children)) <- zooGetChildren watcherCtxZHandle path
      oldChs <- readIORef prevServers
      let newChs = Set.fromList children
      let act
            | oldChs `Set.isSubsetOf` newChs = do
                atomically $ writeTChan serverEvents ZooCreateEvent
            | newChs `Set.isSubsetOf` oldChs = do
                atomically $ writeTChan serverEvents ZooDeleteEvent
            | otherwise = error "Unknown internal error"
      act
      void $ watchChildren' watcherCtxZHandle path
    ret (StringsCompletion (StringVector children)) = do
      writeIORef prevServers (Set.fromList children)


watcher :: ZHandle -> CBytes -> CBytes -> Int -> IO () -> IO ()
watcher zk path self minServer app = forever $ do
  event <- atomically $ readTChan serverEvents
  Log.debug . Log.buildString $ "Event " <> show event <> " detected"
  (StringsCompletion (StringVector servers)) <- zooGetChildren zk path
  cnt <- readIORef retryCount
  readys <- forM servers $ \serverName -> do
    (e' :: Either SomeException NodeStatus) <- try $ getNodeStatus zk serverName
    case e' of
      Right Ready   -> return (1 :: Int)
      Right Working -> return (1 :: Int)
      _             -> return (0 :: Int)
  let readyServers = sum readys

  e_selfStatus <- try $ getNodeStatus zk self
  case e_selfStatus of
    Left (_ :: SomeException) -> do
      Log.warning . Log.buildString $
        "Trial #" <> show cnt <> ": Failed to get self server info. There are "
        <> show readyServers <> " ready servers, which is fewer than the minimum "
        <> "requirement " <> show minServer
      modifyIORef retryCount (+ 1)
    Right selfStatus ->
      case selfStatus of
        Ready
          | event == ZooCreateEvent && readyServers < minServer -> do
              Log.warning . Log.buildString $
                "Trial #" <> show cnt <> ": There are " <> show readyServers <> " ready servers"
                <> ", which is fewer than the minimum requirement " <> show minServer
              modifyIORef retryCount (+ 1)
          | event == ZooCreateEvent && readyServers >= minServer -> do
              setNodeStatus zk self Working -- exception?
              isRunning <- readIORef isSelfRunning
              unless isRunning $ do
                Log.debug "Cluster is ready, starting hstream server..."
                void $ forkIO app
                writeIORef isSelfRunning True
          | event == ZooChangedEvent && readyServers < minServer -> do
              Log.warning . Log.buildString $
                "Trial #" <> show cnt <> ": There are " <> show readyServers <> " ready servers"
                <> ", which is fewer than the minimum requirement " <> show minServer
              modifyIORef retryCount (+ 1)
          | event == ZooChangedEvent && readyServers >= minServer -> do
              setNodeStatus zk self Working -- exception?
              isRunning <- readIORef isSelfRunning
              unless isRunning $ do
                Log.debug "Cluster is ready, starting hstream server..."
                void $ forkIO app
                writeIORef isSelfRunning True
          | event == ZooDeleteEvent && readyServers < minServer -> do
              Log.warning . Log.buildString $
                "Trial #" <> show cnt <> ": There are " <> show readyServers <> " ready servers"
                <> ", which is fewer than the minimum requirement " <> show minServer
              modifyIORef retryCount (+ 1)
          | event == ZooDeleteEvent && readyServers >= minServer -> do -- impossible!
              Log.fatal "Internal server error"
          | otherwise -> return ()
        Working
          | event == ZooCreateEvent && readyServers < minServer -> do -- impossible!
              Log.fatal "Internal server error"
          | event == ZooCreateEvent && readyServers >= minServer -> do
              return ()
          | event == ZooChangedEvent && readyServers < minServer -> do
              setNodeStatus zk self Ready -- exception? stop app?
              Log.warning "No enough nodes found, server may not work properly "
          | event == ZooChangedEvent && readyServers >= minServer -> do
              return ()
          | event == ZooDeleteEvent && readyServers < minServer -> do
              setNodeStatus zk self Ready -- exception? stop app?
              Log.warning "No enough nodes found, server may not work properly"
          | event == ZooDeleteEvent && readyServers >= minServer -> do
              return ()
          | otherwise -> return ()
        _ -> return ()

getNodeStatus :: ZHandle -> CBytes -> IO NodeStatus
getNodeStatus zk serverName = do
  (DataCompletion val _ ) <- zooGet zk ("/hserver/servers/" <> serverName)
  case Aeson.decode' . bytesToLaztByteString =<< val of
    Just (NodeInfo status) -> return status
    Nothing                ->
      error "Failed to get node status, no status found or data corrupted"

setNodeStatus :: ZHandle -> CBytes -> NodeStatus -> IO ()
setNodeStatus zk serverName status = do
  let nodeInfo = NodeInfo { nodeStatus = status }
  void $ zooSet zk ("/hserver/servers/" <> serverName) (Just $ valueToBytes nodeInfo) Nothing

valueToBytes :: (ToJSON a) => a -> Bytes
valueToBytes = lazyByteStringToBytes . Aeson.encode
