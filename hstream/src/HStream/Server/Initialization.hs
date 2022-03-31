{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Initialization
  ( initializeServer
  , initNodePath
  , initializeTlsConfig
  ) where

import           Control.Concurrent               (MVar, newMVar)
import           Control.Concurrent.STM           (newTVarIO)
import           Control.Exception                (SomeException, catch, try)
import           Control.Monad                    (void)
import qualified Data.HashMap.Strict              as HM
import           Data.List                        (find, sort)
import qualified Data.Text                        as T
import           Data.Unique                      (hashUnique, newUnique)
import           Data.Word                        (Word32)
import           System.Exit                      (exitFailure)
import qualified Z.Data.CBytes                    as CB
import           ZooKeeper                        (zooCreateOpInit,
                                                   zooGetChildren, zooMulti,
                                                   zooSetOpInit)
import qualified ZooKeeper.Recipe                 as Recipe
import           ZooKeeper.Types

import qualified HStream.Admin.Store.API          as AA
import           HStream.Common.ConsistentHashing (HashRing, constructServerMap)
import qualified HStream.Logger                   as Log
import           HStream.Server.HStreamApi
import           HStream.Server.Persistence       (NodeInfo (..),
                                                   decodeZNodeValue,
                                                   encodeValueToBytes,
                                                   getServerNode',
                                                   serverRootLockPath,
                                                   serverRootPath)
import           HStream.Server.Types
import           HStream.Stats                    (newStatsHolder)
import qualified HStream.Store                    as S
import           HStream.Utils
import           Network.GRPC.HighLevel           (AuthProcessorResult (AuthProcessorResult),
                                                   AuthProperty (authPropName),
                                                   ProcessMeta,
                                                   ServerSSLConfig (ServerSSLConfig),
                                                   SslClientCertificateRequestType (SslDontRequestClientCertificate, SslRequestAndRequireClientCertificateAndVerify),
                                                   StatusCode (StatusOk),
                                                   getAuthProperties)
import           Text.Printf                      (printf)

{-
  Starting hservers will no longer be happening in parallel.
  The status Running does not fully reflect running, however,
  if we want status to be more precise,
  two things are needed:
-}
-- TODO: a callback when grpc server is successfully started
-- TODO: a consistent algorithm to oversee the whole cluster
initNodePath :: ZHandle -> ServerID -> T.Text -> Word32 -> Word32 -> IO ()
initNodePath zk serverID host port port' = do
  let nodeInfo = NodeInfo { serverHost = host
                          , serverPort = port
                          , serverInternalPort = port'
                          }
  uniq <- newUnique
  void $ Recipe.withLock zk serverRootLockPath (CB.pack . show . hashUnique $ uniq) $ do
    serverStatusMap <- decodeZNodeValue zk serverRootPath
    let nodeStatus = ServerNodeStatus
          { serverNodeStatusState = EnumPB NodeStateRunning
          , serverNodeStatusNode  = Just ServerNode
              { serverNodeId = serverID
              , serverNodeHost = host
              , serverNodePort = port
              }
          }
    let val = case serverStatusMap of
          Just hmap -> HM.insert serverID nodeStatus hmap
          Nothing   -> HM.singleton serverID nodeStatus
    let ops = [ createEphemeral (serverRootPath, Just $ encodeValueToBytes nodeInfo)
              , zooSetOpInit serverRootPath (Just $ encodeValueToBytes val) Nothing
              ]
    e' <- try $ zooMulti zk ops
    case e' of
      Left (e :: SomeException) -> do
        Log.fatal . Log.buildString $ "Server failed to start: " <> show e
        exitFailure
      Right _ -> return ()
  where
    createEphemeral (path, content) =
      zooCreateOpInit (path <> "/" <> CB.pack (show serverID))
                      content 0 zooOpenAclUnsafe ZooEphemeral

initializeServer :: ServerOpts -> ZHandle -> MVar ServerState -> IO ServerContext
initializeServer ServerOpts{..} zk serverState = do
  ldclient <- S.newLDClient _ldConfigPath
  let attrs = S.def{S.logReplicationFactor = S.defAttr1 _ckpRepFactor}
  _ <- catch (void $ S.initCheckpointStoreLogID ldclient attrs)
             (\(_ :: S.EXISTS) -> return ())
  let headerConfig = AA.HeaderConfig _ldAdminHost _ldAdminPort _ldAdminProtocolId _ldAdminConnTimeout _ldAdminSendTimeout _ldAdminRecvTimeout

  statsHolder <- newStatsHolder

  runningQs <- newMVar HM.empty
  runningCs <- newMVar HM.empty
  subCtxs <- newTVarIO HM.empty

  hashRing <- initializeHashRing zk

  return
    ServerContext
      { zkHandle                 = zk
      , scLDClient               = ldclient
      , serverID                 = _serverID
      , scDefaultStreamRepFactor = _topicRepFactor
      , runningQueries           = runningQs
      , runningConnectors        = runningCs
      , scSubscribeContexts      = subCtxs
      , cmpStrategy              = _compression
      , headerConfig             = headerConfig
      , scStatsHolder            = statsHolder
      , loadBalanceHashRing      = hashRing
      , scServerState            = serverState
      }

--------------------------------------------------------------------------------

initializeHashRing :: ZHandle -> IO (MVar HashRing)
initializeHashRing zk = do
  StringsCompletion (StringVector children) <-
    zooGetChildren zk serverRootPath
  serverNodes <- mapM (getServerNode' zk) children
  newMVar . constructServerMap . sort $ serverNodes

initializeTlsConfig :: TlsConfig -> ServerSSLConfig
initializeTlsConfig TlsConfig {..} = ServerSSLConfig caPath keyPath certPath authType authHandler
  where
    authType = maybe SslDontRequestClientCertificate (const SslRequestAndRequireClientCertificateAndVerify) caPath
    authHandler = fmap (const authProcess) caPath

-- ref: https://github.com/grpc/grpc/blob/master/doc/server_side_auth.md
authProcess :: ProcessMeta
authProcess authCtx _ = do
  prop <- getAuthProperties authCtx
  let cn = find ((== "x509_common_name") . authPropName) prop
  Log.info . Log.buildString . printf "user:[%s] is logging in" $ show cn
  return $ AuthProcessorResult mempty mempty StatusOk ""
