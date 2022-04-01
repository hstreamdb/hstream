{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Initialization
  ( initializeServer
  , initializeTlsConfig
  ) where

import           Control.Concurrent               (MVar, newMVar)
import           Control.Concurrent.STM           (newTVarIO)
import           Control.Exception                (catch)
import           Control.Monad                    (void)
import qualified Data.HashMap.Strict              as HM
import           Data.List                        (find, sort)
import           Network.GRPC.HighLevel           (AuthProcessorResult (AuthProcessorResult),
                                                   AuthProperty (authPropName),
                                                   ProcessMeta,
                                                   ServerSSLConfig (ServerSSLConfig),
                                                   SslClientCertificateRequestType (SslDontRequestClientCertificate, SslRequestAndRequireClientCertificateAndVerify),
                                                   StatusCode (StatusOk),
                                                   getAuthProperties)
import           Text.Printf                      (printf)
import           ZooKeeper                        (zooGetChildren)
import           ZooKeeper.Types

import qualified HStream.Admin.Store.API          as AA
import           HStream.Common.ConsistentHashing (HashRing, constructServerMap)
import qualified HStream.Logger                   as Log
import           HStream.Server.Config            (ServerOpts (..),
                                                   TlsConfig (..))
import           HStream.Server.Persistence       (getServerNode',
                                                   serverRootPath)
import           HStream.Server.Types
import           HStream.Stats                    (newStatsHolder)
import qualified HStream.Store                    as S

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
      , scMaxRecordSize          = _maxRecordSize
      , runningQueries           = runningQs
      , runningConnectors        = runningCs
      , scSubscribeContexts      = subCtxs
      , cmpStrategy              = _compression
      , headerConfig             = headerConfig
      , scStatsHolder            = statsHolder
      , loadBalanceHashRing      = hashRing
      , scServerState            = serverState
      }

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
