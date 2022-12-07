{-# LANGUAGE CPP                 #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Initialization
  ( initializeServer
  , initializeTlsConfig
  , readTlsPemFile
  ) where

import           Control.Concurrent               (MVar, newMVar)
import           Control.Concurrent.STM           (TVar, newTVarIO, readTVarIO)
import           Control.Exception                (catch)
import           Control.Monad                    (void)
import qualified Data.ByteString                  as BS
import qualified Data.HashMap.Strict              as HM
import           Data.List                        (find, sort)
import qualified HsGrpc.Server.Types              as G
import           Network.GRPC.HighLevel           (AuthProcessorResult (..),
                                                   AuthProperty (..),
                                                   ProcessMeta,
                                                   ServerSSLConfig (..),
                                                   SslClientCertificateRequestType (..),
                                                   StatusCode (..),
                                                   getAuthProperties)
import           Text.Printf                      (printf)
import qualified Z.Data.CBytes                    as CB

#if __GLASGOW_HASKELL__ < 902
import qualified HStream.Admin.Store.API          as AA
#endif
import           HStream.Common.ConsistentHashing (HashRing, constructServerMap,
                                                   getAllocatedNodeId)
import           HStream.Gossip                   (GossipContext, getMemberList)
import qualified HStream.IO.Types                 as IO
import qualified HStream.IO.Worker                as IO
import qualified HStream.Logger                   as Log
import           HStream.MetaStore.Types          (MetaHandle (..))
import           HStream.Server.Config            (ServerOpts (..),
                                                   TlsConfig (..))
import           HStream.Server.Types
import           HStream.Stats                    (newServerStatsHolder)
import qualified HStream.Store                    as S
import           HStream.Utils

initializeServer
  :: ServerOpts
  -> GossipContext
  -> MetaHandle
  -> MVar ServerState
  -> IO ServerContext
initializeServer opts@ServerOpts{..} gossipContext hh serverState = do
  ldclient <- S.newLDClient _ldConfigPath
  let attrs = S.def{S.logReplicationFactor = S.defAttr1 _ckpRepFactor}
  _ <- catch (void $ S.initCheckpointStoreLogID ldclient attrs)
             (\(_ :: S.EXISTS) -> return ())
#if __GLASGOW_HASKELL__ < 902
  let headerConfig = AA.HeaderConfig _ldAdminHost _ldAdminPort _ldAdminProtocolId _ldAdminConnTimeout _ldAdminSendTimeout _ldAdminRecvTimeout
#endif

  statsHolder <- newServerStatsHolder

  runningQs <- newMVar HM.empty
  subCtxs <- newTVarIO HM.empty

  hashRing <- initializeHashRing gossipContext

  ioWorker <-
    IO.newWorker
      hh
      (IO.HStreamConfig (cBytesToText (CB.pack _serverAddress <> ":" <> CB.pack (show _serverPort))))
      _ioOptions
      (\k -> do
        hr <- readTVarIO hashRing
        return $ getAllocatedNodeId hr k == _serverID
       )

  shardInfo  <- newMVar HM.empty
  shardTable <- newMVar HM.empty
  shardReaderMap <- newMVar HM.empty

  return
    ServerContext
      { metaHandle                 = hh
      , scLDClient               = ldclient
      , serverID                 = _serverID
      , scAdvertisedListenersKey = Nothing
      , scDefaultStreamRepFactor = _topicRepFactor
      , scMaxRecordSize          = _maxRecordSize
      , runningQueries           = runningQs
      , scSubscribeContexts      = subCtxs
      , cmpStrategy              = _compression
#if __GLASGOW_HASKELL__ < 902
      , headerConfig             = headerConfig
#endif
      , scStatsHolder            = statsHolder
      , loadBalanceHashRing      = hashRing
      , scServerState            = serverState
      , scIOWorker               = ioWorker
      , gossipContext            = gossipContext
      , serverOpts               = opts
      , shardInfo                = shardInfo
      , shardTable               = shardTable
      , shardReaderMap           = shardReaderMap
      , defaultResourceSettings  = _defaultResourceSettings
      }

--------------------------------------------------------------------------------

initializeHashRing :: GossipContext -> IO (TVar HashRing)
initializeHashRing gc = do
  serverNodes <- getMemberList gc
  newTVarIO . constructServerMap . sort $ serverNodes

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

readTlsPemFile :: TlsConfig -> IO G.SslServerCredentialsOptions
readTlsPemFile TlsConfig{..} = do
  key <- BS.readFile keyPath
  cert <- BS.readFile certPath
  ca <- mapM BS.readFile caPath
  let authType = maybe G.GrpcSslDontRequestClientCertificate
                       (const G.GrpcSslRequestAndRequireClientCertificateAndVerify)
                       caPath
  pure $ G.SslServerCredentialsOptions{ pemKeyCertPairs = [(key, cert)]
                                      , pemRootCerts = ca
                                      , clientAuthType = authType
                                      }
