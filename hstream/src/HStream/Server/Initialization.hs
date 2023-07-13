{-# LANGUAGE CPP                 #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Initialization
  ( initializeServer
  , initializeTlsConfig
  , readTlsPemFile
  , openRocksDBHandle
  , closeRocksDBHandle
  ) where

import           Control.Concurrent               (MVar, newMVar)
import           Control.Concurrent.STM           (TVar, atomically, newTVar,
                                                   newTVarIO, readTVarIO)
import           Control.Exception                (SomeException, catch, try)
import           Control.Monad                    (void)
import qualified Data.ByteString                  as BS
import           Data.Default                     (def)
import           Data.Functor                     ((<&>))
import qualified Data.HashMap.Strict              as HM
import           Data.List                        (find, sort)
import           Data.Word                        (Word32)
import qualified Database.RocksDB                 as RocksDB
import qualified HsGrpc.Server.Types              as G
import           Network.GRPC.HighLevel           (AuthProcessorResult (..),
                                                   AuthProperty (..),
                                                   ProcessMeta,
                                                   ServerSSLConfig (..),
                                                   SslClientCertificateRequestType (..),
                                                   StatusCode (..),
                                                   getAuthProperties)
import qualified System.Directory                 as Directory
import qualified System.Posix.Files               as Files
import           Text.Printf                      (printf)
import qualified Z.Data.CBytes                    as CB


#if __GLASGOW_HASKELL__ < 902
import qualified HStream.Admin.Store.API          as AA
#endif
import           HStream.Common.ConsistentHashing (HashRing, constructServerMap,
                                                   getAllocatedNodeId)
import           HStream.Gossip                   (GossipContext,
                                                   getMemberListWithEpochSTM)
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
  -> Maybe RocksDB.DB
  -> IO ServerContext
initializeServer opts@ServerOpts{..} gossipContext hh db_m = do
  ldclient <- S.newLDClient _ldConfigPath
  let attrs = S.def{S.logReplicationFactor = S.defAttr1 _ckpRepFactor}
  Log.debug $ "checkpoint replication factor: " <> Log.build _ckpRepFactor
  S.initSubscrCheckpointDir ldclient attrs
#if __GLASGOW_HASKELL__ < 902
  let headerConfig = AA.HeaderConfig _ldAdminHost _ldAdminPort _ldAdminProtocolId _ldAdminConnTimeout _ldAdminSendTimeout _ldAdminRecvTimeout
#endif

  -- XXX: Should we add a server option to toggle Stats?
  statsHolder <- newServerStatsHolder

  runningQs <- newMVar HM.empty
  subCtxs <- newTVarIO HM.empty

  epochHashRing <- initializeHashRing gossipContext

  ioWorker <-
    IO.newWorker
      hh
      statsHolder
      (IO.HStreamConfig (cBytesToText ("hstream://" <> CB.pack _serverAddress <> ":" <> CB.pack (show _serverPort))))
      _ioOptions

  shardTable <- newMVar HM.empty
  shardReaderMap <- newMVar HM.empty

  -- recovery tasks

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
      , loadBalanceHashRing      = epochHashRing
      , scIOWorker               = ioWorker
      , gossipContext            = gossipContext
      , serverOpts               = opts
      , shardTable               = shardTable
      , shardReaderMap           = shardReaderMap
      , querySnapshotPath        = _querySnapshotPath
      , querySnapshotter         = db_m
      }

--------------------------------------------------------------------------------

openRocksDBHandle :: FilePath -> IO (Maybe RocksDB.DB)
openRocksDBHandle dbPath = do
  let dbOption = def { RocksDB.createIfMissing = True }
  Directory.doesPathExist dbPath >>= \case
    True -> Files.fileAccess dbPath True True False >>= \case
      True  -> RocksDB.open dbOption dbPath <&> Just
      False -> return Nothing
    False -> try (Directory.createDirectory dbPath) >>= \case
      Right _                    -> RocksDB.open dbOption dbPath <&> Just
      Left  (_ :: SomeException) -> return Nothing

closeRocksDBHandle :: Maybe RocksDB.DB -> IO ()
closeRocksDBHandle Nothing   = return ()
closeRocksDBHandle (Just db) = RocksDB.close db

initializeHashRing :: GossipContext -> IO (TVar (Word32, HashRing))
initializeHashRing gc = atomically $ do
  (epoch, serverNodes) <- getMemberListWithEpochSTM gc
  newTVar (epoch, constructServerMap . sort $ serverNodes)

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
