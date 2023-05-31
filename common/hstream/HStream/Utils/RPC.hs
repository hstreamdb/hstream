{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE DeriveAnyClass    #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE PatternSynonyms   #-}

module HStream.Utils.RPC
  ( HStreamClientApi

  , SocketAddr(..)
  , runWithAddr
  , mkGRPCClientConf
  , mkGRPCClientConfWithSSL
  , mkClientNormalRequest

  , mkServerErrResp
  , returnErrResp
  , returnResp
  , returnServerStreamingResp
  , returnBiDiStreamingResp
  , returnCommandQueryResp
  , returnCommandQueryEmptyResp
  , getServerResp
  , getServerRespPure
  , getProtoTimestamp
  , msTimestampToProto
  , isSuccessful

  , pattern EnumPB
  , showNodeStatus
  , TaskStatus (Creating, Running, Resuming, Terminated, Aborted, Unknown, Paused, ..)
  , ResourceType (ResStream, ResSubscription, ResShard, ResShardReader, ResConnector, ResQuery, ResView, ..)
  , QType (QueryCreateStream, QueryCreateView, ..)
  ) where

import           Control.Monad
import           Data.Aeson                       (FromJSON, ToJSON)
import           Data.ByteString                  (ByteString)
import qualified Data.ByteString.Char8            as BSC
import           Data.String                      (IsString)
import           Data.Time.Clock.System           (SystemTime (MkSystemTime),
                                                   getSystemTime)
import qualified Data.Vector                      as V
import           GHC.Generics                     (Generic)
import           Network.GRPC.HighLevel.Client
import           Network.GRPC.HighLevel.Generated (GRPCIOError (..),
                                                   withGRPCClient)
import           Network.GRPC.HighLevel.Server
import qualified Proto3.Suite                     as PB
import           Z.Data.JSON                      (JSON)

import           Data.Int                         (Int64)
import           HStream.Server.HStreamApi
import           HStream.ThirdParty.Protobuf      (Struct (..), Timestamp (..))

type HStreamClientApi = HStreamApi ClientRequest ClientResult
data SocketAddr = SocketAddr ByteString Int
  deriving (Eq, Show)

runWithAddr :: SocketAddr -> (HStreamClientApi -> IO a) -> IO a
runWithAddr addr action =
  withGRPCClient (mkGRPCClientConf addr) (hstreamApiClient >=> action)

mkGRPCClientConf :: SocketAddr -> ClientConfig
mkGRPCClientConf socketAddr = mkGRPCClientConfWithSSL socketAddr Nothing

mkGRPCClientConfWithSSL :: SocketAddr -> Maybe ClientSSLConfig -> ClientConfig
mkGRPCClientConfWithSSL (SocketAddr host port) sslConfig =
  ClientConfig
  { clientServerHost = Host host
  , clientServerPort = Port port
  , clientArgs = []
  , clientSSLConfig = sslConfig
  , clientAuthority = Nothing
  }

mkClientNormalRequest :: Int -> a -> ClientRequest 'Normal a b
mkClientNormalRequest requestTimeout x = ClientNormalRequest x requestTimeout (MetadataMap mempty)

--------------------------------------------------------------------------------

mkServerErrResp :: StatusCode -> StatusDetails -> ServerResponse 'Normal a
mkServerErrResp = ServerNormalResponse Nothing mempty
{-# INLINE mkServerErrResp #-}

returnErrResp
  :: Monad m
  => StatusCode -> StatusDetails -> m (ServerResponse 'Normal a)
returnErrResp = (return .) . mkServerErrResp
{-# INLINE returnErrResp #-}

returnResp :: Monad m => a -> m (ServerResponse 'Normal a)
returnResp resp = return (ServerNormalResponse (Just resp) mempty StatusOk "")
{-# INLINE returnResp #-}

returnServerStreamingResp :: Monad m => StatusCode -> StatusDetails -> m (ServerResponse 'ServerStreaming a)
returnServerStreamingResp code = return . ServerWriterResponse mempty code
{-# INLINE returnServerStreamingResp #-}

returnBiDiStreamingResp :: Monad m => StatusCode -> StatusDetails -> m (ServerResponse 'BiDiStreaming a)
returnBiDiStreamingResp code = return . ServerBiDiResponse mempty code
{-# INLINE returnBiDiStreamingResp #-}

returnCommandQueryResp :: Monad m
                       => V.Vector Struct
                       -> m (ServerResponse 'Normal CommandQueryResponse)
returnCommandQueryResp v = returnResp $ CommandQueryResponse v
{-# INLINE returnCommandQueryResp #-}

returnCommandQueryEmptyResp :: Monad m => m (ServerResponse 'Normal CommandQueryResponse)
returnCommandQueryEmptyResp = returnResp $ CommandQueryResponse V.empty
{-# INLINE returnCommandQueryEmptyResp #-}

-- | Extract response value from ClientResult, if there is any error happened,
-- throw IOException.
getServerResp :: ClientResult 'Normal a -> IO a
getServerResp result = do
  case result of
    ClientNormalResponse x _meta1 _meta2 StatusOk _details -> return x
    ClientNormalResponse _resp _meta1 _meta2 _status _details -> do
      error $ "Impossible happened..." <> show _status
    ClientErrorResponse (ClientIOError (GRPCIOBadStatusCode x details))
        -> errorWithoutStackTrace ("Server Error: "  <> BSC.unpack (unStatusDetails details))
    ClientErrorResponse err -> errorWithoutStackTrace ("Error: " <> show err )
{-# INLINE getServerResp #-}

getServerRespPure :: ClientResult 'Normal a -> Either String a
getServerRespPure result = do
  case result of
    ClientNormalResponse x _meta1 _meta2 StatusOk _details -> Right x
    ClientNormalResponse _resp _meta1 _meta2 _status _details ->
      Left $ "Impossible happened..." <> show _status
    ClientErrorResponse err -> Left $ "Error: " <> show err
{-# INLINE getServerRespPure #-}

getProtoTimestamp :: IO Timestamp
getProtoTimestamp = do
  MkSystemTime sec nano <- getSystemTime
  return $ Timestamp sec (fromIntegral nano)

msTimestampToProto :: Int64 -> Timestamp
msTimestampToProto millis =
  let (sec, remain) = millis `divMod` 1000
      nano = remain * 1000000
   in Timestamp sec (fromIntegral nano)

isSuccessful :: ClientResult 'Normal a -> Bool
isSuccessful (ClientNormalResponse _ _ _ StatusOk _) = True
isSuccessful _                                       = False

pattern EnumPB :: a -> PB.Enumerated a
pattern EnumPB x = PB.Enumerated (Right x)

showNodeStatus :: IsString s => PB.Enumerated NodeState -> s
showNodeStatus = \case
  EnumPB NodeStateStarting    -> "Starting"
  EnumPB NodeStateRunning     -> "Running"
  EnumPB NodeStateUnavailable -> "Unavailable"
  EnumPB NodeStateDead        -> "Dead"
  _                           -> "Unknown"

-- A type synonym could also work but the pattern synonyms defined below cannot
-- be bundled with a type synonym when other modules import these definitions
newtype TaskStatus = TaskStatus { getPBStatus :: PB.Enumerated TaskStatusPB}
  deriving (Generic, Show, JSON, Eq, ToJSON, FromJSON)

instance JSON (PB.Enumerated TaskStatusPB)
instance JSON TaskStatusPB
instance ToJSON (PB.Enumerated TaskStatusPB)
instance FromJSON (PB.Enumerated TaskStatusPB)

pattern Running, Resuming, Creating, Aborted, Paused, Unknown :: TaskStatus
pattern Running  = TaskStatus (PB.Enumerated (Right TaskStatusPBTASK_RUNNING))
pattern Creating = TaskStatus (PB.Enumerated (Right TaskStatusPBTASK_CREATING))
pattern Paused   = TaskStatus (PB.Enumerated (Right TaskStatusPBTASK_PAUSED))
pattern Resuming = TaskStatus (PB.Enumerated (Right TaskStatusPBTASK_RESUMING))
pattern Aborted  = TaskStatus (PB.Enumerated (Right TaskStatusPBTASK_ABORTED))
pattern Unknown  = TaskStatus (PB.Enumerated (Right TaskStatusPBTASK_UNKNOWN))
pattern Terminated = TaskStatus (PB.Enumerated (Right TaskStatusPBTASK_TERMINATED))

pattern ResStream, ResSubscription, ResShard, ResConnector, ResShardReader, ResQuery, ResView :: ResourceType
pattern ResStream       = ResourceTypeResStream
pattern ResSubscription = ResourceTypeResSubscription
pattern ResShard        = ResourceTypeResShard
pattern ResShardReader  = ResourceTypeResShardReader
pattern ResConnector    = ResourceTypeResConnector
pattern ResQuery        = ResourceTypeResQuery
pattern ResView         = ResourceTypeResView

newtype QType = QType { getQueryType :: PB.Enumerated QueryType }
  deriving (Generic, Show, JSON, Eq, ToJSON, FromJSON)

instance JSON (PB.Enumerated QueryType)
instance JSON QueryType
instance ToJSON (PB.Enumerated QueryType)
instance FromJSON (PB.Enumerated QueryType)

pattern QueryCreateStream, QueryCreateView :: QType
pattern QueryCreateStream = QType (PB.Enumerated (Right QueryTypeCreateStreamAs))
pattern QueryCreateView   = QType (PB.Enumerated (Right QueryTypeCreateViewAs))
