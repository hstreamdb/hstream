{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE DeriveAnyClass    #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE PatternSynonyms   #-}

module HStream.Utils.RPC
  ( HStreamClientApi

  , mkClientNormalRequest
  , mkServerErrResp
  , returnErrResp
  , returnResp
  , returnServerStreamingResp
  , returnBiDiStreamingResp
  , returnCommandQueryResp
  , returnCommandQueryEmptyResp
  , getServerResp
  , getProtoTimestamp
  , isSuccessful
  , pattern EnumPB
  , showNodeStatus
  , TaskStatus (Created, Creating, Running, CreationAbort, ConnectionAbort, Terminated, ..)
  ) where

import           Data.Aeson                    (FromJSON, ToJSON)
import           Data.Swagger                  (ToSchema)
import qualified Data.Vector                   as V
import           GHC.Generics                  (Generic)
import           Network.GRPC.HighLevel.Client
import           Network.GRPC.HighLevel.Server
import qualified Proto3.Suite                  as PB
import           Z.Data.JSON                   (JSON)
import           Z.IO.Time                     (SystemTime (..), getSystemTime')

import qualified Data.Text                     as T
import           HStream.Server.HStreamApi
import           HStream.ThirdParty.Protobuf   (Struct, Timestamp (..))

type HStreamClientApi = HStreamApi ClientRequest ClientResult

mkClientNormalRequest :: Int -> a -> ClientRequest 'Normal a b
mkClientNormalRequest requestTimeout x = ClientNormalRequest x requestTimeout (MetadataMap mempty)

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
returnCommandQueryResp v = do
  let resp = CommandQueryResponse v
  return (ServerNormalResponse (Just resp) mempty StatusOk "")
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
    ClientErrorResponse err -> ioError . userError $ "Server error happened: " <> show err
{-# INLINE getServerResp #-}

getProtoTimestamp :: IO Timestamp
getProtoTimestamp = do
  MkSystemTime sec nano <- getSystemTime'
  return $ Timestamp sec (fromIntegral nano)

isSuccessful :: ClientResult 'Normal a -> Bool
isSuccessful (ClientNormalResponse _ _ _ StatusOk _) = True
isSuccessful _                                       = False

pattern EnumPB :: a -> PB.Enumerated a
pattern EnumPB x = PB.Enumerated (Right x)

showNodeStatus :: PB.Enumerated NodeState -> T.Text
showNodeStatus = \case
  EnumPB NodeStateStarting    -> "Starting"
  EnumPB NodeStateRunning     -> "Running"
  EnumPB NodeStateUnavailable -> "Unavailable"
  EnumPB NodeStateDead        -> "Dead"
  _                           -> "Unknown"

-- A type synonym could also work but the pattern synonyms defined below cannot
-- be bundled with a type synonym when other modules import these definitions
newtype TaskStatus = TaskStatus { getPBStatus :: PB.Enumerated TaskStatusPB}
  deriving (Generic, Show, JSON, Eq, ToJSON, FromJSON, ToSchema)

instance JSON (PB.Enumerated TaskStatusPB)
instance JSON TaskStatusPB
instance ToJSON (PB.Enumerated TaskStatusPB)
instance FromJSON (PB.Enumerated TaskStatusPB)

-- | A task for running connectors, views, and queries has one of the following
-- states:
--
-- * Running: The task is running on a working thread
--
-- * Terminated: The task has stopped as per user request and the thread running
-- the task is killed
--
-- * ConnectionAbort: The task has stopped due to an error occurred when the
-- thread is running, e.g. the execution of a SQL command by the connector
-- failed
--
-- The rest of the states are specific to connectors:
--
-- * Creating: The server has received the task and started connecting to the
-- external database system
--
-- * Created: The connection with the external database system has been
-- established but the worker thread has not started running yet
--
-- * CreationAbort: An error occurred when connecting to the external database

pattern Running :: TaskStatus
pattern Running = TaskStatus (PB.Enumerated (Right TaskStatusPBTASK_RUNNING))

pattern Terminated :: TaskStatus
pattern Terminated = TaskStatus (PB.Enumerated (Right TaskStatusPBTASK_TERMINATED))

pattern ConnectionAbort :: TaskStatus
pattern ConnectionAbort = TaskStatus (PB.Enumerated (Right TaskStatusPBTASK_CONNECTION_ABORT))

pattern Creating :: TaskStatus
pattern Creating = TaskStatus (PB.Enumerated (Right TaskStatusPBTASK_CREATING))

pattern Created :: TaskStatus
pattern Created = TaskStatus (PB.Enumerated (Right TaskStatusPBTASK_CREATED))

pattern CreationAbort :: TaskStatus
pattern CreationAbort = TaskStatus (PB.Enumerated (Right TaskStatusPBTASK_CREATION_ABORT))
