{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs     #-}
module HStream.Utils.RPC
  ( HStreamClientApi

  , mkServerErrResp
  , returnErrResp
  , returnResp
  , returnStreamingResp
  , returnCommandQueryResp
  , returnCommandQueryEmptyResp
  , getServerResp
  , getProtoTimestamp
  , isSuccessful
  ) where

import qualified Data.Vector                   as V
import           Network.GRPC.HighLevel.Client
import           Network.GRPC.HighLevel.Server
import           Z.IO.Time                     (SystemTime (..), getSystemTime')

import           HStream.Server.HStreamApi
import           HStream.ThirdParty.Protobuf   (Struct, Timestamp (..))

type HStreamClientApi = HStreamApi ClientRequest ClientResult

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

returnStreamingResp :: Monad m => StatusCode -> StatusDetails -> m (ServerResponse 'ServerStreaming a)
returnStreamingResp code = return . ServerWriterResponse mempty code
{-# INLINE returnStreamingResp #-}

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
