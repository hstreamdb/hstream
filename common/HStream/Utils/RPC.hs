{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs     #-}
module HStream.Utils.RPC
  ( mkServerErrResp
  , returnErrResp
  , getServerResp
  ) where

import           Network.GRPC.HighLevel.Client
import           Network.GRPC.HighLevel.Server

mkServerErrResp :: StatusCode -> StatusDetails -> ServerResponse 'Normal a
mkServerErrResp = ServerNormalResponse Nothing mempty
{-# INLINE mkServerErrResp #-}

returnErrResp
  :: Monad m
  => StatusCode -> StatusDetails -> m (ServerResponse 'Normal a)
returnErrResp = (return .) . mkServerErrResp
{-# INLINE returnErrResp #-}

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
