{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs     #-}
module HStream.HTTP.Server.Utils where

import qualified Data.Map                         as Map
import qualified HStream.Logger                   as Log
import           Network.GRPC.HighLevel.Generated

getServerResp :: ClientResult 'Normal a -> IO (Maybe a)
getServerResp result = do
  case result of
    ClientNormalResponse x _meta1 _meta2 StatusOk _details -> return (Just x)
    ClientNormalResponse _resp _meta1 _meta2 _status _details -> do
      Log.e . Log.buildString $ "Impossible happened..." <> show _status
      return Nothing
    ClientErrorResponse err -> do
      Log.e . Log.buildString $ "Server error happened: " <> show err
      return Nothing

mkClientNormalRequest :: a -> ClientRequest 'Normal a b
mkClientNormalRequest x = ClientNormalRequest x 100 (MetadataMap Map.empty)
