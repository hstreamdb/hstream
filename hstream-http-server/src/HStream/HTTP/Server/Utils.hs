{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs     #-}
module HStream.HTTP.Server.Utils where

import           Control.Monad.IO.Class           (liftIO)
import qualified Data.ByteString.Lazy             as BSL
import qualified Data.Map                         as Map
import qualified HStream.Logger                   as Log
import           Network.GRPC.HighLevel.Generated
import           Servant                          hiding (Stream)

getServerResp :: ClientResult 'Normal a -> IO (Maybe a)
getServerResp result = do
  case result of
    ClientNormalResponse x _meta1 _meta2 StatusOk _details -> return (Just x)
    ClientNormalResponse _resp _meta1 _meta2 status _details -> do
      Log.e . Log.buildString $ "Impossible happened..." <> show status
      return Nothing
    ClientErrorResponse err -> do
      Log.e . Log.buildString $ "Server error happened: " <> show err
      return Nothing

getServerResp' :: ClientResult 'Normal a -> Handler a
getServerResp' result = do
  case result of
    ClientNormalResponse x _meta1 _meta2 StatusOk _details -> pure x
    ClientNormalResponse _resp _meta1 _meta2 status _details -> do
      liftIO $ Log.e . Log.buildString $ "Impossible happened..." <> show status
      throwError $ err500 { errBody = coeErrStr $ "Impossible happened..." <> show status }

    ClientErrorResponse err -> do
      liftIO $ Log.e . Log.buildString $ "Server error happened: " <> show err
      throwError $ err500 { errBody = coeErrStr $ "Server error happened: " <> show err }

mkClientNormalRequest :: a -> ClientRequest 'Normal a b
mkClientNormalRequest x = ClientNormalRequest x 100 (MetadataMap Map.empty)

--------------------------------------------------------------------------------
coeErrStr :: String -> BSL.ByteString
coeErrStr = BSL.pack . map (fromIntegral . fromEnum)
