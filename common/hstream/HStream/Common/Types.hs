module HStream.Common.Types
  ( fromInternalServerNode
  , fromInternalServerNodeWithKey
  , getHStreamVersion
  ) where

import qualified Data.Map.Strict                as Map
import           Data.Text                      (Text)
import qualified Data.Text                      as Text
import qualified Data.Text.Encoding             as Text
import qualified Data.Vector                    as V

import           Data.Maybe                     (fromMaybe)
import qualified HStream.Logger                 as Log
import qualified HStream.Server.HStreamApi      as A
import qualified HStream.Server.HStreamInternal as I
import           HStream.Version                (hstreamCommit, hstreamVersion)

-- | Simple convert internal ServerNode to client known ServerNode
fromInternalServerNode :: I.ServerNode -> A.ServerNode
fromInternalServerNode I.ServerNode{..} =
  A.ServerNode { serverNodeId   = serverNodeId
               , serverNodeHost = Text.decodeUtf8 serverNodeAdvertisedAddress
               , serverNodePort = serverNodePort
               , serverNodeVersion = Nothing
               }

fromInternalServerNodeWithKey :: Maybe Text -> I.ServerNode -> IO (V.Vector A.ServerNode)
fromInternalServerNodeWithKey Nothing I.ServerNode{..} = pure . V.singleton $
  A.ServerNode { serverNodeId   = serverNodeId
               , serverNodeHost = Text.decodeUtf8 serverNodeAdvertisedAddress
               , serverNodePort = serverNodePort
               , serverNodeVersion = Nothing
               }
fromInternalServerNodeWithKey (Just key) I.ServerNode{..} =
  case Map.lookup key serverNodeAdvertisedListeners of
    Nothing -> do Log.warning "Unexpected happened! There may be misconfiguration(AdvertisedListeners) in hserver cluter."
                  pure V.empty
    Just Nothing -> do Log.warning $ "There is no AdvertisedListeners with key " <> Log.build key
                       pure V.empty
    Just (Just (I.ListOfListener xs)) -> pure $ V.map (\I.Listener{..} ->
      A.ServerNode { serverNodeId   = serverNodeId
                   , serverNodeHost = listenerAddress
                   , serverNodePort = fromIntegral listenerPort
                   , serverNodeVersion = Nothing
                   }) xs

getHStreamVersion :: IO A.HStreamVersion
getHStreamVersion = do
  let tVersion = fromMaybe "unknown version" . Text.stripPrefix prefix $ Text.pack hstreamVersion
      tCommit = Text.pack hstreamCommit
      ver = A.HStreamVersion { hstreamVersionVersion = tVersion, hstreamVersionCommit = tCommit }
  return ver
 where
   prefix = Text.singleton 'v'
