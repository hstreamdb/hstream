{-# LANGUAGE GeneralizedNewtypeDeriving #-}
module HStream.Common.Types
  ( fromInternalServerNode
  , fromInternalServerNodeWithKey
  , getHStreamVersion
  , ShardKey(..)
  , hashShardKey
  , keyToCBytes
  , cBytesToKey
  , devideKeySpace
  ) where

import           Data.Foldable                  (foldl')
import qualified Data.Map.Strict                as Map
import           Data.Text                      (Text)
import qualified Data.Text                      as Text
import qualified Data.Text.Encoding             as Text
import qualified Data.Vector                    as V

import qualified Crypto.Hash                    as CH
import           Data.Bits                      (Bits (..))
import qualified Data.ByteArray                 as BA
import           Data.Hashable                  (Hashable)
import           Data.List                      (iterate')
import           Data.Maybe                     (fromMaybe)
import           Data.Text.Encoding             (encodeUtf8)
import qualified HStream.Logger                 as Log
import qualified HStream.Server.HStreamApi      as A
import qualified HStream.Server.HStreamInternal as I
import           HStream.Version                (hstreamCommit, hstreamVersion)
import qualified Z.Data.CBytes                  as CB

-- | Simple convert internal ServerNode to client known ServerNode
fromInternalServerNode :: I.ServerNode -> A.ServerNode
fromInternalServerNode I.ServerNode{..} =
  A.ServerNode { serverNodeId   = serverNodeId
               , serverNodeHost = Text.decodeUtf8 serverNodeAdvertisedAddress
               , serverNodePort = serverNodePort
               , serverNodeVersion = convertHStreamVersion serverNodeVersion
               }

fromInternalServerNodeWithKey :: Maybe Text -> I.ServerNode -> IO (V.Vector A.ServerNode)
fromInternalServerNodeWithKey Nothing I.ServerNode{..} = pure . V.singleton $
  A.ServerNode { serverNodeId   = serverNodeId
               , serverNodeHost = Text.decodeUtf8 serverNodeAdvertisedAddress
               , serverNodePort = serverNodePort
               , serverNodeVersion = convertHStreamVersion serverNodeVersion
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
                   , serverNodeVersion = convertHStreamVersion serverNodeVersion
                   }) xs

convertHStreamVersion :: Maybe I.HStreamVersion -> Maybe A.HStreamVersion
convertHStreamVersion vs = do
  I.HStreamVersion{..} <- vs
  return A.HStreamVersion{..}

getHStreamVersion :: IO A.HStreamVersion
getHStreamVersion = do
  let tVersion = fromMaybe "unknown version" . Text.stripPrefix prefix $ Text.pack hstreamVersion
      tCommit = Text.pack hstreamCommit
      ver = A.HStreamVersion { hstreamVersionVersion = tVersion, hstreamVersionCommit = tCommit }
  return ver
 where
   prefix = Text.singleton 'v'

newtype ShardKey = ShardKey Integer
  deriving (Show, Eq, Ord, Integral, Real, Enum, Num, Hashable)

instance Bounded ShardKey where
  minBound = ShardKey 0
  maxBound = ShardKey ((1 `shiftL` 128) - 1)

hashShardKey :: Text -> ShardKey
hashShardKey key =
  let w8KeyList = BA.unpack (CH.hash . encodeUtf8 $ key :: CH.Digest CH.MD5)
   in ShardKey $ foldl' (\acc c -> (.|.) (acc `shiftL` 8) (fromIntegral c)) (0 :: Integer) w8KeyList

keyToCBytes :: ShardKey -> CB.CBytes
keyToCBytes (ShardKey key) = CB.pack . show $ key

cBytesToKey :: CB.CBytes -> ShardKey
cBytesToKey = ShardKey . read . CB.unpack

-- Devide the key space into N parts, return [(startKey, endKey)]
devideKeySpace :: Int -> [(ShardKey, ShardKey)]
devideKeySpace num =
  let startKeys = take num $ iterate' (+cnt) minBound
      cnt = maxBound @ShardKey `div` fromIntegral num
   in zipWith (\idx s -> if idx == num then (s, maxBound) else (s, s + cnt - 1)) [1..] startKeys

