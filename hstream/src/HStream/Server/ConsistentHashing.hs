{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.Server.ConsistentHashing
  ( empty
  , lookup
  , insert
  , delete
  , constructHashRing
  , getAllocatedNode
  , updateHashRing
  ) where

import           Control.Concurrent         (MVar, putMVar, readMVar, takeMVar)
import           Data.ByteString            (ByteString)
import qualified Data.ByteString            as BS
import qualified Data.ByteString.Internal   as BSI
import           Data.Digest.CRC32          (CRC32 (..))
import qualified Data.HashMap.Strict        as HM
import qualified Data.Map                   as M
import           Data.Maybe                 (fromMaybe)
import qualified Data.Text                  as T
import           Data.Text.Encoding         (encodeUtf8)
import           Data.Word                  (Word32)
import           Foreign                    (pokeByteOff)
import           Prelude                    hiding (lookup, null)
import           ZooKeeper                  (zooWatchGetChildren)
import           ZooKeeper.Types

import           HStream.Server.HStreamApi  (ServerNode (..))
import           HStream.Server.Persistence (getServerNode', serverRootPath)
import           HStream.Server.Types       (HashRing (..), ServerContext (..))

getAllocatedNode :: ServerContext -> T.Text -> IO ServerNode
getAllocatedNode ServerContext{..} key = do
  hashRing <- readMVar loadBalanceHashRing
  return $ lookup (encodeUtf8 key) hashRing

empty :: Int -> HashRing
empty = HashRing M.empty HM.empty

lookup :: ByteString -> HashRing -> ServerNode
lookup msg HashRing {..} = case M.lookupGE (crc32 msg) nodeMap of
  Just (_, node) -> node
  Nothing        -> snd $ M.findMin nodeMap

insert :: ServerNode -> HashRing -> HashRing
insert node@ServerNode{..} HashRing {..} = HashRing
  { nodeMap  = foldr (`M.insert` node) nodeMap keys
  , vnodes   = HM.insert serverNodeId keys vnodes
  , replicas = replicas
  }
  where
    keys = take replicas
      $ filter (\key -> not $ M.member key nodeMap)
      $ generateVirtualNodes node

delete :: ServerNode -> HashRing -> HashRing
delete ServerNode{..} HashRing {..} = HashRing
  { nodeMap  = foldr M.delete nodeMap keys
  , vnodes   = HM.delete serverNodeId vnodes
  , replicas = replicas
  }
  where
    keys = fromMaybe [] (HM.lookup serverNodeId vnodes)

constructHashRing :: Int -> [ServerNode] -> HashRing
constructHashRing n = foldr insert (empty n)

--------------------------------------------------------------------------------

generateVirtualNodes :: ServerNode -> [Word32]
generateVirtualNodes node = crc32Update (crc32 . encodeServerNode $ node) . BS.singleton <$> [0..]

encodeServerNode :: ServerNode -> ByteString
encodeServerNode ServerNode{..}
  =  encodeWord32 serverNodeId
  <> encodeUtf8   serverNodeHost
  <> encodeWord32 serverNodePort

encodeWord32 :: Word32 -> ByteString
encodeWord32 w32 =
  BSI.unsafeCreate 4 $ \p -> pokeByteOff p 0 w32

updateHashRing :: ZHandle -> MVar HashRing -> IO ()
updateHashRing zk mhr = do
  zooWatchGetChildren zk serverRootPath
    callback app
  where
    callback HsWatcherCtx {..} =
      updateHashRing watcherCtxZHandle mhr
    app (StringsCompletion (StringVector children)) = do
      _ <- takeMVar mhr
      serverNodes <- mapM (getServerNode' zk) children
      let hr' = constructHashRing 7 serverNodes
      putMVar mhr hr'
