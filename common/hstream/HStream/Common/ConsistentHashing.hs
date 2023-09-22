{-# OPTIONS_GHC -Wno-orphans   #-}

module HStream.Common.ConsistentHashing
  ( ServerMap
  , HashRing
  , constructServerMap
  , insert
  , delete
  , size
  , getAllocatedNode
  , getAllocatedNodeId
  , getResNode
  ) where

import           Control.Exception              (throwIO)
import           Data.Hashable                  (hash)
import qualified Data.Map.Strict                as M
import qualified Data.Text                      as T
import qualified Data.Vector                    as V
import           Data.Word                      (Word32, Word64)
import           Prelude                        hiding (lookup, null)

import           HStream.Common.Types           (fromInternalServerNodeWithKey)
import qualified HStream.Exception              as HE
import qualified HStream.Server.HStreamApi      as A
import qualified HStream.Server.HStreamInternal as I

--------------------------------------------------------------------------------

getResNode :: HashRing -> T.Text -> Maybe T.Text -> IO A.ServerNode
getResNode hashRing hashKey listenerKey = do
  let serverNode = getAllocatedNode hashRing hashKey
  theNodes <- fromInternalServerNodeWithKey listenerKey serverNode
  if V.null theNodes then throwIO $ HE.NodesNotFound "Got empty nodes"
                     else pure $ V.head theNodes

--------------------------------------------------------------------------------

getAllocatedNodeId :: ServerMap -> T.Text -> ServerNodeId
getAllocatedNodeId = (I.serverNodeId .) . getAllocatedNode

getAllocatedNode :: ServerMap -> T.Text -> I.ServerNode
getAllocatedNode nodes k =
  snd $ M.elemAt serverNum nodes
  where
    serverNum = fromIntegral (c_get_allocated_num key nums)
    nums = fromIntegral $ size nodes
    key  = fromIntegral $ hash k

--------------------------------------------------------------------------------

type ServerNodeId = Word32

type HashRing = ServerMap
type ServerMap = M.Map Word32 I.ServerNode

insert :: I.ServerNode -> ServerMap -> ServerMap
insert node@I.ServerNode{..} = M.insert serverNodeId node

delete :: I.ServerNode -> ServerMap -> ServerMap
delete I.ServerNode{..} = M.delete serverNodeId

size :: ServerMap -> Int
size = M.size

constructServerMap :: [I.ServerNode] -> ServerMap
constructServerMap = foldr insert M.empty

-------------------------------------------------------------------------------

foreign import ccall unsafe "hs_common.h get_allocated_num"
  c_get_allocated_num ::  Word64 -> Word64 -> Word64
