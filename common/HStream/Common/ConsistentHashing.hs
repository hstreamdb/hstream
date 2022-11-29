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
  ) where

import           Data.Hashable                  (hash)
import qualified Data.Map.Strict                as M
import qualified Data.Text                      as T
import           Data.Word                      (Word32, Word64)
import           Prelude                        hiding (lookup, null)

import           HStream.Server.HStreamInternal (ServerNode (..))

--------------------------------------------------------------------------------

getAllocatedNodeId :: ServerMap -> T.Text -> ServerNodeId
getAllocatedNodeId = (serverNodeId .) . getAllocatedNode

getAllocatedNode :: ServerMap -> T.Text -> ServerNode
getAllocatedNode nodes k =
  snd $ M.elemAt serverNum nodes
  where
    serverNum = fromIntegral (c_get_allocated_num key nums)
    nums = fromIntegral $ size nodes
    key  = fromIntegral $ hash k

--------------------------------------------------------------------------------

type ServerNodeId = T.Text

type HashRing = ServerMap
type ServerMap = M.Map ServerNodeId ServerNode

insert :: ServerNode -> ServerMap -> ServerMap
insert node@ServerNode{..} = M.insert serverNodeId node

delete :: ServerNode -> ServerMap -> ServerMap
delete ServerNode{..} = M.delete serverNodeId

size :: ServerMap -> Int
size = M.size

constructServerMap :: [ServerNode] -> ServerMap
constructServerMap = foldr insert M.empty

-------------------------------------------------------------------------------

foreign import ccall unsafe "hs_common.h get_allocated_num"
  c_get_allocated_num ::  Word64 -> Word64 -> Word64
