{-# OPTIONS_GHC -Wno-orphans   #-}

module HStream.Common.ConsistentHashing
  ( HashRing(..)
  , Key(..)

  , constructHashRing
  , empty
  , insert
  , delete
  , lookupHashed
  , lookupKey

  , getAllocatedNode
  , getAllocatedNodeId
  , size
  ) where

import           Data.Foldable             (foldr')
import           Data.Hashable
import qualified Data.HashMap.Strict       as HM
import           Data.IntMap.Strict        (IntMap)
import qualified Data.IntMap.Strict        as IntMap
import           Data.Maybe                (fromMaybe)
import qualified Data.Text                 as T
import           Data.Word                 (Word32)
import           Prelude                   hiding (lookup, null)

import           HStream.Server.HStreamApi (ServerNode (..))

type ServerNodeId = Word32

data HashRing = HashRing
  { nodeMap  :: !(IntMap ServerNode)
    -- ^ Map of { HashedKey: ServerNode }
  , vnodes   :: !(HM.HashMap ServerNodeId [Int])
    -- ^ Virtual nodes, we will rehash by 'replicas' to generate this map.
    -- The value is a list of HashedKey.
  , replicas :: !Int
  } deriving (Eq, Show)

empty :: HashRing
empty = HashRing IntMap.empty HM.empty 5

lookupHashed :: Int -> HashRing -> ServerNode
lookupHashed h HashRing {..} = case IntMap.lookupGE h nodeMap of
  Just (_, node) -> node
  Nothing        -> snd $ IntMap.findMin nodeMap

lookupKey :: Key -> HashRing -> ServerNode
lookupKey k HashRing {..} = case IntMap.lookupGE (hash k) nodeMap of
  Just (_, node) -> node
  Nothing        -> snd $ IntMap.findMin nodeMap

insert :: ServerNode -> HashRing -> HashRing
insert node@ServerNode{..} ring@HashRing {..}
  | HM.member serverNodeId vnodes = ring
  | otherwise = HashRing
  { nodeMap  = foldr' (`IntMap.insert` node) nodeMap keys
  , vnodes   = HM.insert serverNodeId keys vnodes
  , replicas = replicas
  }
  where
    keys = take replicas
      $ filter (\key -> not $ IntMap.member key nodeMap)
      $ generateVirtualNodes node

delete :: ServerNode -> HashRing -> HashRing
delete ServerNode{..} HashRing {..} = HashRing
  { nodeMap  = foldr IntMap.delete nodeMap keys
  , vnodes   = HM.delete serverNodeId vnodes
  , replicas = replicas
  }
  where
    keys = fromMaybe [] (HM.lookup serverNodeId vnodes)

size :: HashRing -> Int
size HashRing{..} = HM.size vnodes

constructHashRing :: [ServerNode] -> HashRing
constructHashRing = foldr insert empty

generateVirtualNodes :: ServerNode -> [Int]
generateVirtualNodes node = (`hashWithSalt` node) <$> [0..]

instance Hashable ServerNode where
  hashWithSalt = flip hashServerNode

newtype Key = Key T.Text
  deriving Show

instance Hashable Key where
  hashWithSalt = hashKey

--------------------------------------------------------------------------------

-- Modify the following two functions to change hash function
hashServerNode :: ServerNode -> Int -> Int
hashServerNode ServerNode{..} x
  = x `hashWithSalt` show serverNodeId
      `hashWithSalt` serverNodeHost
      `hashWithSalt` serverNodePort

hashKey :: Int -> Key -> Int
hashKey salt (Key key) = salt `hashWithSalt` hash key

getAllocatedNode :: HashRing -> T.Text -> ServerNode
getAllocatedNode hashRing key = lookupKey (Key key) hashRing

getAllocatedNodeId :: HashRing -> T.Text -> ServerNodeId
getAllocatedNodeId = (serverNodeId .) . getAllocatedNode
