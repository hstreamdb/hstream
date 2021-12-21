{-# OPTIONS_GHC -Wno-orphans   #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.Common.ConsistentHashing
  (
    HashRing(..)
  , Key(..)

  , empty
  , insert
  , delete
  , lookupHashed
  , lookupKey
  , constructHashRing

  , getAllocatedNode
  , size
  ) where

import qualified Data.HashMap.Strict       as HM
import           Data.Hashable
import qualified Data.Map                  as M
import           Data.Maybe                (fromMaybe)
import qualified Data.Text                 as T
import           Data.Word                 (Word32)
import           Prelude                   hiding (lookup, null)

import           HStream.Server.HStreamApi (ServerNode (..))

data HashRing = HashRing
  { nodeMap  :: M.Map Int ServerNode
  , vnodes   :: HM.HashMap Word32 [Int]
  , replicas :: Int
  }
  deriving (Eq, Show)

empty :: HashRing
empty = HashRing M.empty HM.empty 5

lookupHashed :: Int -> HashRing -> ServerNode
lookupHashed h HashRing {..} = case M.lookupGE h nodeMap of
  Just (_, node) -> node
  Nothing        -> snd $ M.findMin nodeMap

lookupKey :: Key -> HashRing -> ServerNode
lookupKey k HashRing {..} = case M.lookupGE (hash k) nodeMap of
  Just (_, node) -> node
  Nothing        -> snd $ M.findMin nodeMap

insert :: ServerNode -> HashRing -> HashRing
insert node@ServerNode{..} ring@HashRing {..}
  | HM.member serverNodeId vnodes = ring
  | otherwise = HashRing
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
