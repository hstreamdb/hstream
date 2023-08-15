{-# LANGUAGE DeriveAnyClass       #-}
{-# LANGUAGE DeriveGeneric        #-}
{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE RecordWildCards      #-}
{-# LANGUAGE TypeFamilies         #-}
{-# LANGUAGE TypeSynonymInstances #-}

module HStream.SQL.PlannerNew.Types where

import           Control.Applicative  ((<|>))
import           Control.Monad.Reader
import           Control.Monad.State
import qualified Data.Aeson           as Aeson
import           Data.Function        (on)
import           Data.Hashable
import qualified Data.HashMap.Strict  as HM
import           Data.Int             (Int64)
import           Data.IntMap          (IntMap)
import qualified Data.IntMap          as IntMap
import           Data.Kind            (Type)
import qualified Data.List            as L
import           Data.Maybe           (fromMaybe)
import           Data.Set             (Set)
import qualified Data.Set             as Set
import           Data.Text            (Text)
import qualified Data.Text            as T
import           GHC.Generics         (Generic)
import           GHC.Stack

import           HStream.SQL.Binder   hiding (lookupColumn, (<::>))

type AggregateExpr = Aggregate ScalarExpr

data RelationExpr
  = StreamScan Schema
  | LoopJoinOn Schema RelationExpr RelationExpr ScalarExpr BoundJoinType Int64
  | Filter     Schema RelationExpr ScalarExpr
  | Project    Schema RelationExpr [ScalarExpr]
  | Reduce     Schema RelationExpr [ScalarExpr]    -- Note: indexes start from 0 in schema
                                   [AggregateExpr] -- Note: indexes start from [GROUPBY COLUMNS], not 0!
                                   (Maybe WindowType)
  | Distinct   Schema RelationExpr
  | Union      Schema RelationExpr RelationExpr

relationExprSchema :: RelationExpr -> Schema
relationExprSchema relation = case relation of
  StreamScan schema           -> schema
  LoopJoinOn schema _ _ _ _ _ -> schema
  Filter     schema _ _       -> schema
  Project    schema _ _       -> schema
  Reduce     schema _ _ _ _   -> schema
  Distinct   schema _         -> schema
  Union      schema _ _       -> schema

setRelationExprSchema :: Schema -> RelationExpr -> RelationExpr
setRelationExprSchema schema relation = case relation of
  StreamScan _             -> StreamScan schema
  LoopJoinOn _ r1 r2 e j t -> LoopJoinOn schema r1 r2 e j t
  Filter     _ r e         -> Filter schema r e
  Project    _ r e         -> Project schema r e
  Reduce     _ r e a w     -> Reduce schema r e a w
  Distinct   _ r           -> Distinct schema r
  Union      _ r1 r2       -> Union schema r1 r2

data ScalarExpr
  = ColumnRef   Int Int -- stream, column
  | Literal     Constant
  | CallUnary   UnaryOp  ScalarExpr
  | CallBinary  BinaryOp ScalarExpr ScalarExpr
  | CallTernary TerOp    ScalarExpr ScalarExpr ScalarExpr
  | CallCast    ScalarExpr BoundDataType
  | CallJson    JsonOp ScalarExpr ScalarExpr
  | ValueArray  [ScalarExpr]
  | AccessArray ScalarExpr BoundArrayAccessRhs

instance Show ScalarExpr where
  show expr = case expr of
                ColumnRef si ci -> show si <> ".#" <> show ci
                Literal   constant       -> show constant
                CallUnary op e           -> show op <> "(" <> show e <> ")"
                CallBinary op e1 e2      -> show op <> "(" <> show e1 <> "," <> show e2 <> ")"
                CallTernary op e1 e2 e3  -> show op <> "(" <> show e1 <> "," <> show e2 <> "," <> show e3 <> ")"
                CallCast e typ           -> show e <> "::" <> show typ
                CallJson op e1 e2        -> show e1 <> show op <> show e2
                ValueArray arr           -> "Array" <> show arr
                AccessArray e rhs        -> show e <> show rhs

----------------------------------------
--         planner context
----------------------------------------
data PlanContext = PlanContext
  { planContextSchemas   :: IntMap Schema
  , planContextBasicRefs :: HM.HashMap (Text,Int) (Text,Int)
  -- absolute -> relative e.g. s1.0 -> _join#0.1
  -- this is only used in `lookupColumn`.
  -- FIXME: design a better structure to handle relative schemas
  } deriving (Show)

defaultPlanContext :: PlanContext
defaultPlanContext = PlanContext mempty mempty

-- | Compose two 'IntMap's of stream schemas with their indexes. It assumes
-- that the two 'IntMap's have contiguous indexes starting from 0 in their
-- keys. The result 'IntMap' will still have contiguous indexes starting from 0
-- and of course, end with the sum of the two 'IntMap's' sizes. Also, the
-- `columnStreamId` of the schemas will also be updated.
-- Example: m1 = {0: sc1, 1: sc2}, m2 = {0: sc3}
--          m1 <::> m2 = {0: sc1, 1: sc2, 2: sc3}
(<::>) :: IntMap Schema -> IntMap Schema -> IntMap Schema
(<::>) m1 m2 =
  let tups1 = IntMap.toList m1
      tups2 = IntMap.toList m2
      maxId1 = if null tups1 then (-1) else maximum (map fst tups1)
      tups2' = map (\(i,s) -> (i+maxId1+1, setSchemaStreamId (i+maxId1+1) s)) tups2
   in IntMap.fromList (tups1 <> tups2')

instance Semigroup PlanContext where
  (PlanContext s1 hm1) <> (PlanContext s2 hm2) = PlanContext (s1 <::> s2) (hm1 `HM.union` hm2)
instance Monoid PlanContext where
  mempty = PlanContext mempty mempty

-- | Lookup a certain column in the planning context with
--   stream name and column index. Return the index of
--   matched stream, the real index of the column
--   and the catalog of matched column.
--   WARNING: The returned stream index may not be the same
--            as the `streamId` of the column! This should
--            be fixed in the future.
lookupColumn :: PlanContext -> Text -> Int -> Maybe (Int, Int, ColumnCatalog)
lookupColumn (PlanContext m baseRefs) streamName colId =
  let (streamName', colId') = go (streamName, colId)
   in L.foldr (\(i,Schema{..}) acc -> case acc of
                  Just _  -> acc
                  Nothing ->
                    let catalogTup_m = L.find (\(n, ColumnCatalog{..}) ->
                                                 columnStream == streamName' &&
                                                 columnId     == colId'
                                              ) (IntMap.toList schemaColumns)
                     in (fmap (\(n,catalog) -> (i,n,catalog)) catalogTup_m) <|> acc
              ) Nothing (IntMap.toList m)
  where go (s,i) = case HM.lookup (s,i) baseRefs of
                     Just (s',i') -> if (s,i) == (s',i')
                                       then (s,i)
                                       else go (s',i')
                     Nothing      -> (s,i)

-- | Lookup a certain column name in the planning context. Return the index of
--   matched stream, the real index of the column
--   and the catalog of matched column.
--   WARNING: The returned stream index may not be the same
--            as the `columnStreamId` of the column! This should
--            be fixed in the future.
lookupColumnName :: PlanContext -> Text -> Maybe (Int, Int, ColumnCatalog)
lookupColumnName (PlanContext m _) k =
  L.foldr (\(i,Schema{..}) acc -> case acc of
              Just _  -> acc
              Nothing ->
                let catalogTup_m = L.find (\(n, ColumnCatalog{..}) ->
                                          columnName == k
                                       ) (IntMap.toList schemaColumns)
                 in (fmap (\(n,catalog) -> (i,n,catalog)) catalogTup_m) <|> acc
          ) Nothing (IntMap.toList m)

----------------------------------------
--            Some helpers
----------------------------------------
-- Set the 'streamId' field in a 'Schema' to a given value.
-- This is useful during planning. For example, most of
-- nodes are 1-in and 1-out so we set the 'streamId' to 0.
-- And for 'Join' nodes, we set two input streams to 0 and 1.
setSchemaStreamId :: Int -> Schema -> Schema
setSchemaStreamId n Schema{..} =
  Schema { schemaOwner = schemaOwner
         , schemaColumns = IntMap.map (\catalog -> catalog{columnStreamId = n}) schemaColumns
         }

setSchemaStream :: Text -> Schema -> Schema
setSchemaStream streamName Schema{..} =
  Schema { schemaOwner = streamName
         , schemaColumns = IntMap.map (\catalog -> catalog{columnStream = streamName}) schemaColumns
         }

----------------------------------------
--            Plan class
----------------------------------------
type family PlannedType a :: Type
class Plan a where
  plan  :: ( HasCallStack
           , MonadIO m
           , MonadReader (Text -> IO (Maybe Schema)) m
           , MonadState PlanContext m
           ) => a -> m (PlannedType a)
