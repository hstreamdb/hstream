{-# LANGUAGE CPP                  #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE RecordWildCards      #-}
{-# LANGUAGE ScopedTypeVariables  #-}
{-# LANGUAGE TypeFamilies         #-}
{-# LANGUAGE TypeSynonymInstances #-}

module HStream.SQL.PlannerNew (
#ifdef HStreamEnableSchema
    module HStream.SQL.PlannerNew.Types
  , module HStream.SQL.PlannerNew.Expr
  , module HStream.SQL.PlannerNew.Pretty
  , module HStream.SQL.PlannerNew.Extra
  , planIO
  , planning
#endif
  ) where

#ifdef HStreamEnableSchema
import           Control.Applicative           ((<|>))
import           Control.Monad.State
import           Data.Function                 (on)
import           Data.Functor                  ((<&>))
import           Data.Int                      (Int64)
import           Data.IntMap                   (IntMap)
import qualified Data.IntMap                   as IntMap
import           Data.Kind                     (Type)
import qualified Data.List                     as L
import qualified Data.Map                      as Map
import           Data.Maybe                    (fromJust, fromMaybe)
import           Data.Text                     (Text)
import qualified Data.Text                     as T
import           GHC.Stack

import           HStream.SQL.Binder            hiding (lookupColumn)
import           HStream.SQL.Exception
import           HStream.SQL.Extra
import           HStream.SQL.ParseNew
import           HStream.SQL.PlannerNew.Expr
import           HStream.SQL.PlannerNew.Extra
import           HStream.SQL.PlannerNew.Pretty
import           HStream.SQL.PlannerNew.Types

----
type instance PlannedType BoundTableRef = RelationExpr
instance Plan BoundTableRef where
  plan (BoundTableRefSimple name) = do
    schema <- liftIO $ getSchema name <&> fromJust -- FIXME: fromJust
    -- 1-in/1-out
    let schema' = setSchemaStreamId 0 schema
    let ctx = PlanContext (IntMap.singleton 0 schema')
    put ctx
    return $ StreamScan schema'

  plan (BoundTableRefSubquery name sel) = do
    relationExpr <- plan sel
    -- 1-in/1-out
    let schema = setSchemaStream name $
          setSchemaStreamId 0 (relationExprSchema relationExpr)
    let ctx = PlanContext (IntMap.singleton 0 schema)
    put ctx
    return relationExpr

  -- Note: As `HStream.SQL.Binder.Select` says, 'BoundTableRefWindowed' is
  -- only used when binding 'SELECT'. So we can ignore the window part here
  -- because it has been already absorbed into the 'BoundGroupBy' part.
  plan (BoundTableRefWindowed name ref win) = plan ref

  plan (BoundTableRefJoin name ref1 typ ref2 expr interval) = do
    -- 2-in!
    relationExpr1 <- plan ref1
    let schema1 = relationExprSchema relationExpr1
    let schema1' = setSchemaStreamId 0 schema1
        relationExpr1' = setRelationExprSchema schema1' relationExpr1
    ctx1 <- get

    relationExpr2 <- plan ref2
    let schema2 = relationExprSchema relationExpr2
    let schema2' = setSchemaStreamId 1 schema2
        relationExpr2' = setRelationExprSchema schema2' relationExpr2
    ctx2 <- get

    let win = calendarDiffTimeToMs interval

    -- 1-out!
    let schema = setSchemaStream name $
          setSchemaStreamId 0 $ Schema
                 { schemaOwner = name
                 , schemaColumns = schemaColumns schema1
                             <:+:> schemaColumns schema2
                 }
    put $ PlanContext
      { planContextSchemas = IntMap.map (setSchemaStreamId 0)
                               (planContextSchemas ctx1 <::> planContextSchemas ctx2)
      }
    (scalarExpr,_) <- plan expr

    return $ LoopJoinOn schema relationExpr1' relationExpr2' scalarExpr typ win

type instance PlannedType BoundFrom = RelationExpr
instance Plan BoundFrom where
  plan (BoundFrom ref) = plan ref

type instance PlannedType BoundWhere = (RelationExpr -> RelationExpr)
instance Plan BoundWhere where
  plan BoundWhereEmpty   = return id
  plan (BoundWhere expr) = do
    (scalarExpr,_) <- plan expr
    return $ \upstream -> Filter (relationExprSchema upstream) upstream scalarExpr

data BoundAgg = BoundAgg
  { boundAggGroupBy :: BoundGroupBy
  , boundAggSel     :: BoundSel
  } deriving Show

type instance PlannedType BoundAgg = (RelationExpr -> RelationExpr)
instance Plan BoundAgg where
  plan BoundAgg{..} = case boundAggGroupBy of
    BoundGroupByEmpty           -> return id -- FIXME: validate no aggs
    BoundGroupBy grpExprs win_m -> do
      ctx_old <- get
      (grps :: [(ScalarExpr, ColumnCatalog)]) <- mapM plan grpExprs

      let (BoundSel (BoundSelectItemProject selTups)) = boundAggSel
      (aggTups :: [(AggregateExpr, ColumnCatalog)])
        <- (mapM (\(boundExpr,alias_m) -> do
                     -- Note: a expr can have multiple aggs: `SUM(a) + MAX(b)`
                     let boundAggExprs = exprAggregates boundExpr
                     mapM (\boundAggExpr -> do
                              let (BoundExprAggregate name boundAgg) = boundAggExpr
                              (agg,catalog) <- plan boundAgg
                              let catalog' = catalog { columnName = T.pack name } -- FIXME
                              return (agg,catalog')
                          ) boundAggExprs
                 ) selTups
           ) <&> L.concat

      let grpsIntmap = IntMap.fromList $ L.map (\(i, (_,catalog)) -> (i, catalog)
                                               ) ([0..] `zip` grps)
          aggsIntmap = IntMap.fromList $ L.map (\(i, (_,catalog)) -> (i, catalog)
                                               ) ([0..] `zip` aggTups)
      let new_schema = Schema { schemaOwner = "" -- FIXME
                              , schemaColumns = grpsIntmap <:+:> aggsIntmap
                              }
      let ctx_new = PlanContext (IntMap.singleton 0 new_schema)
      put ctx_new
      return $ \upstream -> Reduce new_schema upstream (fst <$> grps) (fst <$> aggTups) win_m

type instance PlannedType BoundHaving = RelationExpr -> RelationExpr
instance Plan BoundHaving where
  plan BoundHavingEmpty = return id
  plan (BoundHaving expr) = do
    (scalar,_) <- plan expr
    return $ \upstream -> Filter (relationExprSchema upstream) upstream scalar

type instance PlannedType BoundSel = RelationExpr -> RelationExpr
instance Plan BoundSel where
  plan sel@(BoundSel (BoundSelectItemProject tups)) = do
    ctx_old <- get
    (projTups :: [(ScalarExpr, ColumnCatalog)]) <-
      mapM (\(boundExpr,alias_m) -> do
             (scalar, catalog) <- plan boundExpr
             let catalog' = catalog
                           { columnName = fromMaybe (columnName catalog) alias_m
                           }
             return (scalar,catalog')
           ) tups
    let new_schema_cols = IntMap.fromList $ L.map (\(i, (_,catalog)) -> (i, catalog)
                                                  ) ([0..] `zip` projTups)
    let new_schema = Schema { schemaOwner = "" -- FIXME
                            , schemaColumns = new_schema_cols
                            }
    let ctx_new = PlanContext (IntMap.singleton 0 new_schema)
    put ctx_new
    return $ \upstream -> Project new_schema upstream (fst <$> projTups)

type instance PlannedType BoundSelect = RelationExpr
instance Plan BoundSelect where
  plan select@(BoundSelect _ sel frm whr grp hav) = do
    startStream <- plan frm
    whrFun <- plan whr
    let agg = BoundAgg grp sel
    aggFun <- plan agg
    havFun <- plan hav
    projFun <- plan sel
    return (projFun . havFun . aggFun . whrFun $ startStream)

type instance PlannedType BoundSQL = RelationExpr
instance Plan BoundSQL where
  plan (BoundQSelect select) = plan select
  plan _                     = undefined


planIO :: (Plan a, PlannedType a ~ RelationExpr) => a -> IO RelationExpr
planIO bound = evalStateT (plan bound) defaultPlanContext

----------------------------------------
--             module
----------------------------------------
planning :: Text -> IO ()
planning sql = do
  abs <- parse sql
  bound <- evalStateT (bind abs) defaultBindContext
  print $ "====== bind ======="
  print bound
  planned <- evalStateT (plan bound) defaultPlanContext
  print $ "====== plan ======="
  print planned
#endif
