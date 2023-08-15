{-# LANGUAGE CPP                 #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies        #-}

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
import           Control.Monad.Reader
import           Control.Monad.State
import           Data.Functor                  ((<&>))
import qualified Data.HashMap.Strict           as HM
import qualified Data.IntMap                   as IntMap
import qualified Data.List                     as L
import           Data.Maybe                    (fromMaybe)
import           Data.Text                     (Text)
import qualified Data.Text                     as T
import           Text.StringRandom             (stringRandomIO)

import           HStream.SQL.Binder            hiding (lookupColumn)
import           HStream.SQL.Exception
import           HStream.SQL.Exception         (throwSQLException)
import           HStream.SQL.Extra
import           HStream.SQL.ParseNew
import           HStream.SQL.PlannerNew.Expr
import           HStream.SQL.PlannerNew.Extra
import           HStream.SQL.PlannerNew.Pretty
import           HStream.SQL.PlannerNew.Types
import           HStream.SQL.Rts

----
type instance PlannedType BoundTableRef = RelationExpr
instance Plan BoundTableRef where
  plan (BoundTableRefSimple name) = do
    getSchema <- ask
    liftIO (getSchema name) >>= \case
      Nothing -> throwSQLException PlanException Nothing $
        "Schema of stream not found: " <> T.unpack name
      Just schema -> do
        -- 1-in/1-out
        let schema' = setSchemaStreamId 0 schema
        let ctx = PlanContext (IntMap.singleton 0 schema') mempty
        put ctx
        return $ StreamScan schema'

  plan (BoundTableRefSubquery name sel) = do
    relationExpr <- plan sel
    -- 1-in/1-out
    let schema = setSchemaStream name $
          setSchemaStreamId 0 (relationExprSchema relationExpr)
    let ctx = PlanContext (IntMap.singleton 0 schema) mempty
    put ctx
    return $ setRelationExprSchema schema relationExpr

  -- Note: As `HStream.SQL.Binder.Select` says, 'BoundTableRefWindowed' is
  -- only used when binding 'SELECT'. So we can ignore the window part here
  -- because it has been already absorbed into the 'BoundGroupBy' part.
  plan (BoundTableRefWindowed _name ref win) = plan ref

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

    put $ PlanContext
      { planContextSchemas   = planContextSchemas ctx1 <::>
                               planContextSchemas ctx2
      , planContextBasicRefs = mempty
      }
    (scalarExpr,_) <- plan expr

    -- 1-out!
    let schema = setSchemaStreamId 0 $ Schema
                 { schemaOwner = name
                 , schemaColumns = schemaColumns schema1 <:+:>
                                   schemaColumns schema2
                 }
    put $ PlanContext
      { planContextSchemas = IntMap.map (setSchemaStreamId 0) (IntMap.singleton 0 schema)
      , planContextBasicRefs = HM.fromList
          (L.map (\(colAbs, colRel) ->
                    ( (columnStream colAbs, columnId colAbs)
                    , (columnStream colRel, columnId colRel)
                    )
                ) ((IntMap.elems (schemaColumns schema1) ++
                    IntMap.elems (schemaColumns schema2))
                    `zip` (IntMap.elems (schemaColumns schema)))
          ) `HM.union` (planContextBasicRefs ctx1 `HM.union` planContextBasicRefs ctx2)
      -- FIXME: clean up!!
      }
    let win = calendarDiffTimeToMs interval
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
                     mapM (\aggBoundExpr -> do
                              let (BoundExprAggregate name aggBound) = aggBoundExpr
                              (agg,catalog) <- plan aggBound
                              let catalog' = catalog { columnName = T.pack name } -- WARNING: do not use alias here
                              return (agg,catalog')
                          ) (exprAggregates boundExpr)
                 ) selTups
           ) <&> L.concat

      let grpsIntmap = IntMap.fromList $ L.map (\(i, (_,catalog)) -> (i, catalog{columnId = i, columnStream = ""}) -- FIXME: stream name & clean up
                                               ) ([0..] `zip` grps)
          aggsIntmap = IntMap.fromList $ L.map (\(i, (_,catalog)) -> (i, catalog{columnId = i})
                                               ) ([0..] `zip` aggTups)

      let dummyTimewindowCols = IntMap.fromList [ (0, ColumnCatalog
                                                    { columnId = 0
                                                    , columnName = winStartText
                                                    , columnStreamId = 0
                                                    , columnStream = "" -- FIXME
                                                    , columnType = BTypeTimestamp
                                                    , columnIsNullable = True
                                                    , columnIsHidden = True
                                                    }
                                                  )
                                                , (1, ColumnCatalog
                                                    { columnId = 1
                                                    , columnName = winEndText
                                                    , columnStreamId = 0
                                                    , columnStream = "" -- FIXME
                                                    , columnType = BTypeTimestamp
                                                    , columnIsNullable = True
                                                    , columnIsHidden = True
                                                    }
                                                  )
                                                ]
      let new_schema = Schema { schemaOwner = "" -- FIXME
                              , schemaColumns = grpsIntmap <:+:> aggsIntmap <:+:> dummyTimewindowCols
                              }
      let ctx_new = PlanContext
                  { planContextSchemas   = IntMap.singleton 0 new_schema
                  , planContextBasicRefs = HM.fromList
                      (L.map (\(colAbs, colRel) ->
                               ( (columnStream colAbs, columnId colAbs)
                               , (columnStream colRel, columnId colRel)
                               )
                            ) (((snd <$> grps)
                               `zip` (IntMap.elems grpsIntmap)))
                      ) `HM.union` planContextBasicRefs ctx_old -- WARNING: order matters!
                  }
      -- FIXME: clean up!!
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
    let new_schema_cols = IntMap.fromList $ L.map (\(i, (_,catalog)) -> (i, catalog{columnId = i})
                                                  ) ([0..] `zip` projTups)
    let new_schema = Schema { schemaOwner = "" -- FIXME
                            , schemaColumns = new_schema_cols
                            }
    let ctx_new = PlanContext
                { planContextSchemas   = IntMap.singleton 0 new_schema
                , planContextBasicRefs = planContextBasicRefs ctx_old
                }
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
  plan (BoundQPushSelect select) = plan select
  plan (BoundQCreate create) = case create of
    BoundCreateAs streamName select _opts -> do
      relationExpr <- plan select
      let schema = relationExprSchema relationExpr
      let schema' = setSchemaStream streamName schema
      return $ setRelationExprSchema schema' relationExpr
    BoundCreateView viewName select -> do
      relationExpr <- plan select
      let schema = relationExprSchema relationExpr
      let schema' = setSchemaStream viewName schema
      return $ setRelationExprSchema schema' relationExpr
    _ -> throwSQLException PlanException Nothing $ "unsupported create statement: " <> show create
  plan (BoundQSelect select) = do
    relationExpr <- plan select
    let schema = relationExprSchema relationExpr
    streamName <- liftIO $ stringRandomIO "[a-zA-Z]{20}"
    let schema' = setSchemaStream streamName schema
    return $ setRelationExprSchema schema' relationExpr
  plan (BoundQPushSelect select) = do
    relationExpr <- plan select
    let schema = relationExprSchema relationExpr
    streamName <- liftIO $ stringRandomIO "[a-zA-Z]{20}"
    let schema' = setSchemaStream streamName schema
    return $ setRelationExprSchema schema' relationExpr
  plan sql                   = throwSQLException PlanException Nothing $ "unsupported sql statement: " <> show sql

planIO :: (Plan a, PlannedType a ~ RelationExpr)
       => a -> (Text -> IO (Maybe Schema)) -> IO RelationExpr
planIO bound getSchema =
  evalStateT (runReaderT (plan bound) getSchema) defaultPlanContext

----------------------------------------
--             module
----------------------------------------
planning :: Text -> (Text -> IO (Maybe Schema)) -> IO ()
planning sql getSchema = do
  abs <- parse sql
  bound <- evalStateT (runReaderT (bind abs) getSchema) defaultBindContext
  print $ "====== bind ======="
  print bound
  planned <- evalStateT (runReaderT (plan bound) getSchema) defaultPlanContext
  print $ "====== plan ======="
  print planned
#endif
