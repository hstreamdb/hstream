{-# LANGUAGE CPP                  #-}
{-# LANGUAGE DeriveAnyClass       #-}
{-# LANGUAGE DeriveGeneric        #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE LambdaCase           #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE RecordWildCards      #-}
{-# LANGUAGE TypeFamilies         #-}
{-# LANGUAGE TypeSynonymInstances #-}

module HStream.SQL.Binder.Select where

import           Control.Applicative          ((<|>))
import           Control.Monad.Reader
import           Control.Monad.State
import qualified Data.Aeson                   as Aeson
import qualified Data.Bimap                   as Bimap
import qualified Data.HashMap.Strict          as HM
import           Data.IntMap                  (IntMap)
import qualified Data.IntMap                  as IntMap
import qualified Data.List                    as L
import           Data.Maybe                   (fromMaybe)
import qualified Data.Set                     as Set
import           Data.Text                    (Text)
import qualified Data.Text                    as T
import           GHC.Generics                 (Generic)

import           HStream.SQL.Abs
import           HStream.SQL.Binder.Basic
import           HStream.SQL.Binder.Common
import           HStream.SQL.Binder.ValueExpr
import           HStream.SQL.Exception

data WindowType
  = Tumbling BoundInterval
  | Hopping  BoundInterval BoundInterval
#ifdef HStreamUseV2Engine
  | Sliding BoundInterval
#else
  | Session  BoundInterval
#endif
  deriving (Eq, Show, Generic, Aeson.ToJSON, Aeson.FromJSON)

#ifdef HStreamUseV2Engine
data RTableRef = RTableRefSimple StreamName (Maybe StreamName)
               | RTableRefSubquery RSelect  (Maybe StreamName)
               | RTableRefCrossJoin RTableRef RTableRef (Maybe StreamName)
               | RTableRefNaturalJoin RTableRef RJoinType RTableRef (Maybe StreamName)
               | RTableRefJoinOn RTableRef RJoinType RTableRef RValueExpr (Maybe StreamName)
               | RTableRefJoinUsing RTableRef RJoinType RTableRef [Text] (Maybe StreamName)
               | RTableRefWindowed RTableRef WindowType (Maybe StreamName)
#else
data BoundTableRef = BoundTableRefSimple   Text
                   | BoundTableRefSubquery Text BoundSelect
                   | BoundTableRefWindowed Text BoundTableRef WindowType
                   -- Note: 'BoundTableRefWindowed' is only used when binding the whole 'SELECT' statement
                   -- to identify the 'GROUP BY' window. It is the same as 'BoundTableRefSimple' after binding.
                   | BoundTableRefJoin     Text BoundTableRef BoundJoinType BoundTableRef BoundExpr BoundInterval
#endif
               deriving (Eq, Generic, Aeson.ToJSON, Aeson.FromJSON)

instance HasName BoundTableRef where
  getName = \case
    BoundTableRefSimple   name           -> T.unpack name
    BoundTableRefSubquery name _         -> T.unpack name
    BoundTableRefWindowed name _ _       -> T.unpack name
    BoundTableRefJoin     name _ _ _ _ _ -> T.unpack name

-- | Scan for the first time window in a tableref. It is then
-- used in `GROUP BY` clause.
-- Note: Not very neat because the processing engine does not
-- support `win(s)` actually. So we just use the first window
-- then ignore the rest even they are not the same.
scanBoundTableRefWin :: BoundTableRef -> Maybe WindowType
scanBoundTableRefWin ref = case ref of
  BoundTableRefSimple _               -> Nothing
  BoundTableRefSubquery _ _           -> Nothing
  BoundTableRefWindowed _ _ win       -> Just win
  BoundTableRefJoin _ ref1 _ ref2 _ _ -> scanBoundTableRefWin ref1 <|>
                                         scanBoundTableRefWin ref2

instance Show BoundTableRef where
  show = \case
    BoundTableRefSimple   s -> "[SIMPLE] " <> show s
    BoundTableRefSubquery s sel -> "[SUBQUERY] " <> show sel
    BoundTableRefWindowed s ref windowType -> "[WIN] " <> show ref <> ", win=" <> show windowType
    BoundTableRefJoin     s ref joinType ref' expr interval -> "[JOIN] " <> ": \nt1=" <> show ref <> ", \nt2=" <> show ref' <> ", \non=" <> show expr <> ", \ntyp=" <> show joinType <> ", \nwin=" <> show interval

data BoundJoinType = InnerJoin | LeftJoin | RightJoin | FullJoin
                   deriving (Eq, Show, Generic, Aeson.ToJSON, Aeson.FromJSON)
type instance BoundType JoinTypeWithCond = BoundJoinType
instance Bind JoinTypeWithCond where
  bind' joinType = case joinType of
    JoinInner1{} -> return (InnerJoin, 0)
    JoinInner2{} -> return (InnerJoin, 0)
    JoinLeft1{}  -> return (LeftJoin , 0)
    JoinLeft2{}  -> return (LeftJoin , 0)
    JoinRight1{} -> return (RightJoin, 0)
    JoinRight2{} -> return (RightJoin, 0)
    JoinFull1{}  -> return (FullJoin , 0)
    JoinFull2{}  -> return (FullJoin , 0)

#ifdef HStreamUseV2Engine
setRTableRefAlias :: RTableRef -> StreamName -> RTableRef
setRTableRefAlias ref alias = case ref of
  RTableRefSimple s _ -> RTableRefSimple s (Just alias)
  RTableRefSubquery sel _ -> RTableRefSubquery sel (Just alias)
  RTableRefCrossJoin r1 r2 _ -> RTableRefCrossJoin r1 r2 (Just alias)
  RTableRefNaturalJoin r1 typ r2 _ -> RTableRefNaturalJoin r1 typ r2 (Just alias)
  RTableRefJoinOn r1 typ r2 e _ -> RTableRefJoinOn r1 typ r2 e (Just alias)
  RTableRefJoinUsing r1 typ r2 cols _ -> RTableRefJoinUsing r1 typ r2 cols (Just alias)
  RTableRefWindowed r win _ -> RTableRefWindowed r win (Just alias)
#endif

type instance BoundType TableRef = BoundTableRef
instance Bind TableRef where
#ifdef HStreamUseV2Engine
  refine (TableRefIdent _ hIdent) = RTableRefSimple (refine hIdent) Nothing
  refine (TableRefSubquery _ select) = RTableRefSubquery (refine select) Nothing
  refine (TableRefAs _ ref alias) =
    let rRef = refine ref
     in setRTableRefAlias rRef (refine alias)
  refine (TableRefCrossJoin _ r1 _ r2) = RTableRefCrossJoin (refine r1) (refine r2) Nothing
  refine (TableRefNaturalJoin _ r1 typ r2) = RTableRefNaturalJoin (refine r1) (refine typ) (refine r2) Nothing
  refine (TableRefJoinOn _ r1 typ r2 e) = RTableRefJoinOn (refine r1) (refine typ) (refine r2) (refine e) Nothing
  refine (TableRefJoinUsing _ r1 typ r2 cols) = RTableRefJoinUsing (refine r1) (refine typ) (refine r2) (extractStreamNameFromColName <$> cols) Nothing
    where extractStreamNameFromColName col = case col of
            ColNameSimple _ colIdent -> refine colIdent
            ColNameStream pos _ _    -> throwImpossible
  refine (TableRefTumbling _ ref interval) = RTableRefWindowed (refine ref) (Tumbling (refine interval)) Nothing
  refine (TableRefHopping _ ref len hop) = RTableRefWindowed (refine ref) (Hopping (refine len) (refine hop)) Nothing
  refine (TableRefSliding _ ref interval) = RTableRefWindowed (refine ref) (Sliding (refine interval)) Nothing
#else
  bind' (TableRefIdent pos hIdent) = do
    streamName <- bind hIdent
    getSchema <- ask
    liftIO (getSchema streamName) >>= \case
      Nothing     -> throwSQLException BindException pos $ "stream " <> T.unpack streamName <> " not exist"
      Just schema -> do
        -- add alias to context (same as the original name)
        modify' (\ctx@BindContext{..} -> ctx { bindContextAliases = Bimap.insert streamName streamName bindContextAliases })
        -- ctx stack
        let layer = schemaToContextLayer schema Nothing BindUnitBase
        pushLayer layer
        return (BoundTableRefSimple streamName, 1)

  bind' (TableRefSubquery _ select) = do
    (boundSelect, n) <- bind' select -- SELECT's ctx is the same as its FROM's
    subQueryName <- genSubqueryName
    -- add alias to context (same as the original name)
    modify' (\ctx@BindContext{..} -> ctx { bindContextAliases = Bimap.insert subQueryName subQueryName bindContextAliases })
    -- ctx stack
    let (BoundSelect cols _ _ _ _ _) = boundSelect
    _subQueryLayers <- popNLayer n
    pushLayer (genLayerContext cols subQueryName)
    return (BoundTableRefSubquery subQueryName boundSelect, 1)
    where
      genLayerContext :: [Text] -> Text -> BindContextLayer
      genLayerContext cols subQueryName =
        BindContextLayer { layerColumnBindings = HM.fromList $
          L.map (\(i,col) -> (col, HM.singleton subQueryName (i,BindUnitTemp)))
                ([0..] `L.zip` cols)
        }

  -- FIXME: add window start/end to schema
  bind' (TableRefTumbling _ ref interval) = do
    (boundRef, n) <- bind' ref
    boundInterval <- bind interval
    -- window start & end
    -- FIXME: This assumes the prev ref has only one context layer,
    --        because we do not allow `WIN(JOIN)` now.
    --        But this should be fixed in the future.
    -- FIXME: Do not use a new context layer.
    topLayer@(BindContextLayer topHM) <- popLayer
    pushLayer topLayer
    let layerMaxColIndex = L.maximum . L.concat $ map (fmap fst . HM.elems) (HM.elems topHM)
    let windowLayer = BindContextLayer
                    { layerColumnBindings = HM.fromList [ ("winStartText", HM.fromList [("", (layerMaxColIndex+1,BindUnitBase))])
                                                        , ("winEndText"  , HM.fromList [("", (layerMaxColIndex+2,BindUnitBase))])
                                                        ]
                    }
    pushLayer windowLayer
    return (BoundTableRefWindowed (T.pack $ getName boundRef) boundRef (Tumbling boundInterval), n+1)

  bind' (TableRefHopping _ ref len hop) = do
    (boundRef, n) <- bind' ref
    boundLen <- bind len
    boundHop <- bind hop
    -- window start & end
    -- FIXME: This assumes the prev ref has only one context layer,
    --        because we do not allow `WIN(JOIN)` now.
    --        But this should be fixed in the future.
    -- FIXME: Do not use a new context layer.
    topLayer@(BindContextLayer topHM) <- popLayer
    pushLayer topLayer
    let layerMaxColIndex = L.maximum . L.concat $ map (fmap fst . HM.elems) (HM.elems topHM)
    let windowLayer = BindContextLayer
                    { layerColumnBindings = HM.fromList [ ("winStartText", HM.fromList [("", (layerMaxColIndex+1,BindUnitBase))])
                                                        , ("winEndText"  , HM.fromList [("", (layerMaxColIndex+2,BindUnitBase))])
                                                        ]
                    }
    pushLayer windowLayer
    return (BoundTableRefWindowed (T.pack $ getName boundRef) boundRef (Hopping boundLen boundHop), n+1)

  bind' (TableRefSession _ ref interval) = do
    (boundRef, n) <- bind' ref
    boundInterval <- bind interval
    return (BoundTableRefWindowed (T.pack $ getName boundRef) boundRef (Session boundInterval), n)

  bind' (TableRefAs _ ref hIdent) = do
    (boundRef, n) <- bind' ref
    let originalName = T.pack (getName boundRef)
    alias <- bind hIdent
    -- add alias to ctx
    modify' (\ctx@BindContext{..} -> ctx { bindContextAliases = Bimap.insert alias originalName bindContextAliases })
    ---- modified ctx stack
    --layers <- popNLayer n
    --let layers' = L.map (setLayerAlias alias) layers
    --mapM_ pushLayer layers'
    return (boundRef, n)

  bind' (TableRefCrossJoin _ ref1 _ ref2 interval) = do
    (boundRef1, n1) <- bind' ref1
    (boundRef2, n2) <- bind' ref2
    boundInterval <- bind interval
    joinName <- genJoinName
    -- add alias to ctx (same as the original name)
    modify' (\ctx@BindContext{..} -> ctx { bindContextAliases = Bimap.insert joinName joinName bindContextAliases })
    -- ctx stack
    layers2 <- popNLayer n2
    layers1 <- popNLayer n1
    let layer = mconcat (layers1 ++ layers2)
    pushLayer layer
    return (BoundTableRefJoin joinName boundRef1 InnerJoin boundRef2 (BoundExprConst "TRUE" (ConstantBoolean True)) boundInterval, 1)

  bind' (TableRefNaturalJoin pos ref1 typ ref2 interval) = do
    (boundRef1, n1) <- bind' ref1
    (boundRef2, n2) <- bind' ref2
    boundTyp <- bind typ
    boundInterval <- bind interval
    joinName <- genJoinName
    -- add alias to ctx (same as the original name)
    modify' (\ctx@BindContext{..} -> ctx { bindContextAliases = Bimap.insert joinName joinName bindContextAliases })
    -- temp ctxs for left and right table
    layers2 <- popNLayer n2
    ctx_1 <- get
    layers1 <- popNLayer n1
    forM_ (L.reverse layers2) pushLayer
    ctx_2 <- get
    _ <- popNLayer n2

    let sameColumns = scanColumns (mconcat layers1) (mconcat layers2)
    expr <- foldM (\acc colName -> do
      case lookupColumn ctx_1 colName of
          Nothing         -> throwSQLException BindException pos $ "column " <> T.unpack colName <> " is ambiguous in " <> show boundRef1
          Just (s1, i, _) -> case lookupColumn ctx_2 colName of
            Nothing         -> throwSQLException BindException pos $ "column " <> T.unpack colName <> " is ambiguous in " <> show boundRef2
            Just (s2, j, _) -> do
              let col1 = BoundExprCol (T.unpack $ s1 <> "." <> colName) s1 colName i
                  col2 = BoundExprCol (T.unpack $ s2 <> "." <> colName) s2 colName j
              return $ BoundExprBinOp (getName acc <> "AND" <> getName col1 <> "=" <> getName col2) OpAnd
                                      (BoundExprBinOp (getName col1 <> "=" <> getName col2) OpEQ col1 col2) acc
                  ) (BoundExprConst "TRUE" (ConstantBoolean True)) sameColumns
    -- ctx stack
    let layer = mconcat (layers1 ++ layers2)
    pushLayer layer
    return (BoundTableRefJoin joinName boundRef1 boundTyp boundRef2 expr boundInterval, 1)

  bind' (TableRefJoinOn _ ref1 typ ref2 expr interval) = do
    (boundRef1, n1) <- bind' ref1
    (boundRef2, n2) <- bind' ref2
    boundTyp <- bind typ
    boundInterval <- bind interval
    joinName <- genJoinName
    -- add alias to ctx (same as the original name)
    modify' (\ctx@BindContext{..} -> ctx { bindContextAliases = Bimap.insert joinName joinName bindContextAliases })
    -- ctx stack
    layers2 <- popNLayer n2
    layers1 <- popNLayer n1
    let layer = mconcat (layers1 ++ layers2)
    pushLayer layer

    boundExpr <- bind expr
    return (BoundTableRefJoin joinName boundRef1 boundTyp boundRef2 boundExpr boundInterval, 1)

  bind' (TableRefJoinUsing pos ref1 typ ref2 cols interval) = do
    (boundRef1, n1) <- bind' ref1
    (boundRef2, n2) <- bind' ref2
    boundTyp <- bind typ
    colNames <- mapM bind cols
    boundInterval <- bind interval
    joinName <- genJoinName
    -- add alias to ctx (same as the original name)
    modify' (\ctx@BindContext{..} -> ctx { bindContextAliases = Bimap.insert joinName joinName bindContextAliases })
    -- temp ctxs for left and right table
    layers2 <- popNLayer n2
    ctx_1 <- get
    layers1 <- popNLayer n1
    forM_ (L.reverse layers2) pushLayer
    ctx_2 <- get
    _ <- popNLayer n2

    expr <- foldM (\acc colName ->
        case lookupColumn ctx_1 colName of
          Nothing         -> throwSQLException BindException pos $ "column " <> T.unpack colName <> " is ambiguous in " <> show boundRef1
          Just (s1, i, _) -> case lookupColumn ctx_2 colName of
            Nothing         -> throwSQLException BindException pos $ "column " <> T.unpack colName <> " is ambiguous in " <> show boundRef2
            Just (s2, j, _) -> do
              let col1 = BoundExprCol (T.unpack $ s1 <> "." <> colName) s1 colName i
                  col2 = BoundExprCol (T.unpack $ s2 <> "." <> colName) s2 colName j
              return $ BoundExprBinOp (getName acc <> "AND" <> getName col1 <> "=" <> getName col2) OpAnd
                                      (BoundExprBinOp (getName col1 <> "=" <> getName col2) OpEQ col1 col2) acc
                  ) (BoundExprConst "TRUE" (ConstantBoolean True)) colNames
    -- ctx stack
    let layer = mconcat (layers1 ++ layers2)
    pushLayer layer
    return (BoundTableRefJoin joinName boundRef1 boundTyp boundRef2 expr boundInterval, 1)
#endif

-----------------------------------------------------------
-- Sel
type SelectItemAlias = Text
data BoundSelectItem =
  BoundSelectItemProject [(BoundExpr, Maybe SelectItemAlias)]
  deriving (Eq, Generic, Aeson.ToJSON, Aeson.FromJSON)

instance Show BoundSelectItem where
  show (BoundSelectItemProject xs) =
    L.intercalate ", " $ map (\(expr, alias) -> case alias of
      Nothing     -> show expr
      Just alias' -> show expr <> " AS " <> T.unpack alias') xs

instance Semigroup BoundSelectItem where
  BoundSelectItemProject xs <> BoundSelectItemProject ys = BoundSelectItemProject (xs ++ ys)
instance Monoid BoundSelectItem where
  mempty = BoundSelectItemProject []

type instance BoundType SelectItem = BoundSelectItem
instance Bind SelectItem where
  bind' (SelectItemUnnamedExpr _ expr) = do
    boundExpr <- bind expr
    return (BoundSelectItemProject [(boundExpr, Nothing)], 0)

  bind' (SelectItemExprWithAlias _ expr colIdent) = do
    boundExpr <- bind expr
    alias <- bind colIdent
    return (BoundSelectItemProject [(boundExpr, Just alias)], 0)

  bind' (SelectItemQualifiedWildcard _ hIdent) = do
    ctx <- get
    streamName <- bind hIdent
    let tups = listColumnsByStream ctx streamName
    return (BoundSelectItemProject [(BoundExprCol (T.unpack col) originalStream col i, Nothing) | (originalStream,col,i) <- tups], 0)

  bind' (SelectItemWildcard _) = do
    ctx <- get
    let tups = listColumns ctx
    return (BoundSelectItemProject [(BoundExprCol (T.unpack col) stream col i, Nothing) | (stream,col,i) <- tups], 0)

newtype BoundSel = BoundSel BoundSelectItem
  deriving (Eq, Generic, Aeson.ToJSON, Aeson.FromJSON)
instance Show BoundSel where
  show (BoundSel item) = show item

type instance BoundType Sel = BoundSel
instance Bind Sel where
  bind' (DSel _ items) = do
    boundItems <- mapM bind items
    return (BoundSel (mconcat boundItems), 0)

-- Frm
#ifdef HStreamUseV2Engine
newtype BoundFrom = BoundFrom [BoundTableRef]
  deriving (Show, Eq, Generic, Aeson.ToJSON, Aeson.FromJSON)
type instance BoundType From = BoundFrom
instance Bind From where
  bind' _ = undefined
#else
newtype BoundFrom = BoundFrom BoundTableRef
  deriving (Eq, Generic, Aeson.ToJSON, Aeson.FromJSON)
type instance BoundType From = BoundFrom
instance Bind From where
  bind' (DFrom _ ref) = do
    (boundRef, n) <- bind' ref
    return (BoundFrom boundRef, n)

instance Show BoundFrom where
  show (BoundFrom ref) = show ref
#endif

-- Whr
data BoundWhere = BoundWhereEmpty
                | BoundWhere BoundExpr
                deriving (Show, Eq, Generic, Aeson.ToJSON, Aeson.FromJSON)
type instance BoundType Where = BoundWhere
instance Bind Where where
  bind' (DWhereEmpty _) = return (BoundWhereEmpty, 0)
  bind' (DWhere _ expr) = do
    boundExpr <- bind expr
    return (BoundWhere boundExpr, 0)

-- Grp
#ifdef HStreamUseV2Engine
data BoundGroupBy = BoundGroupByEmpty
                  | BoundGroupBy [(Text, Int)] -- (StreamName, FieldIndex)
                  deriving (Eq, Show, Generic, Aeson.ToJSON, Aeson.FromJSON)
type instance BoundType GroupBy = BoundGroupBy
instance Bind GroupBy where
  bind' (DGroupByEmpty _) = return (BoundGroupByEmpty, 0)
  bind' (DGroupBy _ cols) = return (BoundGroupBy undefined, 0)
--    L.map (\col -> let (RExprCol _ m_stream field) = refine col
--                    in (m_stream, field)) cols
#else
data BoundGroupBy = BoundGroupByEmpty
                  | BoundGroupBy [BoundExpr] (Maybe WindowType)
                  deriving (Eq, Show, Generic, Aeson.ToJSON, Aeson.FromJSON)
type instance BoundType GroupBy = BoundGroupBy
instance Bind GroupBy where
  bind' (DGroupByEmpty _) = return (BoundGroupByEmpty, 0)
  bind' (DGroupBy _ cols) = do
    exprs <- foldM (\acc col ->
              case col of
                ColNameSimple pos colIdent -> do
                  ctx <- get
                  colName <- bind colIdent
                  case lookupColumn ctx colName of
                    Nothing      -> throwSQLException BindException pos $ "column not found: " <> show colName
                    Just (s,i,_) -> return $ (BoundExprCol (T.unpack colName) s colName i):acc
                ColNameStream pos hIdent colIdent -> do
                  ctx <- get
                  streamName <- bind hIdent
                  colName <- bind colIdent
                  case lookupColumnWithStream ctx colName streamName of
                    Nothing    -> throwSQLException BindException pos $ "column not found: " <> show col
                    Just (originalStream,i,_) ->
                      return $ (BoundExprCol (T.unpack originalStream <> "." <> T.unpack colName)
                                             originalStream
                                             colName
                                             i
                               ):acc
                  ) [] cols
    return (BoundGroupBy exprs Nothing, 0)
#endif

---- Hav
data BoundHaving = BoundHavingEmpty
                 | BoundHaving BoundExpr
                 deriving (Show, Eq, Generic, Aeson.ToJSON, Aeson.FromJSON)
type instance BoundType Having = BoundHaving
instance Bind Having where
  bind' (DHavingEmpty _) = return (BoundHavingEmpty, 0)
  bind' (DHaving _ expr) = do
    boundExpr <- bind expr
    return (BoundHaving boundExpr, 0)

---- SELECT
data BoundSelect =
  BoundSelect [Text] BoundSel BoundFrom BoundWhere BoundGroupBy BoundHaving
  -- columns, select, from, where, group by, having
  deriving (Eq, Generic, Aeson.ToJSON, Aeson.FromJSON)

instance Show BoundSelect where
  show (BoundSelect cols boundSel boundFrm boundWhr boundGrp boundHav) =
    "BoundSelect [" <> show cols <> "]" <> "\n"
    <> "{ boundSel = " <> show boundSel <> "\n"
    <> ", boundFrom = "         <> show boundFrm <> "\n"
    <> ", boundWhere = "        <> show boundWhr <> "\n"
    <> ", boundGroupBy = "      <> show boundGrp <> "\n"
    <> ", boundHaving = "       <> show boundHav <> "\n"
    <> "}"

type instance BoundType Select = BoundSelect
#ifdef HStreamUseV2Engine
instance Bind Select where
  bind (DSelect _ sel frm whr grp hav) =
    RSelect (refine sel) (refine frm) (refine whr) (refine grp) (refine hav)
#else
instance Bind Select where
  bind' (DSelect pos sel frm whr grp hav) = do
    (boundFrm, frm_n) <- bind' frm
    boundWhr <- bind whr
    boundGrp <- bind grp -- FIXME: `GroupBy` should use the ctx after binding `Sel` (sure? standard?)
    boundHav <- bind hav
    boundSel <- bind sel
    let (BoundFrom boundRef) = boundFrm
    -- FIXME: remove this dirty hack after the engine supports `win(s)`
    let groupbyWin_m = scanBoundTableRefWin boundRef
    let boundGrp' = case boundGrp of
          BoundGroupByEmpty   -> case groupbyWin_m of
            Nothing  -> BoundGroupByEmpty
            Just win -> throwSQLException BindException pos $ "empty GROUPBY encountered a window!"
          BoundGroupBy tups _ -> case groupbyWin_m of
            Nothing  -> BoundGroupBy tups Nothing
            Just win -> BoundGroupBy tups (Just win)
    -- cols
    -- IMPORTANT: always use column name WITHOUT stream in context
    --            because we only use column name to lookup a column
    --            in context, see `ValueExpr.hs`.
    --            Consider `SELECT a FROM (SELECT s1.a FROM s1)`.
    let (BoundSel (BoundSelectItemProject tups)) = boundSel
    let cols = L.map (\(expr,alias_m) -> case expr of
                         BoundExprCol _ _ colName _ ->
                           fromMaybe colName alias_m
                         _ ->
                           fromMaybe (T.pack $ getName expr) alias_m
                     ) tups
    return (BoundSelect cols boundSel boundFrm boundWhr boundGrp' boundHav, frm_n)
#endif
