{-# LANGUAGE CPP               #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies      #-}

module HStream.SQL.Planner where

import           Control.Applicative   ((<|>))
import           Data.Int              (Int64)
import           Data.Kind             (Type)
import qualified Data.List             as L
import qualified Data.Map              as Map
import           Data.Text             (Text)
import qualified Data.Text             as T

import           HStream.SQL.AST
import           HStream.SQL.Exception

data RelationExpr
  = StreamScan   Text
  | StreamRename RelationExpr Text

#ifdef HStreamUseV2Engine
  | CrossJoin RelationExpr RelationExpr
  | LoopJoinOn RelationExpr RelationExpr ScalarExpr RJoinType
  | LoopJoinUsing RelationExpr RelationExpr [Text] RJoinType
  | LoopJoinNatural RelationExpr RelationExpr RJoinType
#else
  | CrossJoin RelationExpr RelationExpr Int64
  | LoopJoinOn RelationExpr RelationExpr ScalarExpr RJoinType Int64
  | LoopJoinUsing RelationExpr RelationExpr [Text] RJoinType Int64
  | LoopJoinNatural RelationExpr RelationExpr RJoinType Int64
#endif

  | Filter RelationExpr ScalarExpr
  | Project RelationExpr [(ColumnCatalog, ColumnCatalog)] [Text] -- project [(column AS alias)] or [stream.*]
  | Affiliate RelationExpr [(ColumnCatalog,ScalarExpr)]

#ifdef HStreamUseV2Engine
  | Reduce RelationExpr [(ColumnCatalog,ScalarExpr)] [(ColumnCatalog,AggregateExpr)]
#else
  | Reduce RelationExpr [(ColumnCatalog,ScalarExpr)] [(ColumnCatalog,AggregateExpr)] (Maybe WindowType)
#endif

  | Distinct RelationExpr

#ifdef HStreamUseV2Engine
  | TimeWindow RelationExpr WindowType
#endif

  | Union RelationExpr RelationExpr
  deriving (Eq)

scanRelationExpr :: (RelationExpr -> Bool)
                 -> RelationExpr
                 -> Maybe RelationExpr
scanRelationExpr p r = if p r then Just r else case r of
  StreamScan _              -> Nothing
  StreamRename r' _         -> scanRelationExpr p r'
#ifdef HStreamUseV2Engine
  CrossJoin r1 r2           -> scanRelationExpr p r1 <|> scanRelationExpr p r2
  LoopJoinOn r1 r2 _ _      -> scanRelationExpr p r1 <|> scanRelationExpr p r2
  LoopJoinUsing r1 r2 _ _   -> scanRelationExpr p r1 <|> scanRelationExpr p r2
  LoopJoinNatural r1 r2 _   -> scanRelationExpr p r1 <|> scanRelationExpr p r2
#else
  CrossJoin r1 r2 _         -> scanRelationExpr p r1 <|> scanRelationExpr p r2
  LoopJoinOn r1 r2 _ _ _    -> scanRelationExpr p r1 <|> scanRelationExpr p r2
  LoopJoinUsing r1 r2 _ _ _ -> scanRelationExpr p r1 <|> scanRelationExpr p r2
  LoopJoinNatural r1 r2 _ _ -> scanRelationExpr p r1 <|> scanRelationExpr p r2
#endif
  Filter r' _               -> scanRelationExpr p r'
  Project r' _ _            -> scanRelationExpr p r'
  Affiliate r' _            -> scanRelationExpr p r'
#ifdef HStreamUseV2Engine
  Reduce r' _ _             -> scanRelationExpr p r'
#else
  Reduce r' _ _ _           -> scanRelationExpr p r'
#endif
  Distinct r'               -> scanRelationExpr p r'
#ifdef HStreamUseV2Engine
  TimeWindow r' _           -> scanRelationExpr p r'
#endif
  Union r1 r2               -> scanRelationExpr p r1 <|> scanRelationExpr p r2

data ScalarExpr
  = ColumnRef  Text (Maybe Text) -- fieldName, streamName_m
  | Literal    Constant
  | CallUnary  UnaryOp  ScalarExpr
  | CallBinary BinaryOp ScalarExpr ScalarExpr
  | CallCast   ScalarExpr RDataType
  | CallJson   JsonOp ScalarExpr ScalarExpr
  | ValueArray [ScalarExpr]
  | AccessArray ScalarExpr RArrayAccessRhs
  deriving (Eq, Ord)

type AggregateExpr = Aggregate ScalarExpr

-------------------
type family DecoupledType a :: Type
class Decouple a where
  decouple :: a -> DecoupledType a

type instance DecoupledType RValueExpr = ScalarExpr
instance Decouple RValueExpr where
  decouple expr = case expr of
    RExprCol _ stream_m field  -> ColumnRef field stream_m
    RExprConst _ constant      -> Literal constant
    RExprBinOp _ op e1 e2      -> CallBinary op (decouple e1) (decouple e2)
    RExprUnaryOp _ op e        -> CallUnary op (decouple e)
    RExprCast _ e typ          -> CallCast (decouple e) typ
    RExprAccessJson _ op e1 e2 -> CallJson op (decouple e1) (decouple e2)
    RExprAggregate name _      -> ColumnRef (T.pack name) Nothing
    RExprArray _ es            -> ValueArray (L.map decouple es)
    RExprAccessArray _ e rhs   -> AccessArray (decouple e) rhs
    -- RExprSubquery _ _          -> throwSQLException RefineException Nothing "subquery is not supported"

rSelToAffiliateItems :: RSel -> [(ColumnCatalog,ScalarExpr)]
rSelToAffiliateItems (RSel items) =
  L.concatMap (\item ->
                 case item of
                   RSelectItemProject expr _ ->
                     case expr of
                       RExprCol _ _ _     -> []
                       RExprAggregate _ _ -> []
                       _ -> let cata = ColumnCatalog
                                     { columnName = T.pack (getName expr)
                                     , columnStream = Nothing
                                     }
                                scalar = decouple expr
                             in [(cata,scalar)]
                   RSelectProjectQualifiedAll _ -> []
                   RSelectProjectAll            -> []
              ) items

-- FIXME: should not use alias. Project [ColumnCatalog] should be Project [(ColumnCatalog, ColumnCatalog)]
rSelToProjectItems :: RSel -> [(ColumnCatalog, ColumnCatalog)]
rSelToProjectItems (RSel items) =
  L.concatMap (\item ->
                 case item of
                   RSelectItemProject expr alias_m ->
                     case expr of
                       RExprCol name stream_m field ->
                         let cata_get = ColumnCatalog
                                        { columnName = field
                                        , columnStream = stream_m
                                        }
                             cata_alias = case alias_m of
                                            Nothing    -> cata_get
                                            Just alias -> ColumnCatalog
                                                        { columnName = alias
                                                        , columnStream = Nothing
                                                        }
                          in [(cata_get,cata_alias)]
                       _ -> let cata_get = ColumnCatalog
                                       { columnName = T.pack (getName expr)
                                       , columnStream = Nothing
                                       }
                                cata_alias = case alias_m of
                                               Nothing    -> cata_get
                                               Just alias -> ColumnCatalog
                                                           { columnName = alias
                                                           , columnStream = Nothing
                                                           }
                             in [(cata_get,cata_alias)]
                   RSelectProjectAll               -> []
                   RSelectProjectQualifiedAll _    -> []
              ) items

rSelToProjectStreams :: RSel -> [Text]
rSelToProjectStreams (RSel items) =
  L.concatMap (\item -> case item of
                          RSelectProjectQualifiedAll s -> [s]
                          RSelectItemProject _ _       -> []
                          RSelectProjectAll            -> []
              ) items

type instance DecoupledType (Aggregate RValueExpr) = Aggregate ScalarExpr
instance Decouple (Aggregate RValueExpr) where
  decouple agg = case agg of
    Nullary AggCountAll -> Nullary AggCountAll
    Unary agg expr      -> Unary agg (decouple expr)
    Binary agg e1 e2    -> Binary agg (decouple e1) (decouple e2)

#ifdef HStreamUseV2Engine
type instance DecoupledType RGroupBy = [(ColumnCatalog,ScalarExpr)]
instance Decouple RGroupBy where
  decouple RGroupByEmpty = []
  decouple (RGroupBy tups) =
    L.map (\(stream_m,field) ->
              let cata = ColumnCatalog
                       { columnName = field
                       , columnStream = stream_m
                       }
                  scalar = ColumnRef field stream_m
               in (cata, scalar)
          ) tups
#else
type instance DecoupledType RGroupBy = ([(ColumnCatalog,ScalarExpr)], Maybe WindowType)
instance Decouple RGroupBy where
  decouple RGroupByEmpty = ([], Nothing)
  decouple (RGroupBy tups win_m) =
    ( L.map (\(stream_m,field) ->
               let cata = ColumnCatalog
                          { columnName = field
                          , columnStream = stream_m
                          }
                   scalar = ColumnRef field stream_m
                in (cata, scalar)
            ) tups
    , win_m)
#endif

type instance DecoupledType RTableRef = RelationExpr
instance Decouple RTableRef where
  decouple (RTableRefSimple s alias_m) =
    let base = StreamScan s
     in case alias_m of
          Nothing    -> base
          Just alias -> StreamRename base alias
#ifdef HStreamUseV2Engine
  decouple (RTableRefCrossJoin ref1 ref2 alias_m) =
    let base_1 = decouple ref1
        base_2 = decouple ref2
        joined = CrossJoin base_1 base_2
     in case alias_m of
          Nothing    -> joined
          Just alias -> StreamRename joined alias
  decouple (RTableRefNaturalJoin ref1 typ ref2 alias_m) =
    let base_1 = decouple ref1
        base_2 = decouple ref2
        joined = LoopJoinNatural base_1 base_2 typ
     in case alias_m of
          Nothing    -> joined
          Just alias -> StreamRename joined alias
  decouple (RTableRefJoinOn ref1 typ ref2 expr alias_m) =
    let base_1 = decouple ref1
        base_2 = decouple ref2
        scalar = decouple expr
        joined = LoopJoinOn base_1 base_2 scalar typ
     in case alias_m of
          Nothing    -> joined
          Just alias -> StreamRename joined alias
  decouple (RTableRefJoinUsing ref1 typ ref2 cols alias_m) =
    let base_1 = decouple ref1
        base_2 = decouple ref2
        joined = LoopJoinUsing base_1 base_2 cols typ
     in case alias_m of
          Nothing    -> joined
          Just alias -> StreamRename joined alias
  decouple (RTableRefWindowed ref win alias_m) =
    let base = decouple ref
        windowed = TimeWindow base win
     in case alias_m of
          Nothing    -> windowed
          Just alias -> StreamRename windowed alias
  decouple (RTableRefSubquery select alias_m) =
    let base = decouple select
     in case alias_m of
          Nothing    -> base
          Just alias -> StreamRename base alias
#else
  decouple (RTableRefCrossJoin ref1 ref2 t) =
    let base_1 = decouple ref1
        base_2 = decouple ref2
    in  CrossJoin base_1 base_2 (calendarDiffTimeToMs t)
  decouple (RTableRefNaturalJoin ref1 typ ref2 t) =
    let base_1 = decouple ref1
        base_2 = decouple ref2
     in LoopJoinNatural base_1 base_2 typ (calendarDiffTimeToMs t)
  decouple (RTableRefJoinOn ref1 typ ref2 expr t) =
    let base_1 = decouple ref1
        base_2 = decouple ref2
        scalar = decouple expr
     in LoopJoinOn base_1 base_2 scalar typ (calendarDiffTimeToMs t)
  decouple (RTableRefJoinUsing ref1 typ ref2 cols t) =
    let base_1 = decouple ref1
        base_2 = decouple ref2
     in LoopJoinUsing base_1 base_2 cols typ (calendarDiffTimeToMs t)
#endif

type instance DecoupledType RFrom = RelationExpr
instance Decouple RFrom where
#ifdef HStreamUseV2Engine
  decouple (RFrom refs) =
    L.foldl1 (\x acc -> CrossJoin acc x) (L.map decouple refs)
#else
  decouple (RFrom ref) = decouple ref
#endif

type instance DecoupledType RSelect = RelationExpr
instance Decouple RSelect where
  decouple (RSelect sel frm whr grp hav) =
    let base = decouple frm
        -- WHERE
        filtered_1 = case whr of
                       RWhereEmpty -> base
                       RWhere expr -> Filter base (decouple expr)
        -- SELECT(affiliate items)
        affiliateItems = rSelToAffiliateItems sel
        affiliated = case affiliateItems of
                       [] -> filtered_1
                       _  -> Affiliate filtered_1 affiliateItems
        -- GROUP BY
        aggs = getAggregates sel ++ getAggregates hav
        projectItems   = rSelToProjectItems sel
        (aggs', projectItems') = L.foldl'
                  (\(ags, pis) (c, e) ->
                      case L.lookup c pis of
                        Nothing -> (ags ++ [(c, e)], pis)
                        Just a  -> (ags ++ [(a, e)],
                                      L.map
                                        (\(it, alias) ->
                                            if it == c
                                            then (alias, alias)
                                            else (it, alias))
                                        pis)
                  )
                  ([], projectItems)
                  aggs
        projectStreams = rSelToProjectStreams sel
        grped = case grp of
                  RGroupByEmpty -> affiliated
#ifdef HStreamUseV2Engine
                  RGroupBy _    -> Reduce affiliated (decouple grp) aggs'
#else
                  RGroupBy _ _  ->
                    let (tups, win_m) = decouple grp
                     in Reduce affiliated tups aggs' win_m
#endif

        -- HAVING
        filtered_2 = case hav of
                       RHavingEmpty -> grped
                       RHaving expr -> Filter grped (decouple expr)
        -- SELECT(project items)
        projected = if L.null projectItems' && L.null projectStreams then
                      filtered_2 else
                      Project filtered_2 projectItems' projectStreams
     in projected

--------------------------------------------------------------------------------
class HasAggregates a where
  getAggregates :: a -> [(ColumnCatalog,AggregateExpr)]

instance HasAggregates RValueExpr where
  getAggregates expr = case expr of
    RExprAggregate name agg   ->
      let cata = ColumnCatalog
               { columnName = T.pack name
               , columnStream = Nothing
               }
       in [(cata, decouple agg)]
    RExprCol _ _ _            -> []
    RExprConst _ _            -> []
    RExprCast _ e _           -> getAggregates e
    RExprArray _ es           -> L.concatMap getAggregates es
    RExprAccessArray _ e _    -> getAggregates e
    RExprAccessJson _ _ e1 e2 -> getAggregates e1 ++ getAggregates e2
    RExprBinOp _ _ e1 e2      -> getAggregates e1 ++ getAggregates e2
    RExprUnaryOp _ _ e        -> getAggregates e
    -- RExprSubquery _ _         -> [] -- do not support subquery in SELECT/HAVING now

instance HasAggregates RSelectItem where
  getAggregates item = case item of
    RSelectItemProject e _ -> getAggregates e
    _                      -> []

instance HasAggregates RSel where
  getAggregates (RSel items) = L.concatMap getAggregates items

instance HasAggregates RHaving where
  getAggregates RHavingEmpty = []
  getAggregates (RHaving e)  = getAggregates e
