{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StrictData          #-}
{-# LANGUAGE ViewPatterns        #-}

module HStream.SQL.Codegen where

import           Data.Aeson                   (Object, Value (..))
import qualified Data.Aeson                   as Aeson
import           Data.Bifunctor
import           Data.Function
import           Data.Functor
import qualified Data.HashMap.Strict          as HM
import qualified Data.List                    as L
import qualified Data.Map.Strict              as Map
import           Data.Maybe
import           Data.Scientific              (fromFloatDigits, scientific)
import qualified Data.Text                    as T
import           Data.Text.Encoding           (decodeUtf8)
import           Data.Time                    (diffTimeToPicoseconds,
                                               showGregorian)
import qualified Proto3.Suite                 as PB
import           RIO
import qualified RIO.ByteString.Lazy          as BL
import qualified Z.Data.CBytes                as CB

import           HStream.SQL.AST
import           HStream.SQL.Exception        (SomeSQLException (..),
                                               throwSQLException)
import           HStream.SQL.Internal.Codegen
import           HStream.SQL.Parse            (parseAndRefine)
import           HStream.Utils                (cBytesToText, genUnique,
                                               jsonObjectToStruct)
import qualified HStream.Utils.Aeson          as HsAeson

import           DiffFlow.Graph
import           DiffFlow.Types

--------------------------------------------------------------------------------
type Row = FlowObject

type SerMat  = Object
type SerPipe = BL.ByteString

type ViewName = T.Text
type ConnectorName  = T.Text
type CheckIfExist  = Bool
type ViewSchema = [String]

data ShowObject = SStreams | SQueries | SConnectors | SViews
data DropObject = DStream Text | DView Text | DConnector Text
data TerminationSelection = AllQueries | OneQuery Text | ManyQueries [Text]
data InsertType = JsonFormat | RawFormat
data PauseObject = PauseObjectConnector Text
data ResumeObject = ResumeObjectConnector Text
data HStreamPlan
  ={-
    SelectPlan          Text [(Node,StreamName)] (Node,StreamName) (Maybe RWindow) (GraphBuilder Row)
  | CreateBySelectPlan  Text [(Node,StreamName)] (Node,StreamName) (Maybe RWindow) (GraphBuilder Row) Int
  | CreateViewPlan      Text ViewSchema [(Node,StreamName)] (Node,StreamName) (Maybe RWindow) (GraphBuilder Row) (MVar (DataChangeBatch Int64))
-}
    CreatePlan          StreamName Int
  | CreateConnectorPlan ConnectorType ConnectorName Text Bool (HM.HashMap Text Value)
  | InsertPlan          StreamName InsertType ByteString
  | DropPlan            CheckIfExist DropObject
  | ShowPlan            ShowObject
  | TerminatePlan       TerminationSelection
  | ExplainPlan         Text
  | PausePlan           PauseObject
  | ResumePlan          ResumeObject
  | SelectPlan [In] Out (GraphBuilder Row)
  | CreateBySelectPlan  [In] Out (GraphBuilder Row) Int -- FIXME
  | CreateViewPlan      [In] Out (GraphBuilder Row) (MVar (DataChangeBatch Row Int64)) -- FIXME

--------------------------------------------------------------------------------

streamCodegen :: HasCallStack => Text -> IO HStreamPlan
streamCodegen input = parseAndRefine input >>= hstreamCodegen

hstreamCodegen :: HasCallStack => RSQL -> IO HStreamPlan
hstreamCodegen = \case
  RQSelect select -> do
    let subgraph = Subgraph 0
    let (startBuilder, _) = addSubgraph emptyGraphBuilder subgraph
    (endBuilder, ins, out) <- elabRSelect select startBuilder subgraph
    return $ SelectPlan ins out endBuilder
{-
  RQCreate (RCreateAs stream select rOptions) -> do
    tName <- genTaskName
    (builder, inNodesWithStreams, outNodeWithStream, window) <- genGraphBuilderWithOutput (Just stream) select
    return $ CreateBySelectPlan tName inNodesWithStreams outNodeWithStream window builder (rRepFactor rOptions)
  RQCreate (RCreateView view select@(RSelect sel _ _ _ _)) -> do
    tName <- genTaskName
    (builder, inNodesWithStreams, outNodeWithStream, window) <- genGraphBuilderWithOutput (Just view) select
    accumulation <- newMVar emptyDataChangeBatch
    let schema = case sel of
          RSelAsterisk    -> ["*"] -- FIXME: schema on 'SELECT *'
          RSelList fields -> map snd fields
    return $ CreateViewPlan tName schema inNodesWithStreams outNodeWithStream window builder accumulation
-}
  RQCreate (RCreate stream rOptions) -> return $ CreatePlan stream (rRepFactor rOptions)
  RQCreate (RCreateConnector cType cName cTarget ifNotExist (RConnectorOptions cOptions)) ->
    return $ CreateConnectorPlan cType cName cTarget ifNotExist cOptions
  RQInsert (RInsert stream tuples)   -> do
    let jsonObj = HsAeson.fromList $
          second (flowValueToJsonValue . constantToFlowValue) <$> tuples
    return $ InsertPlan stream JsonFormat (BL.toStrict . PB.toLazyByteString . jsonObjectToStruct $ jsonObj)
  RQInsert (RInsertBinary stream bs) -> return $ InsertPlan stream RawFormat  bs
  RQInsert (RInsertJSON stream bs)   -> return $ InsertPlan stream JsonFormat (BL.toStrict . PB.toLazyByteString . jsonObjectToStruct . fromJust $ Aeson.decode (BL.fromStrict bs))
  RQShow (RShow RShowStreams)        -> return $ ShowPlan SStreams
  RQShow (RShow RShowQueries)        -> return $ ShowPlan SQueries
  RQShow (RShow RShowConnectors)     -> return $ ShowPlan SConnectors
  RQShow (RShow RShowViews)          -> return $ ShowPlan SViews
  RQDrop (RDrop RDropConnector x)    -> return $ DropPlan False (DConnector x)
  RQDrop (RDrop RDropStream x)       -> return $ DropPlan False (DStream x)
  RQDrop (RDrop RDropView x)         -> return $ DropPlan False (DView x)
  RQDrop (RDropIf RDropConnector x)  -> return $ DropPlan True (DConnector x)
  RQDrop (RDropIf RDropStream x)     -> return $ DropPlan True (DStream x)
  RQDrop (RDropIf RDropView x)       -> return $ DropPlan True (DView x)
  RQTerminate (RTerminateQuery qid)  -> return $ TerminatePlan (OneQuery $ T.pack qid)
  RQTerminate RTerminateAll          -> return $ TerminatePlan AllQueries
  --RQSelectView rSelectView           -> return $ SelectViewPlan rSelectView
  RQExplain rexplain                 -> return $ ExplainPlan rexplain
  RQPause (RPauseConnector name)     -> return $ PausePlan (PauseObjectConnector name)
  RQResume (RResumeConnector name)   -> return $ ResumePlan (ResumeObjectConnector name)

--------------------------------------------------------------------------------
elab :: Text -> IO HStreamPlan
elab sql = do
  ast <- parseAndRefine sql
  case ast of
    RQSelect sel -> do
      let subgraph = Subgraph 0
      let (startBuilder, _) = addSubgraph emptyGraphBuilder subgraph
      (endBuilder, ins, out) <- elabRSelect sel startBuilder subgraph
      return $ SelectPlan ins out endBuilder
    _ -> undefined


--------------------------------------------------------------------------------
data In = In
  { inNode   :: Node
  , inStream :: Text
  , inWindow :: Maybe WindowType
  } deriving (Eq, Show)

newtype Out = Out { outNode :: Node } deriving (Eq, Show)

elabRSelect :: RSelect
            -> GraphBuilder Row
            -> Subgraph
            -> IO (GraphBuilder Row, [In], Out)
elabRSelect (RSelect sel frm whr grp hav) startBuilder subgraph = do
  (builder_1, ins1, Out node_from) <- elabFrom frm grp startBuilder subgraph
  (builder_2, ins2, Out node_where) <- elabRWhere whr grp builder_1 subgraph node_from Nothing
  (builder_3, ins3, Out node_select) <- elabRSel sel grp builder_2 subgraph node_where Nothing
  (builder_4, ins4, Out node_hav) <- elabRHaving hav grp builder_3 subgraph node_select Nothing
  let (builder_5, node_out) = addNode builder_4 subgraph (OutputSpec node_hav)
  return (builder_5, L.nub (ins1++ins2++ins3++ins4), Out node_out)

elabRValueExpr :: RValueExpr
               -> RGroupBy
               -> GraphBuilder Row
               -> Subgraph
               -> Node
               -> Maybe Text
               -> IO (GraphBuilder Row, [In], Out)
elabRValueExpr expr grp startBuilder subgraph startNode startStream_m = case expr of
  RExprCast _ e typ -> do
    (builder1, ins, out) <- elabRValueExpr e grp startBuilder subgraph startNode startStream_m
    let mapper = Mapper undefined -- TODO: elabCast
    let (builder2, node) =
          addNode builder1 subgraph (MapSpec (outNode out) mapper)
    return (builder2, ins, Out node)
  RExprArray name es -> do
    (builder1, ins, outs) <-
      foldM (\acc e -> do
                let (accBuilder, accIns, accOuts) = acc
                (curBuilder, curIns, curOut) <-
                  elabRValueExpr e grp accBuilder subgraph startNode startStream_m
                return (curBuilder, L.nub (accIns++curIns), curOut:accOuts)
            ) (startBuilder, [], []) es
    let composer = Composer $ \os ->
                     let vs = L.map (snd . L.head . HM.toList) os
                      in HM.fromList [(SKey (T.pack name) startStream_m Nothing, FlowArray vs)]
    let (builder2, node) = addNode builder1 subgraph (ComposeSpec (outNode <$> outs) composer)
    return (builder2, ins, Out node)
  RExprMap name emap -> do
    (builder1, ins, outTups) <-
      foldM (\acc (ek,ev) -> do
                let (accBuilder, accIns, accOuts) = acc
                (curBuilder, kIns, kOut) <-
                  elabRValueExpr ek grp accBuilder subgraph startNode startStream_m
                (curBuilder', vIns, vOut) <-
                  elabRValueExpr ev grp curBuilder subgraph startNode startStream_m
                return (curBuilder',
                        L.nub (accIns++kIns++vIns),
                        (kOut,vOut):accOuts)
            ) (startBuilder, [], []) (Map.assocs emap)
    let composer = Composer $ \os ->
                     let vs = L.map (snd . L.head . HM.toList ) os
                      in HM.fromList [(SKey (T.pack name) startStream_m Nothing, FlowMap (mkMap vs))]
    let (kOuts,vOuts) = L.unzip outTups
    let (builder2, node) =
          addNode builder1 subgraph (ComposeSpec (outNode <$> (kOuts++vOuts)) composer)
    return (builder2, ins, Out node)
    where
      mkMap :: [FlowValue] -> Map.Map FlowValue FlowValue
      mkMap xs = let (kxs, vxs) = L.splitAt (L.length xs `div` 2) xs
                  in Map.fromList (L.zip kxs vxs)
  RExprAccessMap name emap ek -> do
    (builder1, ins1, out1) <-
      elabRValueExpr emap grp startBuilder subgraph startNode startStream_m
    (builder2, ins2, out2) <-
      elabRValueExpr ek grp builder1 subgraph startNode startStream_m
    let composer = Composer $ \[omap,okey] ->
                     let (FlowMap theMap) = L.head (HM.elems omap)
                         theKey = L.head (HM.elems okey)
                      in HM.fromList [(SKey (T.pack name) startStream_m Nothing, theMap Map.! theKey)]
    let (builder3, node) =
          addNode builder2 subgraph (ComposeSpec (outNode <$> [out1,out2]) composer)
    return (builder3, L.nub (ins1++ins2), Out node)
  RExprAccessArray name earr rhs -> do
    (builder1, ins, Out out) <-
      elabRValueExpr earr grp startBuilder subgraph startNode startStream_m
    let mapper = Mapper $ \o -> let (FlowArray arr) = L.head (HM.elems o)
                                 in case rhs of
                   RArrayAccessRhsIndex n -> HM.fromList [(SKey (T.pack name) startStream_m Nothing, arr L.!! n)]
                   RArrayAccessRhsRange start_m end_m ->
                     let start = fromMaybe 0 start_m
                         end   = fromMaybe (maxBound :: Int) end_m
                      in HM.fromList [(SKey (T.pack name) startStream_m Nothing, FlowArray (L.drop start (L.take end arr)))]
    let (builder2, node) =
          addNode builder1 subgraph (MapSpec out mapper)
    return (builder2, ins, Out node)
  RExprCol _ stream_m field -> do
    let mapper = Mapper $ \o ->
          case stream_m of
            Nothing     -> HM.filterWithKey (\(SKey k _ _) _ -> k == field) o
            Just stream -> HM.filterWithKey (\(SKey k s _) _ -> k == field && s == stream_m) o
    let (builder1, node) =
          addNode startBuilder subgraph (MapSpec startNode mapper)
    return (builder1, [], Out node)
  RExprConst _ constant -> do
    let mapper = mkConstantMapper constant startStream_m
    let (builder1, node) =
          addNode startBuilder subgraph (MapSpec startNode mapper)
    return (builder1, [], Out node)
  RExprAggregate _ agg -> elabAggregate agg grp startBuilder subgraph startNode startStream_m

mkConstantMapper :: Constant -> Maybe Text -> Mapper Row
mkConstantMapper constant startStream_m =
  let v = constantToFlowValue constant
   in case constant of
        ConstantNull -> Mapper $ \_ -> HM.fromList [(SKey "null" startStream_m Nothing, v)]
        ConstantInt n -> Mapper $ \_ -> HM.fromList [(SKey (T.pack (show n)) startStream_m Nothing, v)]
        ConstantFloat n -> Mapper $ \_ -> HM.fromList [(SKey (T.pack (show n)) startStream_m Nothing, v)]
        ConstantNumeric n -> Mapper $ \_ -> HM.fromList [(SKey (T.pack (show n)) startStream_m Nothing, v)]
        ConstantText t -> Mapper $ \_ -> HM.fromList [(SKey t startStream_m Nothing, v)]
        ConstantBoolean b -> Mapper $ \_ -> HM.fromList [(SKey (T.pack (show b)) startStream_m Nothing, v)]
        ConstantDate d -> Mapper $ \_ -> HM.fromList [(SKey (T.pack (show d)) startStream_m Nothing, v)]
        ConstantTime t -> Mapper $ \_ -> HM.fromList [(SKey (T.pack (show t)) startStream_m Nothing, v)]
        ConstantTimestamp ts -> Mapper $ \_ -> HM.fromList [(SKey (T.pack (show ts)) startStream_m Nothing, v)]
        ConstantInterval i -> Mapper $ \_ -> HM.fromList [(SKey (T.pack (show i)) startStream_m Nothing, v)]
        ConstantBytea bs -> Mapper $ \_ -> HM.fromList [(SKey (cBytesToText bs) startStream_m Nothing, v)]
        ConstantJsonb json -> Mapper $ \_ -> HM.fromList [(SKey (T.pack (show json)) startStream_m Nothing, v)]
        ConstantArray arr -> Mapper $ \_ -> HM.fromList [(SKey (T.pack (show arr)) startStream_m Nothing, v)]
        ConstantMap m -> Mapper $ \_ -> HM.fromList [(SKey (T.pack (show m)) startStream_m Nothing, v)]

elabRTableRef :: RTableRef -> RGroupBy -> GraphBuilder Row -> Subgraph -> IO (GraphBuilder Row, [In], Out)
elabRTableRef ref grp startBuilder subgraph =
  case ref of
    RTableRefSimple s alias_m -> do
      let (builder, node) = addNode startBuilder subgraph InputSpec
          inner = In { inNode = node, inStream = fromMaybe s alias_m, inWindow = Nothing}
          outer = Out { outNode = node }
      return (builder, [inner], outer)
    RTableRefSubquery select alias_m -> do
      (builder, ins, out) <- elabRSelect select startBuilder subgraph
      return (builder, ins, out)
    RTableRefCrossJoin ref1 ref2 alias_m -> do
      (builder1, ins1, out1) <- elabRTableRef ref1 grp startBuilder subgraph
      (builder2, ins2, out2) <- elabRTableRef ref2 grp builder1 subgraph
      let (builder3, node1_indexed) = addNode builder2 subgraph (IndexSpec (outNode out1))
          (builder4, node2_indexed) = addNode builder3 subgraph (IndexSpec (outNode out2))
      let keygen1 = constantKeygen
          keygen2 = constantKeygen
          joiner = Joiner HM.union
      let (builder, node) = addNode builder2 subgraph
                            (JoinSpec node1_indexed node2_indexed keygen1 keygen2 joiner)
      return (builder, L.nub (ins1++ins2), Out node)
    RTableRefNaturalJoin ref1 typ ref2 alias_m -> do
      (builder1, ins1, out1) <- elabRTableRef ref1 grp startBuilder subgraph
      (builder2, ins2, out2) <- elabRTableRef ref2 grp builder1 subgraph
      undefined
    RTableRefJoinOn ref1 typ ref2 expr alias_m -> do
      (builder1, ins1, out1) <- elabRTableRef ref1 grp startBuilder subgraph
      (builder2, ins2, out2) <- elabRValueExpr expr grp builder1 subgraph (outNode out1) alias_m
      (builder3, ins3, out3) <- elabRTableRef ref2 grp builder2 subgraph
      let composer = Composer $ \[os1, oexpr] ->
                 makeExtra "__s1__" os1 `HM.union` makeExtra "__expr__" oexpr
          (builder4, node1_with_expr) = addNode builder3 subgraph (ComposeSpec (outNode <$> [out1,out2]) composer)
          (builder5, node1_indexed) = addNode builder4 subgraph (IndexSpec node1_with_expr)
          (builder6, node2_indexed) = addNode builder5 subgraph (IndexSpec (outNode out3))
      let keygen1 = \o -> HM.mapKeys (\_ -> SKey "__k1__" Nothing Nothing) $
                          getExtra "__expr__" o
          keygen2 = \_ -> HM.fromList [(SKey "__k1__" Nothing Nothing, FlowBoolean True)]
          joiner = Joiner $ \o1 o2 -> let o1' = getExtraAndReset "__s1__" o1
                                          o2' = o2
                                       in o1' <> o2' -- FIXME: join type
      let (builder7, node) = addNode builder6 subgraph (JoinSpec node1_indexed node2_indexed keygen1 keygen2 joiner)
      return (builder7, L.nub (ins1++ins2++ins3), Out node)
    RTableRefJoinUsing ref1 typ ref2 fields alias_m -> do
      (builder1, ins1, out1) <- elabRTableRef ref1 grp startBuilder subgraph
      (builder2, ins2, out2) <- elabRTableRef ref2 grp builder1 subgraph
      let (builder3, node1_indexed) = addNode builder2 subgraph (IndexSpec (outNode out1))
          (builder4, node2_indexed) = addNode builder3 subgraph (IndexSpec (outNode out2))
      let keygen1 = \o -> HM.mapKeys (\(SKey f _ _) -> SKey f Nothing Nothing) $
                          HM.filterWithKey (\(SKey f _ _) _ -> L.elem f fields) o
          keygen2 = keygen1
          joiner = Joiner $ \o1 o2 -> o1 <> o2 --  FIXME: join type
      let (builder5, node) = addNode builder4 subgraph (JoinSpec node1_indexed node2_indexed keygen1 keygen2 joiner)
      return (builder5, L.nub (ins1++ins2), Out node)
    RTableRefWindowed ref win alias_m -> do
      (builder1, ins, out) <- elabRTableRef ref grp startBuilder subgraph
      let ins' = L.map (\thisIn -> thisIn { inWindow = Just win }) (L.nub ins) -- FIXME: when both `win(s1)` and `s1` exist
      return (builder1, ins', out)

elabFrom :: RFrom -> RGroupBy -> GraphBuilder Row -> Subgraph -> IO (GraphBuilder Row, [In], Out)
elabFrom (RFrom []) grp baseBuilder subgraph = throwSQLException CodegenException Nothing "Impossible happened (empty FROM clause)"
elabFrom (RFrom [ref]) grp baseBuilder subgraph = elabRTableRef ref grp baseBuilder subgraph
elabFrom (RFrom refs) grp baseBuilder subgraph = do
  (builder1, ins1, out1) <- elabRTableRef (L.head refs) grp baseBuilder subgraph
  foldM (\(oldBuilder, accIns, oldOut) thisRef -> do
            (thisBuilder, thisIns, thisOut) <- elabRTableRef thisRef grp oldBuilder subgraph
            let (builder1, oldNode_indexed) = addNode thisBuilder subgraph (IndexSpec (outNode oldOut))
                (builder2, thisNode_indexed) = addNode builder1 subgraph (IndexSpec (outNode thisOut))
            let keygen1 = constantKeygen
                keygen2 = constantKeygen
                joiner = Joiner $ HM.union
            let (builder3, node) = addNode builder2 subgraph (JoinSpec oldNode_indexed thisNode_indexed keygen1 keygen2 joiner)
            return (builder3, L.nub (accIns++thisIns), Out node)
        ) (builder1, ins1, out1) (L.tail refs)

----
data AggregateComponents = AggregateComponents
  { aggregateInit :: FlowObject
  , aggregateF    :: FlowObject -> FlowObject -> FlowObject
  }

elabAggregate :: Aggregate
              -> RGroupBy
              -> GraphBuilder Row
              -> Subgraph
              -> Node
              -> Maybe Text
              -> IO (GraphBuilder Row, [In], Out)
elabAggregate agg grp startBuilder subgraph startNode startStream_m =
  case agg of
    Nullary _ -> do
      let (builder_1, indexed) = addNode startBuilder subgraph (IndexSpec startNode)
      let AggregateComponents{..} = genAggregateComponents agg startStream_m
      let reducer = Reducer aggregateF
          keygen = elabGroupBy grp
      let (builder_2, node) = addNode builder_1 subgraph (ReduceSpec indexed aggregateInit keygen reducer)
      return (builder_2, [], Out node)
    Unary _ expr -> do
      (builder_1, ins, Out node_expr) <-
        elabRValueExpr expr grp startBuilder subgraph startNode startStream_m
      let composer = Composer $ \[oexpr,ofrom] ->
            makeExtra "__expr__" oexpr `HM.union` makeExtra "__from__" ofrom
      let (builder_2, composed) =
            addNode builder_1 subgraph (ComposeSpec [node_expr,startNode] composer)
      let AggregateComponents{..} = genAggregateComponents agg startStream_m
      let (builder_3, indexed) =
            addNode builder_2 subgraph (IndexSpec composed)
      let reducer = Reducer aggregateF
          keygen = elabGroupBy grp
      let (builder_4, node) = addNode builder_3 subgraph (ReduceSpec indexed aggregateInit keygen reducer)
      return (builder_4, ins, Out node)
    Binary _ expr1 expr2 -> do
      (builder_1, ins1, Out node_expr1) <-
        elabRValueExpr expr1 grp startBuilder subgraph startNode startStream_m
      (builder_2, ins2, Out node_expr2) <-
        elabRValueExpr expr2 grp builder_1 subgraph startNode startStream_m
      let composer = Composer $ \[oexpr1,oexpr2,ofrom] ->
            makeExtra "__expr1__" oexpr1 `HM.union`
            makeExtra "__expr2__" oexpr2 `HM.union`
            makeExtra "__from__"  ofrom
      let (builder_3, composed) =
            addNode builder_2 subgraph (ComposeSpec [node_expr1,node_expr2,startNode] composer)
      let AggregateComponents{..} = genAggregateComponents agg startStream_m
      let (builder_4, indexed) =
            addNode builder_3 subgraph (IndexSpec composed)
      let reducer = Reducer aggregateF
          keygen = elabGroupBy grp
      let (builder_5, node) = addNode builder_4 subgraph (ReduceSpec indexed aggregateInit keygen reducer)
      return (builder_5, L.nub (ins1++ins2), Out node)

elabGroupBy :: RGroupBy -> FlowObject -> FlowObject
elabGroupBy RGroupByEmpty o = constantKeygen o
elabGroupBy (RGroupBy xs) o = HM.unions $
  L.map (\(s_m, f) ->
           HM.filterWithKey (\(SKey f' s_m' _) v ->
                               case s_m of
                                 Nothing -> f == f'
                                 Just s' -> f == f' && s_m' == s_m
                            ) o
        ) xs

genAggregateComponents :: Aggregate
                       -> Maybe Text
                       -> AggregateComponents
genAggregateComponents agg startStream_m =
  case agg of
       Nullary AggCountAll ->
         AggregateComponents
         { aggregateInit = HM.singleton (SKey (T.pack (show agg)) startStream_m (Just "__reduced__")) (FlowInt 0)
         , aggregateF    = \acc row ->
             let [(k, FlowInt acc_x)] = HM.toList $ getExtra "__reduced__" acc
              in HM.fromList [(k, FlowInt (acc_x + 1))]
         }

       Unary AggCount expr ->
         AggregateComponents
         { aggregateInit = HM.singleton (SKey (T.pack (show agg)) startStream_m (Just "__reduced__")) (FlowInt 0)
         , aggregateF = \acc row ->
             let [(k, FlowInt acc_x)] = HM.toList $ getExtra "__reduced__" acc
              in if HM.null (getExtra "__expr__" row) then
                   HM.fromList [(k, FlowInt acc_x)] else
                   HM.fromList [(k, FlowInt (acc_x + 1))]
         }

       Unary AggSum expr ->
         AggregateComponents
         { aggregateInit = HM.singleton (SKey (T.pack (show agg)) startStream_m (Just "__reduced__")) (FlowNumeral 0)
         , aggregateF = \acc row ->
             let [(k, FlowNumeral acc_x)] = HM.toList $ getExtra "__reduced__" acc
                 [(_, FlowNumeral row_x)] = HM.toList $ getExtra "__expr__" row
              in HM.fromList [(k, FlowNumeral (acc_x + row_x))]
         }

       Unary AggMax expr ->
         AggregateComponents
         { aggregateInit = HM.singleton (SKey (T.pack (show agg)) startStream_m (Just "__reduced__")) (FlowNumeral 0)
         , aggregateF = \acc row ->
             let [(k, FlowNumeral acc_x)] = HM.toList $ getExtra "__reduced__" acc
                 [(_, FlowNumeral row_x)] = HM.toList $ getExtra "__expr__" row
              in HM.fromList [(k, FlowNumeral (max acc_x row_x))]
         }

       Unary AggMin expr ->
         AggregateComponents
         { aggregateInit = HM.singleton (SKey (T.pack (show agg)) startStream_m (Just "__reduced__")) (FlowNumeral 0)
         , aggregateF = \acc row ->
             let [(k, FlowNumeral acc_x)] = HM.toList $ getExtra "__reduced__" acc
                 [(_, FlowNumeral row_x)] = HM.toList $ getExtra "__expr__" row
              in HM.fromList [(k, FlowNumeral (min acc_x row_x))]
         }
       Binary AggTopK expr1 expr2 ->
         AggregateComponents
         { aggregateInit = HM.singleton (SKey (T.pack (show agg)) startStream_m (Just "__reduced__")) FlowNull
         , aggregateF = \acc row ->
             let [(k,v)] = HM.toList $ getExtra "__reduced__" acc
                 [(_,v1)] = HM.toList $ getExtra "__expr1__" row
                 [(_,FlowInt n)] = HM.toList $ getExtra "__expr2__" row
              in case v of
                   FlowNull -> HM.fromList [(k, FlowArray [v1])]
                   FlowArray vs ->
                     let vs' = L.take n (L.sortBy (flip compare) (v1:vs))
                      in HM.fromList [(k, FlowArray vs')]
         }
       Binary AggTopKDistinct expr1 expr2 ->
         AggregateComponents
         { aggregateInit = HM.singleton (SKey (T.pack (show agg)) startStream_m (Just "__reduced__")) FlowNull
         , aggregateF = \acc row ->
             let [(k,v)] = HM.toList $ getExtra "__reduced__" acc
                 [(_,v1)] = HM.toList $ getExtra "__expr1__" row
                 [(_,FlowInt n)] = HM.toList $ getExtra "__expr2__" row
              in case v of
                   FlowNull -> HM.fromList [(k, FlowArray [v1])]
                   FlowArray vs ->
                     let vs' = L.take n (L.sortBy (flip compare) (L.nub (v1:vs)))
                      in HM.fromList [(k, FlowArray vs')]
         }
       _ -> throwSQLException CodegenException Nothing ("Unsupported aggregate function: " <> show agg)


elabRWhere :: RWhere
           -> RGroupBy
           -> GraphBuilder Row
           -> Subgraph
           -> Node
           -> Maybe Text
           -> IO (GraphBuilder Row, [In], Out)
elabRWhere whr grp startBuilder subgraph startNode startStream_m = case whr of
  RWhereEmpty -> return (startBuilder, [], Out startNode)
  RWhere expr -> do
    (builder_1, ins, Out node_expr) <-
      elabRValueExpr expr grp startBuilder subgraph startNode startStream_m
    let composer = Composer $ \[oexpr,ofrom] ->
          makeExtra "__expr__" oexpr `HM.union` makeExtra "__from__" ofrom
    let (builder_2, composed) = addNode builder_1 subgraph (ComposeSpec [node_expr,startNode] composer)
    let filter = Filter $ \o ->
          let oexpr = getExtra "__expr__" o
           in case HM.toList oexpr of
                [(_, FlowBoolean True)] -> True
                _                       -> False
    let mapper = Mapper $ \o -> getExtraAndReset "__from__" o
    let (builder_3, filtered) = addNode builder_2 subgraph (FilterSpec composed filter)
    let (builder_4, mapped) = addNode builder_3 subgraph (MapSpec filtered mapper)
    return (builder_4, ins, Out mapped)

elabRHaving :: RHaving
            -> RGroupBy
            -> GraphBuilder Row
            -> Subgraph
            -> Node
            -> Maybe Text
            -> IO (GraphBuilder Row, [In], Out)
elabRHaving hav grp startBuilder subgraph startNode startStream_m = case hav of
  RHavingEmpty -> return (startBuilder, [], Out startNode)
  RHaving expr -> elabRWhere (RWhere expr) grp startBuilder subgraph startNode startStream_m


elabRSelectItem :: RSelectItem
                -> RGroupBy
                -> GraphBuilder Row
                -> Subgraph
                -> Node
                -> Maybe Text
                -> IO (GraphBuilder Row, [In], Out)
elabRSelectItem item grp startBuilder subgraph startNode startStream_m =
  case item of
    RSelectItemProject expr alias_m -> do
      (builder_1, ins, Out node_expr) <- elabRValueExpr expr grp startBuilder subgraph startNode startStream_m
      case alias_m of
        Nothing -> return (builder_1, ins, Out node_expr)
        Just alias -> do
          let mapper = Mapper $ \o ->
                HM.mapKeys (\(SKey f s_m extra_m) -> SKey alias s_m extra_m) o
          let (builder_2, node) = addNode builder_1 subgraph (MapSpec node_expr mapper)
          return (builder_2, ins, Out node)
    RSelectItemAggregate agg alias_m -> do
      (builder_1, ins, Out node_agg) <- elabAggregate agg grp startBuilder subgraph startNode startStream_m
      case alias_m of
        Nothing -> return (builder_1, ins, Out node_agg)
        Just alias -> do
          let mapper = Mapper $ \o ->
                HM.mapKeys (\(SKey f s_m extra_m) -> SKey alias s_m extra_m) o
          let (builder_2, node) = addNode builder_1 subgraph (MapSpec node_agg mapper)
          return (builder_2, ins, Out node)
    RSelectProjectQualifiedAll s -> do
      let mapper = Mapper $ \o ->
            HM.filterWithKey (\(SKey f s_m extra_m) v -> s_m == Just s) o
      let (builder_1, node) = addNode startBuilder subgraph (MapSpec startNode mapper)
      return (builder_1, [], Out node)
    RSelectProjectAll -> return (startBuilder, [], Out startNode)

elabRSel :: RSel
         -> RGroupBy
         -> GraphBuilder Row
         -> Subgraph
         -> Node
         -> Maybe Text
         -> IO (GraphBuilder Row, [In], Out)
elabRSel (RSel items) grp startBuilder subgraph startNode startStream_m = do
  (builder_1, ins, outs) <-
    foldM (\acc item -> do
              let (oldBuilder, oldIns, oldOuts) = acc
              (newBuilder, ins, out) <-
                elabRSelectItem item grp oldBuilder subgraph startNode startStream_m
              return (newBuilder, L.nub (ins++oldIns), out:oldOuts)
          ) (startBuilder, [], []) items
  let composer = Composer $ \os -> L.foldl1 HM.union os
  let (builder_2, node) = addNode builder_1 subgraph (ComposeSpec (outNode <$> outs) composer)
  return (builder_2, ins, Out node)











{-

fuseAggregateComponents :: [AggregateComponents] -> AggregateComponents
fuseAggregateComponents components =
  AggregateComponents
  { aggregateInit = HM.unions (aggregateInit <$> components)
  , aggregateF = \old recordValue -> L.foldr (\f acc -> f acc recordValue) old (aggregateF <$> components)
  }

----
genFilterRFromHaving :: RHaving -> Object -> Bool
genFilterRFromHaving RHavingEmpty   = const True
genFilterRFromHaving (RHaving cond) = genFilterR (RWhere cond)

genFilterNodeFromHaving :: RHaving -> Node -> NodeSpec
genFilterNodeFromHaving hav prevNode = FilterSpec prevNode filter'
  where filter' = Filter (genFilterRFromHaving hav)

----
genGraphBuilder :: HasCallStack
                => Maybe StreamName
                -> RSelect
                -> IO (GraphBuilder, [(Node, StreamName)], (Node, StreamName), (Maybe RWindow))
genGraphBuilder sinkStream' select@(RSelect sel frm whr grp hav) = do
  let baseSubgraph = Subgraph 0
  let (startBuilder,_) = addSubgraph emptyGraphBuilder baseSubgraph

  (baseBuilder, inNodesWithStreams, thenNode) <- genSourceGraphBuilder frm startBuilder

  let preMapNode = genPreMapNode sel thenNode
      (builder_1, nodePreMap) = addNode baseBuilder baseSubgraph preMapNode

  let filterNode = genFilterNode whr nodePreMap
      (builder_2, nodeFilter) = addNode builder_1 baseSubgraph filterNode

  (nextBuilder, nextNode) <- case grp of
        RGroupByEmpty -> return (builder_2, nodeFilter)
        _ -> do
          let agg = genAggregateComponents sel
          groupbyKeygen <- genGroupByKeygen grp
          let (builder', nodeIndex) = addNode builder_2 baseSubgraph (IndexSpec nodeFilter)
          return $ addNode builder' baseSubgraph (ReduceSpec nodeIndex (aggregateInit agg) groupbyKeygen (Reducer $ aggregateF agg))

  let postMapNode = genPostMapNode sel nextNode
      (builder_3, nodePostMap) = addNode nextBuilder baseSubgraph postMapNode

  let window = case grp of
        RGroupByEmpty    -> Nothing
        RGroupBy _ _ win -> win

  let havingNode = genFilterNodeFromHaving hav nodePostMap
      (builder_4, nodeHav) = addNode builder_3 baseSubgraph havingNode

  sink <- maybe genRandomSinkStream return sinkStream'
  return (builder_4, inNodesWithStreams, (nodeHav,sink), window)

genGraphBuilderWithOutput :: HasCallStack
                          => Maybe StreamName
                          -> RSelect
                          -> IO (GraphBuilder, [(Node, StreamName)], (Node, StreamName), (Maybe RWindow))
genGraphBuilderWithOutput sinkStream' select = do
  let baseSubgraph = Subgraph 0
  (builder_1, inNodesWithStreams, (lastNode,sink), window) <-
    genGraphBuilder sinkStream' select
  let (builder_2, nodeOutput) = addNode builder_1 baseSubgraph (OutputSpec lastNode)
  return (builder_2, inNodesWithStreams, (nodeOutput,sink), window)

--------------------------------------------------------------------------------

mapAlias :: SelectViewSelect -> HM.HashMap T.Text Value -> HM.HashMap T.Text Value
mapAlias SVSelectAll         res = res
mapAlias (SVSelectFields []) _   = HM.empty
mapAlias (SVSelectFields xs) res = HM.fromList
  let ret = xs <&> \(proj1, proj2) ->
        let key = T.pack proj2 & unQuote
            val = HM.lookup (unQuote proj1) res
        in  (key, val)
  in  RIO.filter (isJust . snd) ret <&> second fromJust
  where
    unQuote name
      | T.length name <  2   = name
      | T.head   name /= '`' = name
      | T.last   name /= '`' = name
      | otherwise =
          (snd . fromJust) (T.uncons name ) & \name' ->
          (fst . fromJust) (T.unsnoc name')

--------------------------------------------------------------------------------
-}
pattern ConnectorWritePlan :: T.Text -> HStreamPlan
pattern ConnectorWritePlan name <- (getLookupConnectorName -> Just name)

getLookupConnectorName :: HStreamPlan -> Maybe T.Text
getLookupConnectorName (CreateConnectorPlan _ name _ _ _)        = Just name
getLookupConnectorName (PausePlan (PauseObjectConnector name))   = Just name
getLookupConnectorName (ResumePlan (ResumeObjectConnector name)) = Just name
getLookupConnectorName (DropPlan _ (DConnector name))            = Just name
getLookupConnectorName _                                         = Nothing
