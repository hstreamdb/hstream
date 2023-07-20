{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE CPP                 #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StrictData          #-}

module HStream.SQL.Codegen.V2 where

#ifdef HStreamUseV2Engine
import           Control.Concurrent
import           Data.Aeson                            (Object, Value (..))
import qualified Data.Aeson                            as Aeson
import           Data.Bifunctor
import           Data.ByteString                       (ByteString)
import qualified Data.ByteString.Lazy                  as BL
import qualified Data.HashMap.Strict                   as HM
import           Data.Int                              (Int64)
import qualified Data.List                             as L
import           Data.Maybe
import           Data.Text                             (Text)
import qualified Data.Text                             as T
import           Data.Text.Prettyprint.Doc             as PP
import           Data.Text.Prettyprint.Doc.Render.Text as PP
import           GHC.Stack                             (HasCallStack)
import qualified Proto3.Suite                          as PB

import           DiffFlow.Graph
import qualified DiffFlow.Graph                        as DiffFlow
import           DiffFlow.Types
import           HStream.SQL.AST
import           HStream.SQL.Codegen.ColumnCatalog
import           HStream.SQL.Codegen.Common
import           HStream.SQL.Parse                     (parseAndRefine)
import           HStream.SQL.Planner
import qualified HStream.SQL.Planner                   as Planner
import           HStream.SQL.Planner.Pretty            ()
import           HStream.Utils                         (jsonObjectToStruct)
import qualified HStream.Utils.Aeson                   as HsAeson

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
  = CreatePlan          StreamName RStreamOptions
  | CreateConnectorPlan ConnectorType ConnectorName Text Bool (HM.HashMap Text Value)
  | InsertPlan          StreamName InsertType ByteString
  | DropPlan            CheckIfExist DropObject
  | ShowPlan            ShowObject
  | TerminatePlan       TerminationSelection
  | ExplainPlan         Text
  | PausePlan           PauseObject
  | ResumePlan          ResumeObject
  | SelectPlan          [In] Out (GraphBuilder Row)
  | PushSelectPlan      [In] Out (GraphBuilder Row)
  | CreateBySelectPlan  StreamName [In] Out (GraphBuilder Row) RStreamOptions -- FIXME
  | CreateViewPlan      ViewName [In] Out (GraphBuilder Row) (MVar (DataChangeBatch Row Int64)) -- FIXME

--------------------------------------------------------------------------------
streamCodegen :: HasCallStack => Text -> IO HStreamPlan
streamCodegen input = parseAndRefine input >>= hstreamCodegen

hstreamCodegen :: HasCallStack => RSQL -> IO HStreamPlan
hstreamCodegen = \case
  RQSelect select -> do
    let subgraph = Subgraph 0
    let (startBuilder, _) = addSubgraph emptyGraphBuilder subgraph
    (endBuilder, ins, out) <- elabRSelectWithOut select startBuilder subgraph
    return $ SelectPlan ins out endBuilder
  RQPushSelect select -> do
    let subgraph = Subgraph 0
    let (startBuilder, _) = addSubgraph emptyGraphBuilder subgraph
    (endBuilder, ins, out) <- elabRSelectWithOut select startBuilder subgraph
    return $ PushSelectPlan ins out endBuilder
  RQCreate (RCreateAs stream select rOptions) -> do
    let subgraph = Subgraph 0
    let (startBuilder, _) = addSubgraph emptyGraphBuilder subgraph
    (endBuilder, ins, out) <- elabRSelectWithOut select startBuilder subgraph
    return $ CreateBySelectPlan stream ins out endBuilder rOptions
  RQCreate (RCreateView view select) -> do
    let subgraph = Subgraph 0
    let (startBuilder, _) = addSubgraph emptyGraphBuilder subgraph
    (endBuilder, ins, out) <- elabRSelectWithOut select startBuilder subgraph
    accumulation <- newMVar emptyDataChangeBatch
    return $ CreateViewPlan view ins out endBuilder accumulation
  RQCreate (RCreate stream rOptions) -> return $ CreatePlan stream rOptions
  RQCreate (RCreateConnector cType cName cTarget ifNotExist (RConnectorOptions cOptions)) ->
    return $ CreateConnectorPlan cType cName cTarget ifNotExist cOptions
  RQInsert (RInsert stream tuples)   -> do
    let jsonObj = HsAeson.fromList $
          bimap HsAeson.fromText (flowValueToJsonValue . constantToFlowValue) <$> tuples
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
  RQTerminate (RTerminateQuery qid)  -> return $ TerminatePlan (OneQuery qid)
  RQTerminate RTerminateAll          -> return $ TerminatePlan AllQueries
  RQExplain rselect                  -> do
    let relationExpr = decouple rselect
    return $ ExplainPlan (PP.renderStrict $ PP.layoutPretty PP.defaultLayoutOptions (PP.pretty relationExpr))
  RQPause (RPauseConnector name)     -> return $ PausePlan (PauseObjectConnector name)
  RQResume (RResumeConnector name)   -> return $ ResumePlan (ResumeObjectConnector name)

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
elabRSelect select startBuilder subgraph = do
  let relationExpr = decouple select
  relationExprToGraph relationExpr startBuilder subgraph

elabRSelectWithOut :: RSelect
                   -> GraphBuilder Row
                   -> Subgraph
                   -> IO (GraphBuilder Row, [In], Out)
elabRSelectWithOut select startBuilder subgraph = do
  (builder, ins, out) <- elabRSelect select startBuilder subgraph
  let (builder', node') = addNode builder subgraph (OutputSpec (outNode out))
  return (builder', ins, Out node')
--------------------------------------------------------------------------------
relationExprToGraph :: RelationExpr
                    -> GraphBuilder Row
                    -> Subgraph
                    -> IO (GraphBuilder Row, [In], Out)
relationExprToGraph relation startBuilder subgraph = case relation of
  StreamScan stream -> do
    let (builder, node) = addNode startBuilder subgraph InputSpec
        inner = In { inNode = node, inStream = stream, inWindow = Nothing }
        out = Out { outNode = node }
    return (builder, [inner], out)
  StreamRename r alias -> do
    (builder1, ins1, out1) <- relationExprToGraph r startBuilder subgraph
    let mapper = Mapper $ \o -> Right $ streamRenamer alias o
        (builder2, node) = addNode builder1 subgraph (MapSpec (outNode out1) mapper)
    let out = Out { outNode = node }
    return (builder2, ins1, out)
  CrossJoin r1 r2 -> do
    (builder1, ins1, out1) <- relationExprToGraph r1 startBuilder subgraph
    (builder2, ins2, out2) <- relationExprToGraph r2 builder1 subgraph
    let (builder3, node1_indexed) = addNode builder2 subgraph (IndexSpec (outNode out1))
        (builder4, node2_indexed) = addNode builder3 subgraph (IndexSpec (outNode out2))
    let joinCond = alwaysTrueJoinCond
        joinType = MergeJoinInner
        joiner = Joiner HM.union
        nullRowgen = HM.map (const FlowNull)
    let (builder, node) = addNode builder4 subgraph
                          (JoinSpec node1_indexed node2_indexed joinType joinCond joiner nullRowgen)
    return (builder, L.nub (ins1++ins2), Out node)
  LoopJoinOn r1 r2 expr typ -> do
    (builder1, ins1, out1) <- relationExprToGraph r1 startBuilder subgraph
    (builder2, ins2, out2) <- relationExprToGraph r2 builder1 subgraph
    let (builder3, node1_indexed) = addNode builder2 subgraph (IndexSpec (outNode out1))
        (builder4, node2_indexed) = addNode builder3 subgraph (IndexSpec (outNode out2))
    let joinCond = \o1 o2 ->
          case scalarExprToFun expr (o1<>o2) of
            Left e  -> False -- FIXME: log error message
            Right v -> v == FlowBoolean True
        joinType = case typ of
                     InnerJoin -> MergeJoinInner
                     LeftJoin  -> MergeJoinLeft
                     RightJoin -> MergeJoinRight
                     FullJoin  -> MergeJoinFull
        joiner = Joiner HM.union
        nullRowgen = HM.map (const FlowNull)
    let (builder, node) = addNode builder4 subgraph
                          (JoinSpec node1_indexed node2_indexed joinType joinCond joiner nullRowgen)
    return (builder, L.nub (ins1++ins2), Out node)
  LoopJoinUsing r1 r2 cols typ -> do
    (builder1, ins1, out1) <- relationExprToGraph r1 startBuilder subgraph
    (builder2, ins2, out2) <- relationExprToGraph r2 builder1 subgraph
    let (builder3, node1_indexed) = addNode builder2 subgraph (IndexSpec (outNode out1))
        (builder4, node2_indexed) = addNode builder3 subgraph (IndexSpec (outNode out2))
    let joinCond = \o1 o2 ->
          HM.mapKeys (\(ColumnCatalog f _) -> ColumnCatalog f Nothing) (HM.filterWithKey (\(ColumnCatalog f s_m) _ -> isJust s_m && L.elem f cols) o1) ==
          HM.mapKeys (\(ColumnCatalog f _) -> ColumnCatalog f Nothing) (HM.filterWithKey (\(ColumnCatalog f s_m) _ -> isJust s_m && L.elem f cols) o2)
        joinType = case typ of
                     InnerJoin -> MergeJoinInner
                     LeftJoin  -> MergeJoinLeft
                     RightJoin -> MergeJoinRight
                     FullJoin  -> MergeJoinFull
        joiner = Joiner HM.union
        nullRowgen = HM.map (const FlowNull)
    let (builder, node) = addNode builder4 subgraph
                          (JoinSpec node1_indexed node2_indexed joinType joinCond joiner nullRowgen)
    return (builder, L.nub (ins1++ins2), Out node)
  LoopJoinNatural r1 r2 typ -> do
    (builder1, ins1, out1) <- relationExprToGraph r1 startBuilder subgraph
    (builder2, ins2, out2) <- relationExprToGraph r2 builder1 subgraph
    let (builder3, node1_indexed) = addNode builder2 subgraph (IndexSpec (outNode out1))
        (builder4, node2_indexed) = addNode builder3 subgraph (IndexSpec (outNode out2))
    let joinCond = \o1 o2 ->
          HM.foldlWithKey (\acc k@(ColumnCatalog f _) v ->
                               if acc then
                                 case getField (ColumnCatalog f Nothing) o2 of
                                   Nothing     -> acc
                                   Just (_,v') -> v == v'
                               else False
                            ) True o1
        joinType = MergeJoinInner
        joiner = Joiner HM.union
        nullRowgen = HM.map (const FlowNull)
    let (builder, node) = addNode builder4 subgraph
                          (JoinSpec node1_indexed node2_indexed joinType joinCond joiner nullRowgen)
    return (builder, L.nub (ins1++ins2), Out node)
  Planner.Filter r scalar -> do
    (builder1, ins1, out1) <- relationExprToGraph r startBuilder subgraph
    let filter = DiffFlow.Filter $ \o ->
          case scalarExprToFun scalar o of
            Left e  -> False -- FIXME: log error message
            Right v -> v == FlowBoolean True
    let (builder, node) = addNode builder1 subgraph (FilterSpec (outNode out1) filter)
    return (builder, ins1, Out node)
  Project r cataTups streams -> do
    (builder1, ins1, out1) <- relationExprToGraph r startBuilder subgraph
    let mapper = Mapper $ \o -> Right $
          L.foldr (\(cata_get,cata_as) acc ->
                     case getField cata_get o of
                       Nothing    -> acc
                       Just (_,v) -> HM.insert cata_as v acc
                  ) HM.empty cataTups
          `HM.union`
          L.foldr (\stream acc ->
                     acc `HM.union` (HM.filterWithKey (\(ColumnCatalog _ s_m) _ -> s_m == Just stream) o)
                  ) HM.empty streams
    let (builder, node) = addNode builder1 subgraph (MapSpec (outNode out1) mapper)
    return (builder, ins1, Out node)
  Affiliate r tups -> do
    (builder1, ins1, out1) <- relationExprToGraph r startBuilder subgraph
    let mapper = Mapper $ \o -> Right $
          L.foldr (\(cata,scalar) acc ->
                     case scalarExprToFun scalar o of
                       Left e  -> HM.insert cata FlowNull acc
                       Right v -> HM.insert cata v acc
                  ) o tups
    let (builder, node) = addNode builder1 subgraph (MapSpec (outNode out1) mapper)
    return (builder, ins1, Out node)
  Reduce r keyTups aggTups -> do
    (builder1, ins1, out1) <- relationExprToGraph r startBuilder subgraph
    let (builder2, node_indexed) = addNode builder1 subgraph (IndexSpec (outNode out1))
    let keygen = \o ->
          L.foldr (\(cata,scalar) acc ->
                      case scalarExprToFun scalar o of
                        Left _  -> HM.insert cata FlowNull acc
                        Right v -> HM.insert cata v acc
                  ) HM.empty keyTups
    let aggComp = composeAggs
                  (L.map (\(cata,agg) -> genAggregateComponent agg cata) aggTups)
    let reducer = Reducer (aggregateF aggComp)
    let (builder, node) = addNode builder2 subgraph (ReduceSpec node_indexed (aggregateInit aggComp) keygen reducer)
    return (builder, ins1, Out node)
  Distinct r -> do
    (builder1, ins1, out1) <- relationExprToGraph r startBuilder subgraph
    let (builder2, node_indexed) = addNode builder1 subgraph (IndexSpec (outNode out1))
    let (builder, node) = addNode builder2 subgraph (DistinctSpec node_indexed)
    return (builder, ins1, Out node)
  TimeWindow r win -> do
    (builder1, ins1, out1) <- relationExprToGraph r startBuilder subgraph
    let ins1' = L.map (\i -> i { inWindow = Just win }) ins1
    return (builder1, ins1', out1)
  Union r1 r2 -> do
    (builder1, ins1, out1) <- relationExprToGraph r1 startBuilder subgraph
    (builder2, ins2, out2) <- relationExprToGraph r2 builder1 subgraph
    let (builder, node) = addNode builder2 subgraph (UnionSpec (outNode out1) (outNode out2))
    return (builder, L.nub (ins1++ins2), Out node)
#endif
