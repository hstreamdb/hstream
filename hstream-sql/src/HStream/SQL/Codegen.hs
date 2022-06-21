{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StrictData          #-}

module HStream.SQL.Codegen where

import           Data.Aeson                                      (Object,
                                                                  Value (Bool, Null, Number, String))
import qualified Data.Aeson                                      as Aeson
import           Data.Bifunctor
import qualified Data.ByteString.Char8           as BSC
import           Data.Function
import           Data.Functor
import qualified Data.HashMap.Strict             as HM
import qualified Data.List                       as L
import           Data.Maybe
import           Data.Scientific                 (fromFloatDigits, scientific)
import           Data.Text                       (pack)
import qualified Data.Text                       as T
import           Data.Time                       (diffTimeToPicoseconds,
                                                  showGregorian)
import qualified Database.ClickHouseDriver.Types as Clickhouse
import qualified Database.MySQL.Base             as MySQL
import qualified Proto3.Suite                    as PB
import           RIO
import qualified RIO.ByteString.Lazy             as BL
import qualified Z.Data.CBytes                   as CB

import qualified HStream.Connector.Type          as HCT
import           HStream.SQL.AST                 hiding (StreamName)
import           HStream.SQL.Exception           (SomeSQLException (..),
                                                  throwSQLException)
import           HStream.SQL.Internal.Codegen    (binOpOnValue, compareValue,
                                                  composeColName, diffTimeToMs,
                                                  genJoiner,
                                                  genRandomSinkStream,
                                                  getFieldByName,
                                                  unaryOpOnValue)
import           HStream.SQL.Parse               (parseAndRefine)
import           HStream.Utils                   (genUnique)

import           DiffFlow.Graph
import           DiffFlow.Types
--------------------------------------------------------------------------------

type SerMat  = Object
type SerPipe = BL.ByteString

type StreamName     = HCT.StreamName
type ViewName = T.Text
type ConnectorName  = T.Text
type CheckIfExist  = Bool
type ViewSchema = [String]

data ShowObject = SStreams | SQueries | SConnectors | SViews
data DropObject = DStream Text | DView Text | DConnector Text
data TerminationSelection = AllQueries | OneQuery CB.CBytes | ManyQueries [CB.CBytes]
data InsertType = JsonFormat | RawFormat
data StartObject = StartObjectConnector Text
data StopObject = StopObjectConnector Text

data ConnectorConfig
  = ClickhouseConnector Clickhouse.ConnParams
  | MySqlConnector T.Text MySQL.ConnectInfo
  deriving Show

data HStreamPlan
  = SelectPlan          Text [(Node,StreamName)] (Node,StreamName) (Maybe RWindow) GraphBuilder
  | CreateBySelectPlan  Text [(Node,StreamName)] (Node,StreamName) (Maybe RWindow) GraphBuilder Int
  | CreateViewPlan      Text ViewSchema [(Node,StreamName)] (Node,StreamName) (Maybe RWindow) GraphBuilder (MVar (DataChangeBatch HCT.Timestamp))
  | CreatePlan          StreamName Int
  | CreateConnectorPlan ConnectorType ConnectorName Bool (HM.HashMap Text Value)
  | InsertPlan          StreamName InsertType ByteString
  | DropPlan            CheckIfExist DropObject
  | ShowPlan            ShowObject
  | TerminatePlan       TerminationSelection
  | SelectViewPlan      RSelectView
  | ExplainPlan         Text
  | StartPlan StartObject
  | StopPlan StopObject

--------------------------------------------------------------------------------

streamCodegen :: HasCallStack => Text -> IO HStreamPlan
streamCodegen input = parseAndRefine input >>= hstreamCodegen

hstreamCodegen :: HasCallStack => RSQL -> IO HStreamPlan
hstreamCodegen = \case
  RQSelect select -> do
    tName <- genTaskName
    (builder, inNodesWithStreams, outNodeWithStream, window) <- genGraphBuilderWithOutput Nothing select
    return $ SelectPlan tName inNodesWithStreams outNodeWithStream window builder
  RQCreate (RCreateAs stream select rOptions) -> do
    tName <- genTaskName
    (builder, inNodesWithStreams, outNodeWithStream, window) <- genGraphBuilderWithOutput (Just stream) select
    return $ CreateBySelectPlan tName inNodesWithStreams outNodeWithStream window builder (rRepFactor rOptions)
  RQCreate (RCreateView view select@(RSelect sel _ _ _ _)) -> do
    tName <- genTaskName
    (builder, inNodesWithStreams, outNodeWithStream, window) <- genGraphBuilderWithOutput (Just view) select
    accumulation <- newMVar emptyDataChangeBatch
    let schema = case sel of
          RSelAsterisk -> throwSQLException CodegenException Nothing "Impossible happened"
          RSelList fields -> map snd fields
    return $ CreateViewPlan tName schema inNodesWithStreams outNodeWithStream window builder accumulation
  RQCreate (RCreate stream rOptions) -> return $ CreatePlan stream (rRepFactor rOptions)
  RQCreate (RCreateConnector cType cName ifNotExist (RConnectorOptions cOptions)) ->
    return $ CreateConnectorPlan cType cName ifNotExist cOptions
  RQInsert (RInsert stream tuples)   -> return $ InsertPlan stream JsonFormat (BL.toStrict . PB.toLazyByteString . jsonObjectToStruct . HM.fromList $ second constantToValue <$> tuples)
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
  RQTerminate (RTerminateQuery qid)  -> return $ TerminatePlan (OneQuery $ CB.pack qid)
  RQTerminate RTerminateAll          -> return $ TerminatePlan AllQueries
  RQSelectView rSelectView           -> return $ SelectViewPlan rSelectView
  RQExplain rexplain                 -> return $ ExplainPlan rexplain
  RQStart (RStartConnector name)     -> return $ StartPlan (StartObjectConnector name)
  RQStop (RStopConnector name)       -> return $ StopPlan (StopObjectConnector name)

--------------------------------------------------------------------------------

genCreateConnectorPlan :: RCreate -> HStreamPlan
genCreateConnectorPlan (RCreateConnector cType cName ifNotExist (RConnectorOptions cOptions)) =
  CreateConnectorPlan cType cName ifNotExist cOptions
genCreateConnectorPlan _ =
  throwSQLException CodegenException Nothing "Implementation: Wrong function called"




extractInt :: String -> Maybe Constant -> Int
extractInt errPrefix = \case
  Just (ConstantInt s) -> s;
  _ -> throwSQLException CodegenException Nothing $ errPrefix <> "type should be integer."

----

type TaskName = Text
genSourceGraphBuilder :: HasCallStack => RFrom -> GraphBuilder -> IO (GraphBuilder, [(Node, StreamName)], Node)
genSourceGraphBuilder (RFrom tableRefs) baseBuilder = do
  let baseSubgraph = Subgraph 0
  go baseBuilder baseSubgraph tableRefs
  where
    go :: GraphBuilder -> Subgraph -> [RTableRef] -> IO (GraphBuilder, [(Node, StreamName)], Node)
    go startBuilder subgraph [] = throwSQLException CodegenException Nothing "Impossible happened"
    go startBuilder subgraph [ref] = case ref of
      RTableRefSimple s alias -> do
        let (builder, nodeIn) = addNode startBuilder subgraph InputSpec
        return (builder, [(nodeIn, s)], nodeIn)
      RTableRefSubquery select alias -> do
        (builder, inNodesWithStreams, outNodeWithStream, win) <- genGraphBuilder alias select
        return (builder, inNodesWithStreams, fst outNodeWithStream)
      RTableRefUnion ref1 ref2 alias -> do
        (builder_1, inNodesWithStreams_1, outNode_1) <- go startBuilder subgraph [ref1]
        (builder_2, inNodesWithStreams_2, outNode_2) <- go builder_1    subgraph [ref2]
        let (builder_3, nodeUnion) = addNode builder_2 subgraph (UnionSpec outNode_1 outNode_2)
        return (builder_3, inNodesWithStreams_1 <> inNodesWithStreams_2, nodeUnion)
    go startBuilder subgraph [ref1, ref2] = do
      (builder_1, inNodesWithStreams_1, outNode_1) <- go startBuilder subgraph [ref1]
      (builder_2, inNodesWithStreams_2, outNode_2) <- go builder_1    subgraph [ref2]
      let (builder_3, nodeIndex_1) = addNode builder_2 subgraph (IndexSpec outNode_1)
          (builder_4, nodeIndex_2) = addNode builder_3 subgraph (IndexSpec outNode_2)
      let keygen_1 = \o -> HM.fromList [("key", String "__constant_key__")]
          keygen_2 = \o -> HM.fromList [("key", String "__constant_key__")]
          joiner = Joiner (\o1 o2 -> o1 <> o2)
          (builder_5, nodeJoin) = addNode builder_4 subgraph (JoinSpec nodeIndex_1 nodeIndex_2 keygen_1 keygen_2 joiner)
      return (builder_5, inNodesWithStreams_1 <> inNodesWithStreams_2, nodeJoin)
    go startBuilder subgraph refs = do
      (builder_1, inNodesWithStreams_1, outNode_1) <- go startBuilder subgraph (L.take 2 refs)
      let (builder_2, outNode_2) = addNode builder_1 subgraph (IndexSpec outNode_1)
      foldM (\acc ref -> do
                let (curBuilder, curInNodesWithStreams, curOutNode) = acc
                (newBuilder_1, newInNodesWithStreams, newOutNode) <-
                  go curBuilder subgraph [ref]
                let (newBuilder_2, nodeIndex) = addNode newBuilder_1 subgraph (IndexSpec newOutNode)
                let keygen_1 = \o -> HM.fromList [("key", String "__constant_key__")]
                    keygen_2 = \o -> HM.fromList [("key", String "__constant_key__")]
                    joiner = Joiner (\o1 o2 -> o1 <> o2)
                    (newBuilder_3, nodeJoin) = addNode newBuilder_2 subgraph (JoinSpec curOutNode nodeIndex keygen_1 keygen_2 joiner)
                return (newBuilder_3, curInNodesWithStreams <> newInNodesWithStreams, nodeJoin)
            ) (builder_2, inNodesWithStreams_1, outNode_2) (L.drop 2 refs)


genTaskName :: IO Text
-- Please do not encode the this id to other forms,
-- since there is a minor issue related with parsing.
-- When parsing a identifier, the first letter is required to be a letter.
-- When parsing a string, quotes are required.
-- Currently there is no way to parse an id start with digit but contains letters/
genTaskName = pack . show <$> genUnique

----
constantToValue :: Constant -> Value
constantToValue (ConstantInt n)         = Number (scientific (toInteger n) 0)
constantToValue (ConstantNum n)         = Number (fromFloatDigits n)
constantToValue (ConstantString s)      = String (pack s)
constantToValue (ConstantNull)          = Null
constantToValue (ConstantBool b)        = Bool b
constantToValue (ConstantDate day)      = String (pack $ showGregorian day) -- FIXME: No suitable type in `Value`
constantToValue (ConstantTime diff)     = Number (scientific (diffTimeToPicoseconds diff) (-12)) -- FIXME: No suitable type in `Value`
constantToValue (ConstantInterval diff) = Number (scientific (diffTimeToPicoseconds diff) (-12)) -- FIXME: No suitable type in `Value`

-- May raise exceptions
genRExprValue :: HasCallStack => RValueExpr -> Object -> (Text, Value)
genRExprValue (RExprCol name stream' field) o = (pack name, getFieldByName o (composeColName stream' field))
genRExprValue (RExprConst name constant)          _ = (pack name, constantToValue constant)
genRExprValue (RExprBinOp name op expr1 expr2)    o =
  let (_,v1) = genRExprValue expr1 o
      (_,v2) = genRExprValue expr2 o
   in (pack name, binOpOnValue op v1 v2)
genRExprValue (RExprUnaryOp name op expr) o =
  let (_,v) = genRExprValue expr o
  in (pack name, unaryOpOnValue op v)
genRExprValue (RExprAggregate name agg) o =
  case agg of
    Nullary _     -> (pack name, Null) -- FIXME: should return nothing
    Unary _ rexpr -> genRExprValue rexpr o
    Binary _ rexpr1 rexpr2 -> genRExprValue rexpr1 o -- FIXME: different binary aggs?

genFilterR :: RWhere -> Object -> Bool
genFilterR RWhereEmpty _ = True
genFilterR (RWhere cond) recordValue =
  case cond of
    RCondOp op expr1 expr2 ->
      let (_,v1) = genRExprValue expr1 recordValue
          (_,v2) = genRExprValue expr2 recordValue
       in case op of
            RCompOpEQ  -> v1 == v2
            RCompOpNE  -> v1 /= v2
            RCompOpLT  -> case compareValue v1 v2 of
                            LT -> True
                            _  -> False
            RCompOpGT  -> case compareValue v1 v2 of
                            GT -> True
                            _  -> False
            RCompOpLEQ -> case compareValue v1 v2 of
                            GT -> False
                            _  -> True
            RCompOpGEQ -> case compareValue v1 v2 of
                            LT -> False
                            _  -> True
    RCondOr cond1 cond2    ->
      genFilterR (RWhere cond1) recordValue || genFilterR (RWhere cond2) recordValue
    RCondAnd cond1 cond2   ->
      genFilterR (RWhere cond1) recordValue && genFilterR (RWhere cond2) recordValue
    RCondNot cond1         ->
      not $ genFilterR (RWhere cond1) recordValue
    RCondBetween expr1 expr expr2 ->
      let (_,v1)    = genRExprValue expr1 recordValue
          (_,v)     = genRExprValue expr recordValue
          (_,v2)    = genRExprValue expr2 recordValue
          ordering1 = compareValue v1 v
          ordering2 = compareValue v v2
       in case ordering1 of
            GT -> False
            _  -> case ordering2 of
                    GT -> False
                    _  -> True

genFilterNode :: RWhere -> Node -> NodeSpec
genFilterNode whr prevNode = FilterSpec prevNode filter'
  where filter' = Filter (genFilterR whr)

----
genPreMapR :: RSel -> Object -> Object
genPreMapR RSelAsterisk recordValue = recordValue
genPreMapR (RSelList exprsWithAlias) recordValue = HM.fromList fields <> recordValue
  where
    fields = (\(e,alias) ->
                 case e of
                   Left expr -> let (_,v) = genRExprValue expr recordValue
                                in (pack alias,v)
                   Right agg -> let (_,v) = genRExprValue (RExprAggregate alias agg) recordValue
                                in (pack alias,v)
             ) <$> exprsWithAlias

genPreMapNode :: RSel -> Node -> NodeSpec
genPreMapNode sel prevNode = MapSpec prevNode mapper
  where mapper = Mapper (genPreMapR sel)

genPostMapR :: RSel -> Object -> Object
genPostMapR RSelAsterisk recordValue = recordValue
genPostMapR (RSelList exprsWithAlias) recordValue = HM.fromList fields
  where
    fields = (\(_,alias) ->
                (pack alias, getFieldByName recordValue (pack alias))
             ) <$> exprsWithAlias

genPostMapNode :: RSel -> Node -> NodeSpec
genPostMapNode sel prevNode = MapSpec prevNode mapper
  where mapper = Mapper (genPostMapR sel)

----
data AggregateComponents = AggregateComponents
  { aggregateInit :: Object
  , aggregateF    :: Object -> Object -> Object
  }

genAggregateComponents :: HasCallStack => RSel -> AggregateComponents
genAggregateComponents RSelAsterisk =
  throwSQLException CodegenException Nothing "SELECT * does not support GROUP BY clause"
genAggregateComponents (RSelList dcols) =
  fuseAggregateComponents $ genAggregateComponentsFromDerivedCol <$> dcols

genAggregateComponentsFromDerivedCol :: HasCallStack
                       => (Either RValueExpr Aggregate, FieldAlias)
                       -> AggregateComponents
genAggregateComponentsFromDerivedCol (Right agg, alias) =
  case agg of
    Nullary AggCountAll ->
      AggregateComponents
      { aggregateInit = HM.singleton (pack alias) (Number 0)
      , aggregateF    = \o _ -> HM.update (\(Number n) -> Just (Number $ n+1)) (pack alias) o
      }
    Unary AggCount (RExprCol _ stream' field) ->
      AggregateComponents
      { aggregateInit = HM.singleton (pack alias) (Number 0)
      , aggregateF = \o recordValue ->
          case HM.lookup (composeColName stream' field) recordValue of
            Nothing -> o
            Just _  -> HM.update (\(Number n) -> Just (Number $ n+1)) (pack alias) o
      }
    Unary AggSum (RExprCol _ stream' field)   ->
      AggregateComponents
      { aggregateInit = HM.singleton (pack alias) (Number 0)
      , aggregateF = \o recordValue ->
          case HM.lookup (composeColName stream' field) recordValue of
            Nothing         -> o
            Just (Number x) -> HM.update (\(Number n) -> Just (Number $ n+x)) (pack alias) o
            _               ->
              throwSQLException CodegenException Nothing "Only columns with Int or Number type can use SUM function"
      }
    Unary AggMax (RExprCol _ stream' field)   ->
      AggregateComponents
      { aggregateInit = HM.singleton (pack alias) (Number $ scientific (toInteger (minBound :: Int)) 0)
      , aggregateF = \o recordValue ->
          case HM.lookup (composeColName stream' field) recordValue of
            Nothing         -> o
            Just (Number x) -> HM.update (\(Number n) -> Just (Number $ max n x)) (pack alias) o
            _               ->
              throwSQLException CodegenException Nothing "Only columns with Int or Number type can use MAX function"
      }
    Unary AggMin (RExprCol _ stream' field)   ->
      AggregateComponents
      { aggregateInit = HM.singleton (pack alias) (Number $ scientific (toInteger (maxBound :: Int)) 0)
      , aggregateF = \o recordValue ->
          case HM.lookup (composeColName stream' field) recordValue of
            Nothing         -> o
            Just (Number x) -> HM.update (\(Number n) -> Just (Number $ min n x)) (pack alias) o
            _               ->
              throwSQLException CodegenException Nothing "Only columns with Int or Number type can use MIN function"
      }
    _ -> throwSQLException CodegenException Nothing ("Unsupported aggregate function: " <> show agg)
genAggregateComponentsFromDerivedCol (Left rexpr, alias) =
  AggregateComponents
  { aggregateInit = HM.singleton (pack alias) (Number 0)
  , aggregateF = \old recordValue -> HM.adjust (\_ -> updateV recordValue rexpr) (pack alias) old
  }
  where updateV recordValue rexpr = let (_,v) = genRExprValue rexpr recordValue in v

fuseAggregateComponents :: [AggregateComponents] -> AggregateComponents
fuseAggregateComponents components =
  AggregateComponents
  { aggregateInit = HM.unions (aggregateInit <$> components)
  , aggregateF = \old recordValue -> L.foldr (\f acc -> f acc recordValue) old (aggregateF <$> components)
  }

genGroupByKeygen :: HasCallStack
                 => RGroupBy
                 -> IO (Object -> Object)
genGroupByKeygen RGroupByEmpty =
  throwSQLException CodegenException Nothing "Impossible happened"
genGroupByKeygen (RGroupBy stream' field win') = do
  return $ \value ->
    let keyName = composeColName stream' field
        key = getFieldByName value keyName
     in HM.fromList [(keyName, key)]

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
