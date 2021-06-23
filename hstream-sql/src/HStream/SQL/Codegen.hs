{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE StrictData        #-}

module HStream.SQL.Codegen where

import           Data.Aeson
import qualified Data.ByteString.Lazy                            as BSL
import qualified Data.HashMap.Strict                             as HM
import           Data.Scientific                                 (fromFloatDigits,
                                                                  scientific)
import           Data.Text                                       (pack)
import           Data.Text.Encoding                              (decodeUtf8)
import           Data.Time                                       (diffTimeToPicoseconds,
                                                                  showGregorian)
import           HStream.Processing.Processor                    (Record (..),
                                                                  TaskBuilder)
import           HStream.Processing.Store                        (mkInMemoryStateKVStore,
                                                                  mkInMemoryStateSessionStore,
                                                                  mkInMemoryStateTimestampedKVStore)
import           HStream.Processing.Stream                       (Materialized (..),
                                                                  Stream,
                                                                  StreamBuilder,
                                                                  StreamJoined (..),
                                                                  StreamSinkConfig (..),
                                                                  StreamSourceConfig (..))
import qualified HStream.Processing.Stream                       as HS
import qualified HStream.Processing.Stream.GroupedStream         as HG
import           HStream.Processing.Stream.JoinWindows           (JoinWindows (..))
import qualified HStream.Processing.Stream.SessionWindowedStream as HSW
import           HStream.Processing.Stream.SessionWindows        (mkSessionWindows)
import qualified HStream.Processing.Stream.TimeWindowedStream    as HTW
import           HStream.Processing.Stream.TimeWindows           (TimeWindowKey,
                                                                  mkHoppingWindow,
                                                                  mkTumblingWindow,
                                                                  timeWindowKeySerde)
import qualified HStream.Processing.Table                        as HT
import qualified HStream.Processing.Type                         as HPT
import           HStream.SQL.AST                                 hiding
                                                                 (StreamName)
import           HStream.SQL.Codegen.Boilerplate                 (objectSerde)
import           HStream.SQL.Codegen.Utils                       (binOpOnValue,
                                                                  compareValue,
                                                                  composeColName,
                                                                  diffTimeToMs,
                                                                  genJoiner,
                                                                  genRandomSinkStream,
                                                                  getFieldByName,
                                                                  unaryOpOnValue)
import           HStream.SQL.Exception                           (SomeSQLException (..),
                                                                  throwSQLException)
import           HStream.SQL.Parse
import           Numeric                                         (showHex)
import           RIO
import qualified RIO.ByteString.Lazy                             as BL
import qualified RIO.Text                                        as T
import qualified Z.Data.CBytes                                   as CB
import           Z.IO.Time

--------------------------------------------------------------------------------

type StreamName     = HPT.StreamName
type ConnectorName  = T.Text
type SourceStream   = [StreamName]
type SinkStream     = StreamName
type CheckIfExist  = Bool

data ShowObject = SStreams | SQueries | SConnectors | SViews
data DropObject = DStream Text | DView Text
data TerminationSelection = AllQuery | OneQuery CB.CBytes

data ExecutionPlan
  = SelectPlan          SourceStream SinkStream TaskBuilder
  | CreatePlan          StreamName Int
  | CreateConnectorPlan ConnectorName RConnectorOptions
  | CreateBySelectPlan  SourceStream SinkStream TaskBuilder Int
  | InsertPlan          StreamName BL.ByteString
  | DropPlan            CheckIfExist DropObject
  | ShowPlan            ShowObject
  | TerminatePlan       TerminationSelection

--------------------------------------------------------------------------------

streamCodegen :: HasCallStack => Text -> IO ExecutionPlan
streamCodegen input = do
  rsql <- parseAndRefine input
  case rsql of
    RQSelect select                     -> do
      tName <- genTaskName
      (builder, source, sink) <- genStreamBuilderWithStream tName Nothing select
      return $ SelectPlan source sink (HS.build builder)
    RQCreate (RCreate stream rOptions)   -> return $ CreatePlan stream (rRepFactor rOptions)
    RQCreate (RCreateAs stream select rOptions) -> do
      tName <- genTaskName
      (builder, source, sink) <- genStreamBuilderWithStream tName (Just stream) select
      return $ CreateBySelectPlan source sink (HS.build builder) (rRepFactor rOptions)
    RQCreate x@(RCreateConnector s ifNotExist cOptions) -> return $ CreateConnectorPlan s cOptions
    RQCreate (RCreateView _ _ ) -> error "not supported"
    RQInsert (RInsert stream tuples)     -> do
      let object_ = HM.fromList $ (\(f,c) -> (f,constantToValue c)) <$> tuples
      return $ InsertPlan stream (encode object_)
    RQInsert (RInsertBinary stream bs)   -> do
      let k = "unknown_binary_data" :: Text
          v = String (decodeUtf8 bs)
      let object_ = HM.fromList [(k,v)]
      return $ InsertPlan stream (encode object_)
    RQInsert (RInsertJSON stream bs) -> do
      return $ InsertPlan stream (BSL.fromStrict bs)
    RQShow (RShow RShowStreams) -> return $ ShowPlan SStreams
    RQShow (RShow RShowQueries) -> return $ ShowPlan SQueries
    RQShow (RShow RShowConnectors) -> return $ ShowPlan SConnectors
    RQShow (RShow RShowViews) -> error "not supported" -- return $ ShowPlan SViews
    RQDrop (RDrop RDropStream x) -> return $ DropPlan False (DStream x)
    RQDrop (RDrop RDropView x) -> error "not supported" -- return $ DropPlan False (DView x)
    RQDrop (RDropIf RDropStream x) -> return $ DropPlan True (DStream x)
    RQDrop (RDropIf RDropView x) -> error "not supported" -- return $ DropPlan True (DView x)
    RQTerminate (RTerminateQuery qid) -> return $ TerminatePlan (OneQuery $ CB.pack qid)
    RQTerminate RTerminateAll         -> return $ TerminatePlan AllQuery

data NotSupported = NotSupported deriving Show
instance Exception NotSupported

--------------------------------------------------------------------------------

type SourceConfigType = HS.StreamSourceConfig Object Object
genStreamSourceConfig :: RFrom -> (SourceConfigType, Maybe SourceConfigType)
genStreamSourceConfig frm =
  let boilerplate = HS.StreamSourceConfig "" objectSerde objectSerde
   in case frm of
        RFromSingle s -> (boilerplate {sscStreamName = s}, Nothing)
        RFromJoin (s1,_) (s2,_) _ _ ->
          ( boilerplate {sscStreamName = s1}
          , Just $ boilerplate {sscStreamName = s2}
          )

defaultTimeWindowSize :: Int64
defaultTimeWindowSize = 3000

data SinkConfigType = SinkConfigType SinkStream (HS.StreamSinkConfig Object Object)
                    | SinkConfigTypeWithWindow SinkStream (HS.StreamSinkConfig (TimeWindowKey Object) Object)

genStreamSinkConfig :: Maybe StreamName -> RGroupBy -> IO SinkConfigType
genStreamSinkConfig sinkStream' grp = do
  stream <- case sinkStream' of
    Nothing         -> genRandomSinkStream
    Just sinkStream -> return sinkStream
  case grp of
    RGroupBy _ _ (Just _) ->
      return $ SinkConfigTypeWithWindow stream HS.StreamSinkConfig
      { sicStreamName = stream
      , sicKeySerde = timeWindowKeySerde objectSerde defaultTimeWindowSize
      , sicValueSerde = objectSerde
      }
    _ ->
      return $ SinkConfigType stream HS.StreamSinkConfig
      { sicStreamName  = stream
      , sicKeySerde   = objectSerde
      , sicValueSerde = objectSerde
      }

genStreamJoinedConfig :: IO (HS.StreamJoined Object Object Object Object)
genStreamJoinedConfig = do
  store1 <- mkInMemoryStateTimestampedKVStore
  store2 <- mkInMemoryStateTimestampedKVStore
  return HS.StreamJoined
    { sjK1Serde    = objectSerde
    , sjV1Serde    = objectSerde
    , sjK2Serde    = objectSerde
    , sjV2Serde    = objectSerde
    , sjThisStore  = store1
    , sjOtherStore = store2
    }

genJoinWindows :: RJoinWindow -> JoinWindows
genJoinWindows diffTime =
  let defaultGraceMs = 3600 * 1000
      windowWidth = diffTimeToMs  diffTime
   in JoinWindows
      { jwBeforeMs = windowWidth
      , jwAfterMs  = windowWidth
      , jwGraceMs  = defaultGraceMs
      }

genKeySelector :: FieldName -> Record Object Object -> Object
genKeySelector field Record{..} =
  HM.singleton "SelectedKey" $ (HM.!) recordValue field

type TaskName = Text
genStreamWithSourceStream :: HasCallStack => TaskName -> RFrom -> IO (Stream Object Object, SourceStream)
genStreamWithSourceStream taskName frm = do
  let (srcConfig1, srcConfig2') = genStreamSourceConfig frm
  baseStream <- HS.mkStreamBuilder taskName >>= HS.stream srcConfig1
  case frm of
    RFromSingle _                     -> return (baseStream, [sscStreamName srcConfig1])
    RFromJoin (s1,f1) (s2,f2) typ win ->
      case srcConfig2' of
        Nothing         ->
          throwSQLException CodegenException Nothing "Impossible happened"
        Just srcConfig2 ->
          case typ of
            RJoinInner -> do
              anotherStream <- HS.mkStreamBuilder "" >>= HS.stream srcConfig2
              streamJoined  <- genStreamJoinedConfig
              joinedStream  <- HS.joinStream anotherStream (genJoiner s1 s2)
                                 (genKeySelector f1) (genKeySelector f2)
                                 (genJoinWindows win) streamJoined
                                 baseStream
              return (joinedStream, [sscStreamName srcConfig1, sscStreamName srcConfig2])
            _          ->
              throwSQLException CodegenException Nothing "Impossible happened"

genTaskName :: IO Text
genTaskName = do
  MkSystemTime time _ <- getSystemTime'
  return $ pack $ showHex time ""
----
constantToValue :: Constant -> Value
constantToValue (ConstantInt n)         = Number (scientific (toInteger n) 0)
constantToValue (ConstantNum n)         = Number (fromFloatDigits n)
constantToValue (ConstantString s)      = String (pack s)
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
genRExprValue (RExprAggregate _ _) _ =
  throwSQLException CodegenException Nothing "Impossible happened"

genFilterR :: RWhere -> Record Object Object -> Bool
genFilterR RWhereEmpty _ = True
genFilterR (RWhere cond) record@Record{..} =
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
      genFilterR (RWhere cond1) record || genFilterR (RWhere cond2) record
    RCondAnd cond1 cond2   ->
      genFilterR (RWhere cond1) record && genFilterR (RWhere cond2) record
    RCondNot cond1         ->
      not $ genFilterR (RWhere cond1) record
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

genFilterNode :: RWhere -> Stream Object Object -> IO (Stream Object Object)
genFilterNode = HS.filter . genFilterR

----
genMapR :: RSel -> Record Object Object -> Record Object Object
genMapR RSelAsterisk record = record
genMapR (RSelList exprsWithAlias) record@Record{..} =
  let newValue = HM.fromList $
        (\(expr,alias) -> let (_,v) = genRExprValue expr recordValue in (pack alias,v)) <$> exprsWithAlias
   in record { recordValue = newValue }
genMapR (RSelAggregate _ _) record = record

genMapNode :: RSel -> Stream Object Object -> IO (Stream Object Object)
genMapNode = HS.map . genMapR

----
genMaterialized :: HasCallStack => RGroupBy -> IO (HS.Materialized Object Object)
genMaterialized grp = do
  aggStore <- case grp of
    RGroupByEmpty     ->
      throwSQLException CodegenException Nothing "Impossible happened"
    RGroupBy _ _ win' ->
      case win' of
        Just (RSessionWindow _) -> mkInMemoryStateSessionStore
        _                       -> mkInMemoryStateKVStore
  return $ HS.Materialized
           { mKeySerde   = objectSerde
           , mValueSerde = objectSerde
           , mStateStore = aggStore
           }

data AggregateComponents = AggregateCompontnts
  { aggregateInit   :: Object
  , aggregateF      :: Object -> Record Object Object -> Object
  , aggregateMergeF :: Object -> Object -> Object -> Object
  }

genAggregateComponents :: HasCallStack => RSel -> AggregateComponents
genAggregateComponents (RSelAggregate agg alias) =
  case agg of
    Nullary AggCountAll ->
      AggregateCompontnts
      { aggregateInit = HM.singleton (pack alias) (Number 0)
      , aggregateF    = \o _ -> HM.update (\(Number n) -> Just (Number $ n+1)) (pack alias) o
      , aggregateMergeF = \_ o1 o2 -> let (Number n1) = (HM.!) o1 (pack alias)
                                          (Number n2) = (HM.!) o2 (pack alias)
                                       in HM.singleton (pack alias) (Number $ n1+n2)
      }
    Unary AggCount (RExprCol _ stream' field) ->
      AggregateCompontnts
      { aggregateInit = HM.singleton (pack alias) (Number 0)
      , aggregateF = \o Record{..} ->
          case HM.lookup (composeColName stream' field) recordValue of
            Nothing -> o
            Just _  -> HM.update (\(Number n) -> Just (Number $ n+1)) (pack alias) o
      , aggregateMergeF = \_ o1 o2 -> let (Number n1) = (HM.!) o1 (pack alias)
                                          (Number n2) = (HM.!) o2 (pack alias)
                                       in HM.singleton (pack alias) (Number $ n1+n2)
      }
    Unary AggSum (RExprCol _ stream' field)   ->
      AggregateCompontnts
      { aggregateInit = HM.singleton (pack alias) (Number 0)
      , aggregateF = \o Record{..} ->
          case HM.lookup (composeColName stream' field) recordValue of
            Nothing         -> o
            Just (Number x) -> HM.update (\(Number n) -> Just (Number $ n+x)) (pack alias) o
            _               ->
              throwSQLException CodegenException Nothing "Only columns with Int or Number type can use SUM function"
      , aggregateMergeF = \_ o1 o2 -> let (Number n1) = (HM.!) o1 (pack alias)
                                          (Number n2) = (HM.!) o2 (pack alias)
                                       in HM.singleton (pack alias) (Number $ n1+n2)
      }
    Unary AggMax (RExprCol _ stream' field)   ->
      AggregateCompontnts
      { aggregateInit = HM.singleton (pack alias) (Number $ scientific (toInteger (minBound :: Int)) 0)
      , aggregateF = \o Record{..} ->
          case HM.lookup (composeColName stream' field) recordValue of
            Nothing         -> o
            Just (Number x) -> HM.update (\(Number n) -> Just (Number $ max n x)) (pack alias) o
            _               ->
              throwSQLException CodegenException Nothing "Only columns with Int or Number type can use MAX function"
      , aggregateMergeF = \_ o1 o2 -> let (Number n1) = (HM.!) o1 (pack alias)
                                          (Number n2) = (HM.!) o2 (pack alias)
                                       in HM.singleton (pack alias) (Number $ max n1 n2)
      }
    Unary AggMin (RExprCol _ stream' field)   ->
      AggregateCompontnts
      { aggregateInit = HM.singleton (pack alias) (Number $ scientific (toInteger (maxBound :: Int)) 0)
      , aggregateF = \o Record{..} ->
          case HM.lookup (composeColName stream' field) recordValue of
            Nothing         -> o
            Just (Number x) -> HM.update (\(Number n) -> Just (Number $ min n x)) (pack alias) o
            _               ->
              throwSQLException CodegenException Nothing "Only columns with Int or Number type can use MIN function"
      , aggregateMergeF = \_ o1 o2 -> let (Number n1) = (HM.!) o1 (pack alias)
                                          (Number n2) = (HM.!) o2 (pack alias)
                                       in HM.singleton (pack alias) (Number $ min n1 n2)
      }
    _                                         ->
      throwSQLException CodegenException Nothing ("Unsupported aggregate function: " <> show agg)

genGroupByNode :: RSelect
               -> Stream Object Object
               -> IO (Either (Stream Object Object) (Stream (TimeWindowKey Object) Object))
genGroupByNode (RSelect _ _ _ RGroupByEmpty _ ) s =
  throwSQLException CodegenException Nothing "Impossible happened"
genGroupByNode (RSelect sel _ _ grp@(RGroupBy stream' field win') _) s = do
  grped <- HS.groupBy
    (\record -> let col = composeColName stream' field
                 in HM.singleton col $ getFieldByName (recordValue record) col) s
  materialized <- genMaterialized grp
  let AggregateCompontnts{..} = genAggregateComponents sel
  case win' of
    Nothing                       -> do
      table <- HG.aggregate aggregateInit aggregateF materialized grped
      Left <$> HT.toStream table
    Just (RTumblingWindow diff)   -> do
      timed <- HG.timeWindowedBy (mkTumblingWindow (diffTimeToMs diff)) grped
      table <- HTW.aggregate aggregateInit aggregateF materialized timed
      Right <$> HT.toStream table
    Just (RHoppingWIndow len hop) -> do
      timed <- HG.timeWindowedBy (mkHoppingWindow (diffTimeToMs len) (diffTimeToMs hop)) grped
      table <- HTW.aggregate aggregateInit aggregateF materialized timed
      Right <$> HT.toStream table
    Just (RSessionWindow diff)    -> do
      timed <- HG.sessionWindowedBy (mkSessionWindows (diffTimeToMs diff)) grped
      table <- HSW.aggregate aggregateInit aggregateF aggregateMergeF materialized timed
      Right <$> HT.toStream table

----
genFilterRFromHaving :: RHaving -> Record Object Object -> Bool
genFilterRFromHaving RHavingEmpty   = const True
genFilterRFromHaving (RHaving cond) = genFilterR (RWhere cond)

genFilteRNodeFromHaving :: RHaving -> Stream Object Object -> IO (Stream Object Object)
genFilteRNodeFromHaving = HS.filter . genFilterRFromHaving

----
genStreamBuilderWithStream :: HasCallStack => TaskName -> Maybe StreamName -> RSelect -> IO (StreamBuilder, SourceStream, SinkStream)
genStreamBuilderWithStream taskName sinkStream' select@(RSelect sel frm whr grp hav) = do
  streamSinkConfig <- genStreamSinkConfig sinkStream' grp
  (s0, source)     <- genStreamWithSourceStream taskName frm
  s1               <- genFilterNode whr s0
                      >>= genMapNode sel
  case grp of
    RGroupByEmpty -> do
      s2 <- genFilteRNodeFromHaving hav s1
      case streamSinkConfig of
        SinkConfigType sink sinkConfig -> do
          builder <- HS.to sinkConfig s2
          return (builder, source, sink)
        _                              ->
          throwSQLException CodegenException Nothing "Impossible happened"
    _ -> do
      s2 <- genGroupByNode select s1
      case streamSinkConfig of
        SinkConfigTypeWithWindow sink sinkConfig -> do
          case s2 of
            Right timeStream -> do
              builder <- HS.to sinkConfig timeStream
              return (builder, source, sink)
            Left _ -> throwSQLException CodegenException Nothing "Expected timeStream but got stream"
        SinkConfigType sink sinkConfig -> do
          case s2 of
            Left stream -> do
              s3 <- genFilteRNodeFromHaving hav stream
              builder <- HS.to sinkConfig s3
              return (builder, source, sink)
            Right _ -> throwSQLException CodegenException Nothing "Expected stream but got timeStream"
