{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE StrictData        #-}

module HStream.SQL.Codegen where

import           Data.Aeson
import qualified Data.HashMap.Strict                             as HM
import           Data.Scientific                                 (fromFloatDigits,
                                                                  scientific)
import           Data.Text                                       (pack)
import           Data.Time                                       (diffTimeToPicoseconds,
                                                                  showGregorian)
import           HStream.Processing.Processor                    (Record (..))
import           HStream.Processing.Processor.Internal           (Task)
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
import           HStream.Processing.Topic                        (TopicName)
import           HStream.SQL.AST
import           HStream.SQL.Codegen.Boilerplate                 (objectSerde)
import           HStream.SQL.Codegen.Utils                       (compareValue,
                                                                  composeColName,
                                                                  diffTimeToMs,
                                                                  genJoiner,
                                                                  genRandomSinkTopic,
                                                                  getFieldByName,
                                                                  opOnValue)
import           HStream.SQL.Parse                               (pSQL,
                                                                  preprocess,
                                                                  tokens)
import           HStream.SQL.Validate                            (Validate (..))
import           RIO
import qualified RIO.ByteString.Lazy                             as BL

--------------------------------------------------------------------------------
type SourceTopic = [TopicName]
type SinkTopic   = TopicName

data ExecutionPlan = SelectPlan         SourceTopic SinkTopic Task
                   | CreatePlan         TopicName
                   | CreateBySelectPlan SourceTopic SinkTopic Task
                   | InsertPlan         TopicName BL.ByteString

--------------------------------------------------------------------------------
streamCodegen :: Text -> IO ExecutionPlan
streamCodegen input = do
  let sql' = pSQL (tokens (preprocess input)) >>= validate
  case sql' of
    Left err  -> error err
    Right sql -> do
      let rsql = refine sql
      case rsql of
        RQSelect select                     -> do
          (builder, source, sink) <- genStreamBuilderWithTopic "demo" Nothing select
          return $ SelectPlan source sink (HS.build builder)
        RQCreate (RCreate topic _)          -> return . CreatePlan $ topic
        RQCreate (RCreateAs topic select _) -> do
          (builder, source, sink) <- genStreamBuilderWithTopic "demo" (Just topic) select
          return $ CreateBySelectPlan source sink (HS.build builder)
        RQInsert (RInsert topic tuples)     -> do
          let object = HM.fromList $ (\(f,c) -> (f,constantToValue c)) <$> tuples
          return $ InsertPlan topic (encode object)

--------------------------------------------------------------------------------
type SourceConfigType = HS.StreamSourceConfig Object Object
genStreamSourceConfig :: RFrom -> (SourceConfigType, Maybe SourceConfigType)
genStreamSourceConfig frm =
  let boilerplate = HS.StreamSourceConfig "" objectSerde objectSerde
   in case frm of
        RFromSingle s -> (boilerplate {sscTopicName = s}, Nothing)
        RFromJoin (s1,_) (s2,_) _ _ ->
          ( boilerplate {sscTopicName = s1}
          , Just $ boilerplate {sscTopicName = s2}
          )

defaultTimeWindowSize :: Int64
defaultTimeWindowSize = 3000

data SinkConfigType = SinkConfigType SinkTopic (HS.StreamSinkConfig Object Object)
                    | SinkConfigTypeWithWindow SinkTopic (HS.StreamSinkConfig (TimeWindowKey Object) Object)

genStreamSinkConfig :: Maybe TopicName -> RGroupBy -> IO SinkConfigType
genStreamSinkConfig sinkTopic' grp = do
  topic <- case sinkTopic' of
    Nothing        -> genRandomSinkTopic
    Just sinkTopic -> return sinkTopic
  case grp of
    RGroupByEmpty ->
      return $ SinkConfigType topic HS.StreamSinkConfig
      { sicTopicName  = topic
      , sicKeySerde   = objectSerde
      , sicValueSerde = objectSerde
      }
    _ ->
      return $ SinkConfigTypeWithWindow topic HS.StreamSinkConfig
      { sicTopicName = topic
      , sicKeySerde = timeWindowKeySerde objectSerde defaultTimeWindowSize
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
genStreamWithSourceTopic :: TaskName -> RFrom -> IO (Stream Object Object, SourceTopic)
genStreamWithSourceTopic taskName frm = do
  let (srcConfig1, srcConfig2') = genStreamSourceConfig frm
  baseStream <- HS.mkStreamBuilder taskName >>= HS.stream srcConfig1
  case frm of
    RFromSingle s                     -> return (baseStream, [sscTopicName srcConfig1])
    RFromJoin (s1,f1) (s2,f2) typ win ->
      case srcConfig2' of
        Nothing         -> error "Impossible happened"
        Just srcConfig2 ->
          case typ of
            RJoinInner -> do
              anotherStream <- HS.mkStreamBuilder "" >>= HS.stream srcConfig2
              streamJoined  <- genStreamJoinedConfig
              joinedStream  <- HS.joinStream anotherStream (genJoiner s1 s2)
                                 (genKeySelector f1) (genKeySelector f2)
                                 (genJoinWindows win) streamJoined
                                 baseStream
              return (joinedStream, [sscTopicName srcConfig1, sscTopicName srcConfig2])
            _          -> error "Impossible happened"

----
constantToValue :: Constant -> Value
constantToValue (ConstantInt n)         = Number (scientific (toInteger n) 0)
constantToValue (ConstantNum n)         = Number (fromFloatDigits n)
constantToValue (ConstantString s)      = String (pack s)
constantToValue (ConstantDate day)      = String (pack $ showGregorian day) -- FIXME: No suitable type in `Value`
constantToValue (ConstantTime diff)     = Number (scientific (diffTimeToPicoseconds diff) (-12)) -- FIXME: No suitable type in `Value`
constantToValue (ConstantInterval diff) = Number (scientific (diffTimeToPicoseconds diff) (-12)) -- FIXME: No suitable type in `Value`

-- May raise exceptions
genRExprValue :: RValueExpr -> Object -> (Text, Value)
genRExprValue (RExprCol name stream' field) o = (pack name, getFieldByName o (composeColName stream' field))
genRExprValue (RExprConst name constant)          _ = (pack name, constantToValue constant)
genRExprValue (RExprBinOp name op expr1 expr2)    o =
  let (_,v1) = genRExprValue expr1 o
      (_,v2) = genRExprValue expr2 o
   in (pack name, opOnValue op v1 v2)
genRExprValue (RExprAggregate _ _) _ = error "Impossible happened"

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
genMaterialized :: RGroupBy -> IO (HS.Materialized Object Object)
genMaterialized grp = do
  aggStore <- case grp of
    RGroupByEmpty     -> error "Impossible happened"
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

genAggregateComponents :: RSel -> AggregateComponents
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
    Unary AggSum (RExprCol _ stream' field) ->
      AggregateCompontnts
      { aggregateInit = HM.singleton (pack alias) (Number 0)
      , aggregateF = \o Record{..} ->
          case HM.lookup (composeColName stream' field) recordValue of
            Nothing -> o
            Just (Number x) -> HM.update (\(Number n) -> Just (Number $ n+x)) (pack alias) o
            _ -> error "Only columns with Int or Number type can use SUM function"
      , aggregateMergeF = \_ o1 o2 -> let (Number n1) = (HM.!) o1 (pack alias)
                                          (Number n2) = (HM.!) o2 (pack alias)
                                       in HM.singleton (pack alias) (Number $ n1+n2)
      }
    Unary AggMax (RExprCol _ stream' field) ->
      AggregateCompontnts
      { aggregateInit = HM.singleton (pack alias) (Number $ scientific (toInteger (minBound :: Int)) 0)
      , aggregateF = \o Record{..} ->
          case HM.lookup (composeColName stream' field) recordValue of
            Nothing -> o
            Just (Number x) -> HM.update (\(Number n) -> Just (Number $ max n x)) (pack alias) o
            _ -> error "Only columns with Int or Number type can use MAX function"
      , aggregateMergeF = \_ o1 o2 -> let (Number n1) = (HM.!) o1 (pack alias)
                                          (Number n2) = (HM.!) o2 (pack alias)
                                       in HM.singleton (pack alias) (Number $ max n1 n2)
      }
    Unary AggMin (RExprCol _ stream' field) ->
      AggregateCompontnts
      { aggregateInit = HM.singleton (pack alias) (Number $ scientific (toInteger (maxBound :: Int)) 0)
      , aggregateF = \o Record{..} ->
          case HM.lookup (composeColName stream' field) recordValue of
            Nothing -> o
            Just (Number x) -> HM.update (\(Number n) -> Just (Number $ min n x)) (pack alias) o
            _ -> error "Only columns with Int or Number type can use MIN function"
      , aggregateMergeF = \_ o1 o2 -> let (Number n1) = (HM.!) o1 (pack alias)
                                          (Number n2) = (HM.!) o2 (pack alias)
                                       in HM.singleton (pack alias) (Number $ min n1 n2)
      }
    _ -> error $ "Unsupported aggreaget function: " <> show agg

genGroupByNode :: RSelect -> Stream Object Object -> IO (Stream (TimeWindowKey Object) Object)
genGroupByNode (RSelect _ _ _ RGroupByEmpty _ ) s = error "Impossible happened"
genGroupByNode (RSelect sel _ _ grp@(RGroupBy stream' field win') _) s = do
  grped <- HS.groupBy
    (\record -> let col = composeColName stream' field
                 in HM.singleton col $ getFieldByName (recordValue record) col) s
  materialized <- genMaterialized grp
  let AggregateCompontnts{..} = genAggregateComponents sel
  table <- case win' of
    Nothing                       -> do
      timed <- HG.timeWindowedBy (mkTumblingWindow (maxBound :: Int64)) grped
      HTW.aggregate aggregateInit aggregateF materialized timed
    Just (RTumblingWindow diff)   -> do
      timed <- HG.timeWindowedBy (mkTumblingWindow (diffTimeToMs diff)) grped
      HTW.aggregate aggregateInit aggregateF materialized timed
    Just (RHoppingWIndow len hop) -> do
      timed <- HG.timeWindowedBy (mkHoppingWindow (diffTimeToMs len) (diffTimeToMs hop)) grped
      HTW.aggregate aggregateInit aggregateF materialized timed
    Just (RSessionWindow diff)    -> do
      timed <- HG.sessionWindowedBy (mkSessionWindows (diffTimeToMs diff)) grped
      HSW.aggregate aggregateInit aggregateF aggregateMergeF materialized timed
  HT.toStream table

----
genFilterRFromHaving :: RHaving -> Record Object Object -> Bool
genFilterRFromHaving RHavingEmpty   = const True
genFilterRFromHaving (RHaving cond) = genFilterR (RWhere cond)

genFilteRNodeFromHaving :: RHaving -> Stream Object Object -> IO (Stream Object Object)
genFilteRNodeFromHaving = HS.filter . genFilterRFromHaving

----
genStreamBuilderWithTopic :: TaskName -> Maybe TopicName -> RSelect -> IO (StreamBuilder, SourceTopic, SinkTopic)
genStreamBuilderWithTopic taskName sinkTopic' select@(RSelect sel frm whr grp hav) = do
  streamSinkConfig <- genStreamSinkConfig sinkTopic' grp
  (s0, source)     <- genStreamWithSourceTopic taskName frm
  s1               <- genFilterNode whr s0
                      >>= genMapNode sel
  case grp of
    RGroupByEmpty -> do
      s2 <- genFilteRNodeFromHaving hav s1
      case streamSinkConfig of
        SinkConfigType sink sinkConfig -> do
          builder <- HS.to sinkConfig s2
          return (builder, source, sink)
        _                         -> error "Impossible happened"
    _ -> do
      s2 <- genGroupByNode select s1
--              >>= genFilteRNodeFromHaving hav
-- WARNING: Having does not work with TimeWindow
      case streamSinkConfig of
        SinkConfigTypeWithWindow sink sinkConfig -> do
          builder <- HS.to sinkConfig s2
          return (builder, source, sink)
        _                                   -> error "Impossible happened"
