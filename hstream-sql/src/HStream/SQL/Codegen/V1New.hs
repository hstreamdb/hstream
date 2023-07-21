{-# LANGUAGE BlockArguments        #-}
{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE KindSignatures        #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NoImplicitPrelude     #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE StrictData            #-}
{-# LANGUAGE TypeFamilies          #-}

module HStream.SQL.Codegen.V1New where

#ifdef HStreamEnableSchema
#ifndef HStreamUseV2Engine
import           Control.Monad.State
import           Data.Aeson                                      (Object,
                                                                  Value (Bool, Null, Number, String))
import qualified Data.Aeson                                      as Aeson
import           Data.Bifunctor
import qualified Data.ByteString.Char8                           as BSC
import           Data.Function
import           Data.Functor
import qualified Data.HashMap.Strict                             as HM
import qualified Data.IntMap                                     as IntMap
import qualified Data.List                                       as L
import           Data.Maybe
import           Data.Scientific                                 (fromFloatDigits,
                                                                  scientific)
import           Data.Text                                       (pack)
import qualified Data.Text                                       as T
import           Data.Text.Prettyprint.Doc                       as PP
import           Data.Text.Prettyprint.Doc.Render.Text           as PP
import           Data.Time                                       (diffTimeToPicoseconds,
                                                                  showGregorian)
import qualified Proto3.Suite                                    as PB
import           RIO
import qualified RIO.ByteString.Lazy                             as BL
import qualified Z.Data.CBytes                                   as CB

import           HStream.Base                                    (genUnique)
import           HStream.Processing.Encoding                     (Serde (..),
                                                                  Serializer (..))
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
import           HStream.Processing.Stream.SessionWindows        (mkSessionWindows,
                                                                  sessionWindowKeySerde)
import qualified HStream.Processing.Stream.TimeWindowedStream    as HTW
import           HStream.Processing.Stream.TimeWindows           (TimeWindow (..),
                                                                  TimeWindowKey (..),
                                                                  mkHoppingWindow,
                                                                  mkTumblingWindow,
                                                                  timeWindowKeySerde)
import qualified HStream.Processing.Table                        as HT
import qualified HStream.Processing.Type                         as HPT
import           HStream.SQL.Binder
import           HStream.SQL.Codegen.ColumnCatalogNew
import           HStream.SQL.Codegen.CommonNew
import           HStream.SQL.Codegen.Utils
import           HStream.SQL.Codegen.V1New.Boilerplate
import           HStream.SQL.Exception                           (SomeSQLException (..),
                                                                  throwSQLException)
import           HStream.SQL.Extra
import           HStream.SQL.ParseNew                            (parseAndBind)
import           HStream.SQL.PlannerNew
import qualified HStream.SQL.PlannerNew                          as Planner
import           HStream.SQL.PlannerNew.Extra
import           HStream.SQL.PlannerNew.Types
import qualified HStream.SQL.PlannerNew.Types                    as Planner
import           HStream.SQL.Rts
import           HStream.Utils                                   (jsonObjectToStruct,
                                                                  newRandomText)
import qualified HStream.Utils.Aeson                             as HsAeson

--------------------------------------------------------------------------------
type K = FlowObject
type V = FlowObject

type Ser  = BL.ByteString

type StreamName = T.Text
type ViewName = T.Text
type ConnectorName  = T.Text
type CheckIfExist  = Bool
type ViewSchema = [String]

data ShowObject = SStreams | SQueries | SConnectors | SViews
data DropObject = DStream Text | DView Text | DConnector Text | DQuery Text
data TerminateObject = TQuery Text
data InsertType = JsonFormat | RawFormat deriving (Show, Eq)
data PauseObject
  = PauseObjectConnector Text
  | PauseObjectQuery Text
data ResumeObject
  =  ResumeObjectConnector Text
  | ResumeObjectQuery Text

type Persist = ([HS.StreamJoined K V K V Ser], [HS.Materialized K V V])

data HStreamPlan
  = CreatePlan          StreamName Schema BoundStreamOptions
  | CreateConnectorPlan ConnectorType ConnectorName Text Bool (HM.HashMap Text Value)
  | InsertPlan          StreamName InsertType ByteString
  | DropPlan            CheckIfExist DropObject
  | ShowPlan            ShowObject
  | TerminatePlan       TerminateObject
  | ExplainPlan         Text
  | PausePlan           PauseObject
  | ResumePlan          ResumeObject
  | SelectPlan          [StreamName] StreamName TaskBuilder Persist
  | PushSelectPlan      [StreamName] StreamName TaskBuilder Persist
  | CreateBySelectPlan  [StreamName] StreamName TaskBuilder BoundStreamOptions Persist
  | CreateViewPlan      [StreamName] StreamName ViewName TaskBuilder Persist
  | InsertBySelectPlan  [StreamName] StreamName TaskBuilder Persist

--------------------------------------------------------------------------------
streamCodegen :: HasCallStack => Text -> (Text -> IO (Maybe Schema)) -> IO HStreamPlan
streamCodegen input getSchema = parseAndBind input getSchema >>= (flip hstreamCodegen) getSchema

hstreamCodegen :: HasCallStack => BoundSQL -> (Text -> IO (Maybe Schema)) -> IO HStreamPlan
hstreamCodegen bsql getSchema = case bsql of
  BoundQSelect select -> do
    tName <- genTaskName
    (builder, srcs, sink, persist) <- elabRSelect tName Nothing select getSchema
    return $ SelectPlan srcs sink (HS.build builder) persist
  BoundQPushSelect select -> do
    tName <- genTaskName
    (builder, srcs, sink, persist) <- elabRSelect tName Nothing select getSchema
    return $ PushSelectPlan srcs sink (HS.build builder) persist
  BoundQCreate (BoundCreateAs stream select rOptions) -> do
    tName <- genTaskName
    (builder, srcs, sink, persist) <- elabRSelect tName (Just stream) select getSchema
    return $ CreateBySelectPlan srcs sink (HS.build builder) rOptions persist
{-
  BoundQInsert (BoundInsertSel stream select) -> do
    tName <- genTaskName
    (builder, srcs, sink, persist) <- elabRSelect tName (Just stream) select getSchema
    pure $ InsertBySelectPlan srcs sink (HS.build builder) persist
-}
  BoundQCreate (BoundCreateView view select) -> do
    tName <- genTaskName
    (builder, srcs, sink, persist) <- elabRSelect tName (Just view) select getSchema
    return $ CreateViewPlan srcs sink view (HS.build builder) persist
  BoundQCreate (BoundCreate stream schema rOptions) -> return $ CreatePlan stream schema rOptions
  BoundQCreate (BoundCreateConnector cType cName cTarget ifNotExist (BoundConnectorOptions cOptions)) ->
    return $ CreateConnectorPlan cType cName cTarget ifNotExist cOptions
  BoundQInsert (BoundInsertKVs stream tuples)   -> do
    let jsonObj = HsAeson.fromList $
          bimap HsAeson.fromText (flowValueToJsonValue . constantToFlowValue) <$> tuples
    return $ InsertPlan stream JsonFormat (BL.toStrict . PB.toLazyByteString . jsonObjectToStruct $ jsonObj)
  BoundQInsert (BoundInsertRawOrJson stream bs payloadType) ->
    pure $ case payloadType of
      BoundInsertPayloadTypeRaw  -> InsertPlan stream RawFormat bs
      BoundInsertPayloadTypeJson -> InsertPlan stream JsonFormat $
        BL.toStrict . PB.toLazyByteString . jsonObjectToStruct . fromJust . Aeson.decode
          $ BL.fromStrict bs
  BoundQShow (BoundShow BoundShowStreams)        -> return $ ShowPlan SStreams
  BoundQShow (BoundShow BoundShowQueries)        -> return $ ShowPlan SQueries
  BoundQShow (BoundShow BoundShowConnectors)     -> return $ ShowPlan SConnectors
  BoundQShow (BoundShow BoundShowViews)          -> return $ ShowPlan SViews
  BoundQDrop (BoundDrop BoundDropConnector x)    -> return $ DropPlan False (DConnector x)
  BoundQDrop (BoundDrop BoundDropStream x)       -> return $ DropPlan False (DStream x)
  BoundQDrop (BoundDrop BoundDropView x)         -> return $ DropPlan False (DView x)
  BoundQDrop (BoundDrop BoundDropQuery x)        -> return $ DropPlan False (DQuery x)
  BoundQDrop (BoundDropIf BoundDropConnector x)  -> return $ DropPlan True (DConnector x)
  BoundQDrop (BoundDropIf BoundDropStream x)     -> return $ DropPlan True (DStream x)
  BoundQDrop (BoundDropIf BoundDropView x)       -> return $ DropPlan True (DView x)
  BoundQDrop (BoundDropIf BoundDropQuery x)      -> return $ DropPlan True (DQuery x)
  BoundQTerminate (BoundTerminateQuery qid)  -> return $ TerminatePlan (TQuery qid)
  BoundQExplain rselect                  -> do
    relationExpr <- planIO rselect getSchema
    return $ ExplainPlan (PP.renderStrict $ PP.layoutPretty PP.defaultLayoutOptions (PP.pretty relationExpr))
  BoundQPause  (BoundPauseConnector name)    -> return $ PausePlan (PauseObjectConnector name)
  BoundQPause  (BoundPauseQuery name)        -> return $ PausePlan (PauseObjectQuery name)
  BoundQResume (BoundResumeConnector name)   -> return $ ResumePlan (ResumeObjectConnector name)
  BoundQResume (BoundResumeQuery name)       -> return $ ResumePlan (ResumeObjectQuery name)
--------------------------------------------------------------------------------

genStreamJoinedConfig :: IO (HS.StreamJoined K V K V Ser)
genStreamJoinedConfig = do
  store1 <- mkInMemoryStateTimestampedKVStore
  store2 <- mkInMemoryStateTimestampedKVStore
  return HS.StreamJoined
    { sjK1Serde    = flowObjectSerde
    , sjV1Serde    = flowObjectSerde
    , sjK2Serde    = flowObjectSerde
    , sjV2Serde    = flowObjectSerde
    , sjThisStore  = store1
    , sjOtherStore = store2
    }

genMaterialized :: Maybe WindowType -> IO (HS.Materialized K V V)
genMaterialized win_m = do
  aggStore <- case win_m of
    Just (Session _) -> mkInMemoryStateSessionStore
    _                -> mkInMemoryStateKVStore
  return $ HS.Materialized
           { mKeySerde   = flowObjectFlowObjectSerde
           , mValueSerde = flowObjectFlowObjectSerde
           , mStateStore = aggStore
           }

data EStream where
  EStream1 :: Stream K V Ser -> EStream
  EStream2 :: Stream (TimeWindowKey K) V Ser -> EStream

type family BTK (b :: Bool) (k :: *) where
  BTK 'False k = k
  BTK 'True  k = TimeWindowKey k

data SK (b :: Bool) where
  SK  :: SK 'False
  SKT :: SK 'True

withEStreamM :: Monad m => EStream -> SK b -> (Stream K V Ser -> m (Stream (BTK b K) V Ser)) -> (Stream (TimeWindowKey K) V Ser -> m (Stream (BTK b (TimeWindowKey K)) V Ser)) -> m EStream
withEStreamM (EStream1 s) SK f1 f2 = do
  s' <- f1 s
  return $ EStream1 s'
withEStreamM (EStream1 s) SKT f1 f2 = do
  s' <- f1 s
  return $ EStream2 s'
withEStreamM (EStream2 ts) SK f1 f2 = do
  ts' <- f2 ts
  return $ EStream2 ts'
withEStreamM (EStream2 ts) SKT f1 f2 = throwSQLException CodegenException Nothing "Applying a time window to a time-windowed stream is not supported"

filterFilterR :: ScalarExpr -> Record k V -> Bool
filterFilterR scalar =
  \Record{..} ->
    case scalarExprToFun scalar recordValue of
      Left e  -> False -- FIXME: log error message
      Right v -> v == FlowBoolean True

projectMapR :: [(ColumnCatalog, ScalarExpr)] -> Record k V -> Record k V
projectMapR tups =
  \record@Record{..} ->
    let recordValue' =
          L.foldr (\(cata,scalar) acc ->
                     case scalarExprToFun scalar recordValue of
                       Left _  -> acc
                       Right v -> HM.insert cata v acc
                  ) HM.empty tups
     in record{ recordValue = recordValue' }

relationExprToGraph :: RelationExpr -> StreamBuilder -> IO (EStream, [StreamName], [StreamJoined K V K V Ser], [HS.Materialized K V V])
relationExprToGraph relation builder = case relation of
  StreamScan schema -> do
    let sourceConfig = HS.StreamSourceConfig
                     { sscStreamName = schemaOwner schema
                     , sscKeySerde   = flowObjectSerde
                     , sscValueSerde = flowObjectSerde
                     }
    -- FIXME: builder name
    s' <- HS.stream sourceConfig builder
    return (EStream1 s', [schemaOwner schema], [], [])
  LoopJoinOn schema r1 r2 expr typ t -> do
    let joiner = HM.union
        joinCond = \record1 record2 ->
          case scalarExprToFun expr (recordValue record1 <> recordValue record2) of
            Left e  -> False -- FIXME: log error message
            Right v -> v == FlowBoolean True
        newKeySelector = \_ _ -> HM.fromList [] -- Default key is empty. See HStream.Processing.Stream#joinStreamProcessor
        joinWindows = JoinWindows
                    { jwBeforeMs = t
                    , jwAfterMs  = t
                    , jwGraceMs  = 0
                    }
    (es1,srcs1,joins1,mats1) <- relationExprToGraph r1 builder
    (es2,srcs2,joins2,mats2) <- relationExprToGraph r2 builder
    -- FIXME: join timewindowed stream
    case (es1, es2) of
      (EStream1 s1, EStream1 s2) -> do
        streamJoined <- genStreamJoinedConfig
        s' <- HS.joinStream s2 joiner joinCond newKeySelector joinWindows streamJoined s1
        return (EStream1 s', srcs1++srcs2, streamJoined:joins1++joins2, mats1++mats2)
      _ -> throwSQLException CodegenException Nothing "Joining time-windowed and non-time-windowed streams is not supported"
  Planner.Filter schema r scalar -> do
    (es,srcs,joins,mats) <- relationExprToGraph r builder
    es' <- withEStreamM es SK (HS.filter $ filterFilterR scalar)
                              (HS.filter $ filterFilterR scalar)
    return (es',srcs,joins,mats)
  Project schema r scalars -> do
    (es,srcs,joins,mats) <- relationExprToGraph r builder
    let cataTups = IntMap.elems (schemaColumns schema) `zip` scalars
    es' <- withEStreamM es SK (HS.map $ projectMapR cataTups)
                              (HS.map $ projectMapR cataTups)
    return (es',srcs,joins,mats)
  Reduce schema r scalars aggs win_m -> do
    let keygen = \Record{..} ->
          L.foldr (\(cata,scalar) acc ->
                      case scalarExprToFun scalar recordValue of
                        Left _  -> HM.insert cata FlowNull acc
                        Right v -> HM.insert cata v acc
                  ) HM.empty (IntMap.elems (schemaColumns schema) `zip` scalars)
    let aggComp@AggregateComponent{..} =
          composeAggs (L.map (\(cata,agg) ->
                                 genAggregateComponent agg cata
                             ) ((L.drop (L.length scalars) (IntMap.elems (schemaColumns schema)))
                                 `zip` aggs)
                      )
    let aggregateR = \acc Record{..} ->
          case aggregateF acc recordValue of
            Left (e,v) -> v -- FIXME: log error message
            Right v    -> v
        aggregateMerge' = \k o1 o2 ->
          case aggregateMergeF k o1 o2 of
            Left (e,v) -> v
            Right v    -> v

    materialized  <- genMaterialized win_m

    (es,srcs,joins,mats) <- relationExprToGraph r builder
    case es of
      EStream2 _ -> throwSQLException CodegenException Nothing "Reducing a time-windowed stream is not supported"
      EStream1 s -> do
        groupedStream <- HS.groupBy keygen s
        case win_m of
          Nothing -> do
            s' <- HG.aggregate aggregateInit
                               aggregateR
                               HM.union
                               flowObjectSerde
                               flowObjectSerde
                               materialized
                               groupedStream
                  >>= HT.toStream
            return (EStream1 s', srcs, joins, materialized:mats)
          Just (Tumbling i) -> do
            s' <- HG.timeWindowedBy (mkTumblingWindow (calendarDiffTimeToMs i)) groupedStream
                  >>= HTW.aggregate aggregateInit
                                    aggregateR
                                    (\a k timeWindow ->
                                        let twFlowObject = runSer (serializer timeWindowFlowObjectSerde) timeWindow
                                         in HM.union (HM.union a k) twFlowObject
                                    )
                                    timeWindowFlowObjectSerde
                                    (timeWindowSerde $ calendarDiffTimeToMs i)
                                    flowObjectSerde
                                    materialized
                  >>= HT.toStream
            return (EStream2 s', srcs, joins, materialized:mats)
          Just (Hopping i1 i2) -> do
            s' <- HG.timeWindowedBy (mkHoppingWindow (calendarDiffTimeToMs i1) (calendarDiffTimeToMs i2)) groupedStream
                  >>= HTW.aggregate aggregateInit
                                    aggregateR
                                    (\a k timeWindow ->
                                        let twFlowObject = runSer (serializer timeWindowFlowObjectSerde) timeWindow
                                         in HM.union (HM.union a k) twFlowObject
                                    )
                                    timeWindowFlowObjectSerde
                                    (timeWindowSerde $ calendarDiffTimeToMs i1)
                                    flowObjectSerde
                                    materialized
                  >>= HT.toStream
            return (EStream2 s', srcs, joins, materialized:mats)
          Just (Session i) -> do
            s' <- HG.sessionWindowedBy (mkSessionWindows (calendarDiffTimeToMs i)) groupedStream
                  >>= HSW.aggregate aggregateInit
                                    aggregateR
                                    aggregateMerge'
                                    HM.union
                                    (timeWindowSerde $ calendarDiffTimeToMs i)
                                    flowObjectSerde
                                    materialized
                  >>= HT.toStream
            return (EStream2 s', srcs, joins, materialized:mats)
  Distinct schema r -> do
    throwSQLException CodegenException Nothing "Distinct is not supported"
  Union schema r1 r2 -> do
    throwSQLException CodegenException Nothing "Union is not supported"

data SinkConfigType = SinkConfigType StreamName (HS.StreamSinkConfig K V Ser)
                    | SinkConfigTypeWithWindow StreamName (HS.StreamSinkConfig (TimeWindowKey K) V Ser)

genStreamSinkConfig :: Maybe StreamName -> RelationExpr -> IO SinkConfigType
genStreamSinkConfig sinkStream' relationExpr = do
  stream <- maybe genRandomSinkStream return sinkStream'
  let (reduce_m,_) = scanRelationExpr (\expr -> case expr of
                                                  Reduce{} -> True
                                                  _        -> False
                                      ) Nothing relationExpr
  case reduce_m of
    Nothing -> return $ SinkConfigType stream HS.StreamSinkConfig
               { sicStreamName  = stream
               , sicKeySerde   = flowObjectSerde
               , sicValueSerde = flowObjectSerde
               }
    Just (Reduce _ _ _ _ win_m) -> case win_m of
      Nothing -> return $ SinkConfigType stream HS.StreamSinkConfig
                 { sicStreamName  = stream
                 , sicKeySerde   = flowObjectSerde
                 , sicValueSerde = flowObjectSerde
                 }
      Just (Tumbling i) -> return $ SinkConfigTypeWithWindow stream HS.StreamSinkConfig
                           { sicStreamName  = stream
                           , sicKeySerde   = timeWindowKeySerde flowObjectSerde (timeWindowSerde $ calendarDiffTimeToMs i) (calendarDiffTimeToMs i)
                           , sicValueSerde = flowObjectSerde
                           }
      Just (Hopping i _) -> return $ SinkConfigTypeWithWindow stream HS.StreamSinkConfig
                              { sicStreamName  = stream
                              , sicKeySerde   = timeWindowKeySerde flowObjectSerde (timeWindowSerde $ calendarDiffTimeToMs i) (calendarDiffTimeToMs i)
                              , sicValueSerde = flowObjectSerde
                              }
      Just (Session i) -> return $ SinkConfigTypeWithWindow stream HS.StreamSinkConfig
                          { sicStreamName = stream
                          , sicKeySerde = sessionWindowKeySerde flowObjectSerde (timeWindowSerde $ calendarDiffTimeToMs i)
                          , sicValueSerde = flowObjectSerde
                          }

genTaskName :: IO Text
-- Please do not encode the this id to other forms,
-- since there is a minor issue related with parsing.
-- When parsing a identifier, the first letter is required to be a letter.
-- When parsing a string, quotes are required.
-- Currently there is no way to parse an id start with digit but contains letters
genTaskName = pack . show <$> genUnique

----
elabRSelect :: Text
            -> Maybe StreamName
            -> BoundSelect
            -> (Text -> IO (Maybe Schema))
            -> IO (StreamBuilder, [StreamName], StreamName, Persist)
elabRSelect taskName sinkStream' select getSchema = do
  relation <- evalStateT (runReaderT (plan select) getSchema) defaultPlanContext
  elabRelationExpr taskName sinkStream' relation

elabRelationExpr :: Text
                 -> Maybe StreamName
                 -> RelationExpr
                 -> IO (StreamBuilder, [StreamName], StreamName, Persist)
elabRelationExpr taskName sinkStream' relationExpr = do
  sinkConfig <- genStreamSinkConfig sinkStream' relationExpr
  builder    <- newRandomText 20 >>= HS.mkStreamBuilder
  (es,srcs,joins,mats) <- relationExprToGraph relationExpr builder
  case es of
    EStream1 s ->
      case sinkConfig of
        SinkConfigType sink conf -> do
          builder <- HS.to conf s
          return (builder, srcs, sink, (joins,mats))
        SinkConfigTypeWithWindow _ _ -> throwSQLException CodegenException Nothing "SinkConfig does not match"
    EStream2 s ->
      case sinkConfig of
        SinkConfigTypeWithWindow sink conf -> do
          builder <- HS.to conf s
          return (builder, srcs, sink, (joins,mats))
        SinkConfigType _ _ -> throwSQLException CodegenException Nothing "SinkConfig does not match"
#endif
#endif
