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

module HStream.SQL.Codegen.V1 where

#ifndef HStreamUseV2Engine
import           Data.Aeson                                      (Object,
                                                                  Value (Bool, Null, Number, String))
import qualified Data.Aeson                                      as Aeson
import           Data.Bifunctor
import qualified Data.ByteString.Char8                           as BSC
import           Data.Function
import           Data.Functor
import qualified Data.HashMap.Strict                             as HM
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
import           HStream.SQL.AST
import           HStream.SQL.Codegen.ColumnCatalog
import           HStream.SQL.Codegen.Common
import           HStream.SQL.Codegen.Utils
import           HStream.SQL.Codegen.V1.Boilerplate
import           HStream.SQL.Codegen.V1.Transform
import           HStream.SQL.Exception                           (SomeSQLException (..),
                                                                  throwSQLException)
import           HStream.SQL.Parse                               (parseAndRefine)
import           HStream.SQL.Planner
import qualified HStream.SQL.Planner                             as Planner
import           HStream.Utils                                   (jsonObjectToStruct,
                                                                  newRandomText)
import qualified HStream.Utils.Aeson                             as HsAeson

--------------------------------------------------------------------------------
type K = FlowObject
type V = FlowObject

type Ser  = BL.ByteString

type ViewName = T.Text
type ConnectorName  = T.Text
type CheckIfExist  = Bool
type ViewSchema = [String]

data ShowObject = SStreams | SQueries | SConnectors | SViews
data DropObject = DStream Text | DView Text | DConnector Text | DQuery Text
data TerminateObject = TQuery Text
data InsertType = JsonFormat | RawFormat
data PauseObject
  = PauseObjectConnector Text
  | PauseObjectQuery Text
data ResumeObject
  =  ResumeObjectConnector Text
  | ResumeObjectQuery Text

type Persist = ([HS.StreamJoined K V K V Ser], [HS.Materialized K V V])

data HStreamPlan
  = CreatePlan          StreamName RStreamOptions
  | CreateConnectorPlan ConnectorType ConnectorName Text Bool (HM.HashMap Text Value)
  | InsertPlan          StreamName InsertType ByteString
  | DropPlan            CheckIfExist DropObject
  | ShowPlan            ShowObject
  | TerminatePlan       TerminateObject
  | ExplainPlan         Text
  | PausePlan           PauseObject
  | ResumePlan          ResumeObject
  | SelectPlan          [StreamName] StreamName TaskBuilder Persist ([ColumnCatalog], [ColumnCatalog])
  | PushSelectPlan      [StreamName] StreamName TaskBuilder Persist
  | CreateBySelectPlan  [StreamName] StreamName TaskBuilder RStreamOptions Persist
  | CreateViewPlan      [StreamName] StreamName ViewName TaskBuilder Persist

--------------------------------------------------------------------------------
streamCodegen :: HasCallStack => Text -> IO HStreamPlan
streamCodegen input = parseAndRefine input >>= hstreamCodegen

hstreamCodegen :: HasCallStack => RSQL -> IO HStreamPlan
hstreamCodegen = \case
  RQSelect select -> do
    tName <- genTaskName
    let (select', keys, keysAdded) = addGroupByKeysToProjectItems select
    (builder, srcs, sink, persist) <- elabRSelect tName Nothing select'
    return $ SelectPlan srcs sink (HS.build builder) persist (keys, keysAdded)
  RQPushSelect select -> do
    tName <- genTaskName
    (builder, srcs, sink, persist) <- elabRSelect tName Nothing select
    return $ PushSelectPlan srcs sink (HS.build builder) persist
  RQCreate (RCreateAs stream select rOptions) -> do
    tName <- genTaskName
    (builder, srcs, sink, persist) <- elabRSelect tName (Just stream) select
    return $ CreateBySelectPlan srcs sink (HS.build builder) rOptions persist
  RQCreate (RCreateView view select) -> do
    tName <- genTaskName
    (builder, srcs, sink, persist) <- elabRSelect tName (Just $ view <> "_view") select
    return $ CreateViewPlan srcs sink view (HS.build builder) persist
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
  RQDrop (RDrop RDropQuery x)        -> return $ DropPlan False (DQuery x)
  RQDrop (RDropIf RDropConnector x)  -> return $ DropPlan True (DConnector x)
  RQDrop (RDropIf RDropStream x)     -> return $ DropPlan True (DStream x)
  RQDrop (RDropIf RDropView x)       -> return $ DropPlan True (DView x)
  RQDrop (RDropIf RDropQuery x)      -> return $ DropPlan True (DQuery x)
  RQTerminate (RTerminateQuery qid)  -> return $ TerminatePlan (TQuery qid)
  RQExplain rselect                  -> do
    let relationExpr = decouple rselect
    return $ ExplainPlan (PP.renderStrict $ PP.layoutPretty PP.defaultLayoutOptions (PP.pretty relationExpr))
  RQPause (RPauseConnector name)     -> return $ PausePlan (PauseObjectConnector name)
  RQPause (RPauseQuery name)         -> return $ PausePlan (PauseObjectQuery name)
  RQResume (RResumeConnector name)   -> return $ ResumePlan (ResumeObjectConnector name)
  RQResume (RResumeQuery name)       -> return $ ResumePlan (ResumeObjectQuery name)

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

renameMapR :: Text -> Record k V -> Record k V
renameMapR alias = \record@Record{..} -> record{ recordValue = streamRenamer alias recordValue }

filterFilterR :: ScalarExpr -> Record k V -> Bool
filterFilterR scalar =
  \Record{..} ->
    case scalarExprToFun scalar recordValue of
      Left e  -> False -- FIXME: log error message
      Right v -> v == FlowBoolean True

projectMapR :: [(ColumnCatalog, ColumnCatalog)] -> [Text] -> Record k V -> Record k V
projectMapR cataTups streams =
  \record@Record{..} ->
    let recordValue' =
          L.foldr (\(cata_get,cata_as) acc ->
                     case getField cata_get recordValue of
                       Nothing    -> acc
                       Just (_,v) -> HM.insert cata_as v acc
                  ) HM.empty cataTups
          `HM.union`
          L.foldr (\stream acc ->
                      acc `HM.union` (HM.filterWithKey (\(ColumnCatalog _ s_m) _ -> s_m == Just stream) recordValue)
                  ) HM.empty streams
     in record{ recordValue = recordValue' }

affiliateMapR :: [(ColumnCatalog,ScalarExpr)] -> Record k V -> Record k V
affiliateMapR tups =
  \record@Record{..} ->
    let recordValue' =
          L.foldr (\(cata,scalar) acc ->
                     case scalarExprToFun scalar recordValue of
                       Left e  -> HM.insert cata FlowNull acc
                       Right v -> HM.insert cata v acc
                  ) recordValue tups
     in record{ recordValue = recordValue' }

relationExprToGraph :: RelationExpr -> StreamBuilder -> IO (EStream, [StreamName], [StreamJoined K V K V Ser], [HS.Materialized K V V])
relationExprToGraph relation builder = case relation of
  StreamScan stream -> do
    let sourceConfig = HS.StreamSourceConfig
                     { sscStreamName = stream
                     , sscKeySerde   = flowObjectSerde
                     , sscValueSerde = flowObjectSerde
                     }
    -- FIXME: builder name
    s' <- HS.stream sourceConfig builder
    return (EStream1 s', [stream], [], [])
  StreamRename r alias -> do
    (es,srcs,joins,mats) <- relationExprToGraph r builder
    es' <- withEStreamM es SK (HS.map $ renameMapR alias) (HS.map $ renameMapR alias)
    return (es',srcs,joins,mats)
  CrossJoin r1 r2 t -> do
    let joiner = HM.union
        joinCond = \_ _ -> True
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
  LoopJoinOn r1 r2 expr typ t -> do
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
  LoopJoinUsing r1 r2 cols typ t -> do
    let joiner = HM.union
        joinCond = \record1 record2 ->
          HM.mapKeys (\(ColumnCatalog f _) -> ColumnCatalog f Nothing) (HM.filterWithKey (\(ColumnCatalog f s_m) _ -> isJust s_m && L.elem f cols) (recordValue record1)) ==
          HM.mapKeys (\(ColumnCatalog f _) -> ColumnCatalog f Nothing) (HM.filterWithKey (\(ColumnCatalog f s_m) _ -> isJust s_m && L.elem f cols) (recordValue record2))
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
  LoopJoinNatural r1 r2 typ t -> do
    let joiner = HM.union
        joinCond = \record1 record2 ->
          HM.foldlWithKey (\acc k@(ColumnCatalog f _) v ->
                               if acc then
                                 case getField (ColumnCatalog f Nothing) (recordValue record2) of
                                   Nothing     -> acc
                                   Just (_,v') -> v == v'
                               else False
                            ) True (recordValue record1)
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
  Planner.Filter r scalar -> do
    (es,srcs,joins,mats) <- relationExprToGraph r builder
    es' <- withEStreamM es SK (HS.filter $ filterFilterR scalar)
                              (HS.filter $ filterFilterR scalar)
    return (es',srcs,joins,mats)
  Project r cataTups streams -> do
    (es,srcs,joins,mats) <- relationExprToGraph r builder
    es' <- withEStreamM es SK (HS.map $ projectMapR cataTups streams)
                              (HS.map $ projectMapR cataTups streams)
    return (es',srcs,joins,mats)
  Affiliate r tups -> do
    (es,srcs,joins,mats) <- relationExprToGraph r builder
    es' <- withEStreamM es SK (HS.map $ affiliateMapR tups)
                              (HS.map $ affiliateMapR tups)
    return (es',srcs,joins,mats)
  Reduce r keyTups aggTups win_m -> do
    let keygen = \Record{..} ->
          L.foldr (\(cata,scalar) acc ->
                      case scalarExprToFun scalar recordValue of
                        Left _  -> HM.insert cata FlowNull acc
                        Right v -> HM.insert cata v acc
                  ) HM.empty keyTups
    let aggComp@AggregateComponent{..} =
          composeAggs (L.map (\(cata,agg) -> genAggregateComponent agg cata) aggTups)
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
  Distinct r -> do
    throwSQLException CodegenException Nothing "Distinct is not supported"
  Union r1 r2 -> do
    throwSQLException CodegenException Nothing "Union is not supported"

data SinkConfigType = SinkConfigType StreamName (HS.StreamSinkConfig K V Ser)
                    | SinkConfigTypeWithWindow StreamName (HS.StreamSinkConfig (TimeWindowKey K) V Ser)

genStreamSinkConfig :: Maybe StreamName -> RGroupBy -> IO SinkConfigType
genStreamSinkConfig sinkStream' grp = do
  stream <- maybe genRandomSinkStream return sinkStream'
  case grp of
    RGroupBy _ (Just (Tumbling i)) ->
      return $ SinkConfigTypeWithWindow stream HS.StreamSinkConfig
      { sicStreamName = stream
      , sicKeySerde = timeWindowKeySerde flowObjectSerde (timeWindowSerde $ calendarDiffTimeToMs i) (calendarDiffTimeToMs i)
      , sicValueSerde = flowObjectSerde
      }
    RGroupBy _ (Just (Hopping i _)) ->
      return $ SinkConfigTypeWithWindow stream HS.StreamSinkConfig
      { sicStreamName = stream
      , sicKeySerde = timeWindowKeySerde flowObjectSerde (timeWindowSerde $ calendarDiffTimeToMs i) (calendarDiffTimeToMs i)
      , sicValueSerde = flowObjectSerde
      }
    RGroupBy _ (Just (Session i)) ->
      return $ SinkConfigTypeWithWindow stream HS.StreamSinkConfig
      { sicStreamName = stream
      , sicKeySerde = sessionWindowKeySerde flowObjectSerde (timeWindowSerde $ calendarDiffTimeToMs i)
      , sicValueSerde = flowObjectSerde
      }
    _ ->
      return $ SinkConfigType stream HS.StreamSinkConfig
      { sicStreamName  = stream
      , sicKeySerde   = flowObjectSerde
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
            -> RSelect
            -> IO (StreamBuilder, [StreamName], StreamName, Persist)
elabRSelect taskName sinkStream' select@(RSelect sel frm whr grp hav) = do
  sinkConfig           <- genStreamSinkConfig sinkStream' grp
  builder <- newRandomText 20 >>= HS.mkStreamBuilder
  (es,srcs,joins,mats) <- relationExprToGraph (decouple select) builder
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
