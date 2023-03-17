{-# LANGUAGE AllowAmbiguousTypes       #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE NoImplicitPrelude         #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE RecordWildCards           #-}
{-# LANGUAGE StrictData                #-}
{-# LANGUAGE TypeApplications          #-}

module HStream.Processing.Processor.Internal where

import           Control.Exception                      (throw)
import           Data.Default
import           Data.Typeable
import           HStream.Processing.Error               (HStreamProcessingError (..))
import           HStream.Processing.Processor.ChangeLog
import           HStream.Processing.Processor.Snapshot
import           HStream.Processing.Store
import           HStream.Processing.Type
import           RIO
import qualified RIO.ByteString.Lazy                    as BL
import qualified RIO.HashMap                            as HM
import qualified RIO.HashSet                            as HS
import qualified RIO.Text                               as T

newtype Processor kin vin = Processor {runP :: Record kin vin -> RIO TaskContext ()}

newtype EProcessor = EProcessor {runEP :: ERecord -> RIO TaskContext ()}

mkEProcessor ::
  (Typeable k, Typeable v) =>
  Processor k v ->
  EProcessor
mkEProcessor proc = EProcessor $ \(ERecord record) ->
  case cast record of
    Just r -> runP proc r
    Nothing -> throw $ TypeCastError ("mkEProcessor: type cast error, real record type is: " `T.append` T.pack (show (typeOf record)))

--
newtype SourceProcessor = SourceProcessor {runSourceP :: RIO TaskContext ()}

newtype SinkProcessor = SinkProcessor {runSinkP :: BL.ByteString -> RIO TaskContext ()}

data Record k v = Record
  { recordKey       :: Maybe k,
    recordValue     :: v,
    recordTimestamp :: Timestamp
  }

data ERecord = forall k v. (Typeable k, Typeable v) => ERecord (Record k v)

mkERecord :: (Typeable k, Typeable v) => Record k v -> ERecord
mkERecord = ERecord

data TaskTopologyConfig = TaskTopologyConfig
  { ttcName    :: T.Text,
    sourceCfgs :: HM.HashMap T.Text InternalSourceConfig,
    topology   :: HM.HashMap T.Text (EProcessor, [T.Text]),
    sinkCfgs   :: HM.HashMap T.Text InternalSinkConfig,
    stores     :: HM.HashMap T.Text (EStateStore, HS.HashSet T.Text)
  }

----
applyStateStoreChangelog :: (Typeable k, Typeable v, Typeable ser,
                             Ord k, Ord ser)
                         => TaskBuilder -> StateStoreChangelog k v ser -> IO TaskBuilder
applyStateStoreChangelog builder@TaskTopologyConfig{..} cl = case cl of
  CLKSPut storeName k v -> do
    case HM.lookup storeName stores of
      Nothing      -> return builder -- FIXME: log error message
      Just (ess,_) -> case ess of
        EKVStateStore dekvs -> do
          let ekvs = fromDEKVStoreToEKVStore dekvs
          ksPut k v ekvs
          return builder
        _                   -> return builder -- FIXME: log error message
  CLSSPut storeName tk v -> do
    case HM.lookup storeName stores of
      Nothing      -> return builder -- FIXME: log error message
      Just (ess,_) -> case ess of
        ESessionStateStore desss -> do
          let esss = fromDESessionStoreToESessionStore desss
          ssPut tk v esss
          return builder
        _                        -> return builder -- FIXME: log error message
  CLSSRemove storeName tk -> do
    case HM.lookup storeName stores of
      Nothing      -> return builder -- FIXME: log error message
      Just (ess,_) -> case ess of
        ESessionStateStore desss -> do
          let esss = fromDESessionStoreToESessionStore @_ @() desss
          ssRemove tk esss
          return builder
        _                        -> return builder -- FIXME: log error message
  CLTKSPut storeName tsk ser -> do
    case HM.lookup storeName stores of
      Nothing      -> return builder -- FIXME: log error message
      Just (ess,_) -> case ess of
        ETimestampedKVStateStore detkvs -> do
          let etkvs = fromDETimestampedKVStoreToETimestampedKVStore detkvs
          tksPut tsk ser etkvs
          return builder
        _                               -> return builder -- FIXME: log error message

applyStateStoreSnapshot :: (Typeable k, Ord k, Typeable v, Typeable ser,
                            Ord ser)
                        => TaskBuilder
                        -> StateStoreSnapshotKey
                        -> StateStoreSnapshotValue i k v ser
                        -> IO (Maybe TaskBuilder, i)
applyStateStoreSnapshot builder@TaskTopologyConfig{..} k v = do
  let storeName = snapshotStoreName k
  case v of
    SnapshotKS i extData -> do
      case HM.lookup storeName stores of
        Nothing      -> return (Nothing, i) -- FIXME: log error message
        Just (ess,_) -> case ess of
          EKVStateStore dekvs -> do
            let ekvs = fromDEKVStoreToEKVStore dekvs
            ksImport ekvs extData
            return (Just builder, i)
          _                   -> return (Nothing, i)
    SnapshotSS i extData -> do
      case HM.lookup storeName stores of
        Nothing      -> return (Nothing, i) -- FIXME: log error message
        Just (ess,_) -> case ess of
          ESessionStateStore desss -> do
            let esss = fromDESessionStoreToESessionStore desss
            ssImport esss extData
            return (Just builder, i)
          _                        -> return (Nothing, i) -- FIXME: log error message
    SnapshotTKS i extData -> do
      case HM.lookup storeName stores of
        Nothing      -> return (Nothing, i) -- FIXME: log error message
        Just (ess,_) -> case ess of
          ETimestampedKVStateStore detkvs -> do
            let etkvs = fromDETimestampedKVStoreToETimestampedKVStore detkvs
            tksImport etkvs extData
            return (Just builder, i)
          _                               -> return (Nothing, i) -- FIXME: log error message

instance Default TaskTopologyConfig where
  def =
    TaskTopologyConfig
      { ttcName = T.empty,
        sourceCfgs = HM.empty,
        topology = HM.empty,
        sinkCfgs = HM.empty,
        stores = HM.empty
      }

instance Semigroup TaskTopologyConfig where
  t1 <> t2 =
    TaskTopologyConfig
      { ttcName = ttcName t1,
        sourceCfgs =
          HM.unionWithKey
            ( \name _ _ ->
                throw $
                  TaskTopologyBuildError $
                    "source named " `T.append` name `T.append` " already existed"
            )
            (sourceCfgs t1)
            (sourceCfgs t2),
        topology =
          HM.unionWithKey
            ( \name _ _ ->
                throw $
                  TaskTopologyBuildError $
                    "processor named " `T.append` name `T.append` " already existed"
            )
            (topology t1)
            (topology t2),
        sinkCfgs =
          HM.unionWithKey
            ( \name _ _ ->
                throw $
                  TaskTopologyBuildError $
                    "sink named " `T.append` name `T.append` " already existed"
            )
            (sinkCfgs t1)
            (sinkCfgs t2),
        stores =
          HM.unionWithKey
            ( \_ (s1, processors1) (_, processors2) ->
                (s1, HS.union processors2 processors1)
            )
            (stores t1)
            (stores t2)
      }

instance Monoid TaskTopologyConfig where
  mempty = def

getTaskName :: TaskTopologyConfig -> T.Text
getTaskName TaskTopologyConfig {..} = ttcName

data InternalSourceConfig = InternalSourceConfig
  { iSourceName       :: T.Text,
    iSourceStreamName :: T.Text
  }

data InternalSinkConfig = InternalSinkConfig
  { iSinkName       :: T.Text,
    iSinkStreamName :: T.Text
  }

type TaskBuilder = TaskTopologyConfig

data Task = Task
  { taskName             :: T.Text,
    taskSourceConfig     :: HM.HashMap T.Text InternalSourceConfig,
    taskTopologyReversed :: HM.HashMap T.Text (EProcessor, [T.Text]),
    taskTopologyForward  :: HM.HashMap T.Text (EProcessor, [T.Text]),
    taskSinkConfig       :: HM.HashMap T.Text InternalSinkConfig,
    taskStores           :: HM.HashMap T.Text (EStateStore, HS.HashSet T.Text)
  }

data TaskContext = forall h1 h2. (ChangeLogger h1, Snapshotter h2) => TaskContext
  { taskConfig     :: Task,
    tctLogFunc     :: LogFunc,
    curProcessor   :: IORef T.Text,
    tcTimestamp    :: IORef Int64,
    tcChangeLogger :: h1,
    tcSnapshotter  :: h2
  }

instance HasLogFunc TaskContext where
  logFuncL = lens tctLogFunc (\x y -> x {tctLogFunc = y})

buildTaskContext ::
  (ChangeLogger h1, Snapshotter h2) =>
  Task ->
  LogFunc ->
  h1 ->
  h2 ->
  IO TaskContext
buildTaskContext task lf changeLogger snapshotter = do
  pRef <- newIORef ""
  tRef <- newIORef (-1)
  return $
    TaskContext
      { taskConfig = task,
        tctLogFunc = lf,
        curProcessor = pRef,
        tcTimestamp = tRef,
        tcChangeLogger = changeLogger,
        tcSnapshotter = snapshotter
      }

updateTimestampInTaskContext :: TaskContext -> Timestamp -> IO ()
updateTimestampInTaskContext TaskContext {..} recordTimestamp = do
  oldTs <- readIORef tcTimestamp
  writeIORef tcTimestamp (max oldTs recordTimestamp)

getTimestampInTaskContext :: TaskContext -> IO Timestamp
getTimestampInTaskContext TaskContext {..} = readIORef tcTimestamp

getStateStoreFromTaskBuilder :: T.Text -> TaskBuilder -> Maybe EStateStore
getStateStoreFromTaskBuilder storeName TaskTopologyConfig {..} =
  fst <$> HM.lookup storeName stores

addEStateStore ::
  T.Text ->
  EStateStore ->
  [T.Text] ->
  TaskBuilder
addEStateStore storeName store processors =
  mempty
    { stores =
        HM.singleton
          storeName
          (store, HS.fromList processors)
    }
