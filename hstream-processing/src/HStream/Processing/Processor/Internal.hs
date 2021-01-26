{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE NoImplicitPrelude         #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE RecordWildCards           #-}
{-# LANGUAGE StrictData                #-}

module HStream.Processing.Processor.Internal where

import           Control.Exception        (throw)
import           Data.Default
import           Data.Typeable
import           HStream.Processing.Error (HStreamError (..))
import           HStream.Processing.Store
import           HStream.Processing.Type
import           RIO
import qualified RIO.HashMap              as HM
import qualified RIO.HashSet              as HS
import qualified RIO.Text                 as T

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

data Record k v
  = Record
      { recordKey :: Maybe k,
        recordValue :: v,
        recordTimestamp :: Timestamp
      }

data ERecord = forall k v. (Typeable k, Typeable v) => ERecord (Record k v)

mkERecord :: (Typeable k, Typeable v) => Record k v -> ERecord
mkERecord = ERecord

data TaskTopologyConfig
  = TaskTopologyConfig
      { ttcName :: T.Text,
        sourceCfgs :: HM.HashMap T.Text InternalSourceConfig,
        topology :: HM.HashMap T.Text (EProcessor, [T.Text]),
        sinkCfgs :: HM.HashMap T.Text InternalSinkConfig,
        stores :: HM.HashMap T.Text (EStateStore, HS.HashSet T.Text)
      }

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
                throw
                  $ TaskTopologyBuildError
                  $ "source named " `T.append` name `T.append` " already existed"
            )
            (sourceCfgs t1)
            (sourceCfgs t2),
        topology =
          HM.unionWithKey
            ( \name _ _ ->
                throw
                  $ TaskTopologyBuildError
                  $ "processor named " `T.append` name `T.append` " already existed"
            )
            (topology t1)
            (topology t2),
        sinkCfgs =
          HM.unionWithKey
            ( \name _ _ ->
                throw
                  $ TaskTopologyBuildError
                  $ "sink named " `T.append` name `T.append` " already existed"
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

data InternalSourceConfig
  = InternalSourceConfig
      { iSourceName :: T.Text,
        iSourceTopicName :: T.Text
      }

data InternalSinkConfig
  = InternalSinkConfig
      { iSinkName :: T.Text,
        iSinkTopicName :: T.Text
      }

type TaskBuilder = TaskTopologyConfig

data Task
  = Task
      { taskName :: T.Text,
        taskSourceConfig :: HM.HashMap T.Text InternalSourceConfig,
        taskTopologyReversed :: HM.HashMap T.Text (EProcessor, [T.Text]),
        taskTopologyForward :: HM.HashMap T.Text (EProcessor, [T.Text]),
        taskSinkConfig :: HM.HashMap T.Text InternalSinkConfig,
        taskStores :: HM.HashMap T.Text (EStateStore, HS.HashSet T.Text)
      }

data TaskContext
  = TaskContext
      { taskConfig :: Task,
        tctLogFunc :: LogFunc,
        curProcessor :: IORef T.Text,
        tcTimestamp :: IORef Int64
      }

instance HasLogFunc TaskContext where
  logFuncL = lens tctLogFunc (\x y -> x {tctLogFunc = y})

buildTaskContext ::
  Task ->
  LogFunc ->
  IO TaskContext
buildTaskContext task lf = do
  pRef <- newIORef ""
  tRef <- newIORef (-1)
  return $
    TaskContext
      { taskConfig = task,
        tctLogFunc = lf,
        curProcessor = pRef,
        tcTimestamp = tRef
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
