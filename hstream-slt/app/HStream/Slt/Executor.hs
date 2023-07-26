module Slt.Executor where

import           Control.Monad       (forM, when)
import           Control.Monad.State
import qualified Data.Aeson.Key      as A
import qualified Data.Aeson.KeyMap   as A
import           Data.Functor
import           Data.Maybe
import qualified Data.Text           as T
import           Slt.Cli.Parser
import           Slt.Utils

----------------------------------------
-- ExecutorCtx
----------------------------------------

class MonadIO (m executor) => ExecutorCtx m executor where
  evalExecutorCtx :: m executor a -> IO a
  setOpts :: GlobalOpts -> m executor ()
  getOpts :: m executor GlobalOpts
  setExecutor :: executor -> m executor ()
  getExecutor :: m executor executor
  isDebug :: m executor Bool
  pushSql :: T.Text -> m executor ()
  getSql :: m executor [T.Text]

evalNewExecutorCtx :: forall m1 a m0 executor0 executor1. (ExecutorCtx m0 executor0, ExecutorCtx m1 executor1) => m1 executor1 a -> m0 executor0 a
evalNewExecutorCtx xs = do
  opts <- getOpts
  liftIO $ evalExecutorCtx @m1 $ do
    setOpts opts
    xs

----------------------------------------

newtype ExecutorM executor a = ExecutorM
  { unExecutorM :: StateT (ExecutorState executor) IO a
  }
  deriving (Functor, Applicative, Monad, MonadIO, MonadState (ExecutorState executor))

data ExecutorState executor = ExecutorState
  { executorStateOpts     :: GlobalOpts,
    executorStateExecutor :: Maybe executor,
    executorStateSqlSnoc  :: [T.Text]
  }

defaultExecutorState :: ExecutorState executor
defaultExecutorState =
  ExecutorState
    { executorStateOpts =
        GlobalOpts
          { debug = False,
            executorsAddr = []
          },
      executorStateExecutor = Nothing,
      executorStateSqlSnoc = []
    }

instance ExecutorCtx ExecutorM executor where
  evalExecutorCtx ExecutorM {unExecutorM = executor} =
    evalStateT executor defaultExecutorState
  setExecutor executor = do
    s <- get
    put $ s {executorStateExecutor = Just executor}
  isDebug = gets $ debug . executorStateOpts
  setOpts opts = do
    s <- get
    put $ s {executorStateOpts = opts}
  getOpts = gets executorStateOpts
  getExecutor = gets $ fromJust . executorStateExecutor
  pushSql x = do
    s <- get
    put $ s {executorStateSqlSnoc = x : executorStateSqlSnoc s}
  getSql = gets $ reverse . executorStateSqlSnoc

----------------------------------------

debugPrint :: (ExecutorCtx m executor, Show a) => a -> m executor ()
debugPrint x = do
  debug <- isDebug
  when debug $ do
    liftIO $ print x

debugPutStrLn :: ExecutorCtx m executor => String -> m executor ()
debugPutStrLn x = do
  debug <- isDebug
  when debug $ do
    liftIO $ putStrLn x

----------------------------------------
-- SltExecutor
----------------------------------------

class ExecutorCtx m executor => SltExecutor m executor | m -> executor where
  open' :: m executor executor
  open :: m executor ()
  open = setExecutor =<< open'
  insertValues :: T.Text -> Kv -> m executor ()
  selectWithoutFrom :: [T.Text] -> m executor Kv
  sqlDataTypeToLiteral' :: SqlDataType -> m executor T.Text
  sqlDataValueToLiteral :: SqlDataValue -> m executor T.Text

sqlDataTypeToLiteral :: SltExecutor m executor => SqlDataValue -> m executor T.Text
sqlDataTypeToLiteral value = sqlDataTypeToLiteral' (getSqlDataType value)

buildValues :: SltExecutor m executor => Kv -> m executor T.Text
buildValues kv = do
  h0 <- hs0
  h1 <- hs1
  pure $ " (" <> T.intercalate ", " h0 <> ") VALUES ( " <> T.intercalate ", " h1 <> " )"
  where
    hs0, hs1 :: SltExecutor m executor => m executor [T.Text]
    hs0 = pure $ A.keys kv <&> A.toText
    hs1 =
      forM (A.elems kv) $ \v -> do
        val <- sqlDataValueToLiteral v
        typ <- sqlDataTypeToLiteral v
        pure $
          "CAST ( "
            <> val
            <> " AS "
            <> typ
            <> " )"
