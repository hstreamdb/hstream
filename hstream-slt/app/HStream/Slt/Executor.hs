module Slt.Executor where

import Control.Monad (forM)
import Control.Monad.State
import Data.Aeson.Key qualified as A
import Data.Aeson.KeyMap qualified as A
import Data.Functor
import Data.Text qualified as T
import Slt.Cli.Parser (GlobalOpts)
import Slt.Utils

newtype ExecutorM executor a = ExecutorM
  { unExecutorM :: StateT (ExecutorState executor) IO a
  }
  deriving (Functor, Applicative, Monad, MonadIO, MonadState (ExecutorState executor))

data ExecutorState executor = ExecutorState
  { executorStateOpts :: GlobalOpts,
    executorStateExecutor :: Maybe executor
  }

setExecutor :: executor -> ExecutorM executor ()
setExecutor executor = do
  s <- get
  put $ s {executorStateExecutor = Just executor}

class MonadIO m => SltExecutor m where
  open :: m ()
  insertValues :: T.Text -> Kv -> m ()
  selectWithoutFrom :: [T.Text] -> m Kv
  sqlDataTypeToLiteral' :: SqlDataType -> m T.Text
  sqlDataValueToLiteral :: SqlDataValue -> m T.Text

sqlDataTypeToLiteral :: SltExecutor m => SqlDataValue -> m T.Text
sqlDataTypeToLiteral value = sqlDataTypeToLiteral' (getSqlDataType value)

buildValues :: SltExecutor m => Kv -> m T.Text
buildValues kv = do
  h0 <- hs0
  h1 <- hs1
  pure $ " (" <> T.intercalate ", " h0 <> ") VALUES ( " <> T.intercalate ", " h1 <> " )"
  where
    hs0, hs1 :: SltExecutor m => m [T.Text]
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
