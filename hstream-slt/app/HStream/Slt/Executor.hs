module Slt.Executor where

import qualified Data.Aeson.Key    as A
import qualified Data.Aeson.KeyMap as A
import           Data.Function
import           Data.Functor
import qualified Data.Text         as T
import           Slt.Utils

class SltExecutor a where
  open :: IO a
  insertValues :: T.Text -> Kv -> a -> IO ()
  selectWithoutFrom :: [T.Text] -> a -> IO Kv
  sqlDataTypeToLiteral' :: SqlDataType -> a -> T.Text
  sqlDataValueToLiteral :: SqlDataValue -> a -> T.Text

sqlDataTypeToLiteral :: SltExecutor a => SqlDataValue -> a -> T.Text
sqlDataTypeToLiteral value executor = executor & sqlDataTypeToLiteral' (getSqlDataType value)

buildValues :: SltExecutor a => Kv -> a -> T.Text
buildValues kv executor = " (" <> T.intercalate ", " h0 <> ") VALUES ( " <> T.intercalate ", " h1 <> " )"
  where
    h0, h1 :: [T.Text]
    h0 = A.keys kv <&> A.toText
    h1 =
      A.elems kv <&> \v ->
        "CAST ( "
          <> (executor & sqlDataValueToLiteral v)
          <> " AS "
          <> (executor & sqlDataTypeToLiteral v)
          <> " )"
