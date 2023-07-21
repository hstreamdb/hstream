module Slt.Utils where

import Control.Exception (SomeException)
import Control.Monad
import Control.Monad.Except (ExceptT)
import Data.Aeson.Key qualified as A
import Data.Aeson.KeyMap qualified as A
import Data.Bifunctor
import Data.ByteString qualified as BS
import Data.Text qualified as T
import Data.Time qualified as Time
import Database.SQLite.Simple.FromField qualified as S
import Database.SQLite.Simple.Ok qualified as S
import Test.QuickCheck
import Test.QuickCheck.Instances.ByteString ()
import Test.QuickCheck.Instances.Text ()
import Test.QuickCheck.Instances.Time ()

----------------------------------------

type Kv = A.KeyMap SqlDataValue

sqlDataValuesToKv :: [(T.Text, SqlDataValue)] -> Kv
sqlDataValuesToKv = A.fromList . fmap (first A.fromText)

----------------------------------------

type Result = ExceptT SomeException IO

----------------------------------------

typeLiterals :: [T.Text]
typeLiterals =
  [ "INTEGER",
    "FLOAT",
    "BOOLEAN",
    "BYTEA",
    "STRING",
    "DATE",
    "TIME",
    "TIMESTAMP",
    "INTERVAL",
    "JSONB",
    "NULL"
  ]

data SqlDataType
  = INTEGER
  | FLOAT
  | BOOLEAN
  | BYTEA
  | STRING
  | DATE
  | TIME
  | TIMESTAMP
  | INTERVAL
  | JSONB
  | NULL
  deriving (Show)

data SqlDataValue
  = VINTEGER Int
  | VFLOAT Double
  | VBOOLEAN Bool
  | VBYTEA BS.ByteString
  | VSTRING T.Text
  | VDATE Time.Day
  | VTIME Time.TimeOfDay
  | VTIMESTAMP Time.ZonedTime
  | VINTERVAL Time.CalendarDiffTime
  | VJSONB Kv
  | VNULL
  deriving (Show)

textToSqlDataType :: T.Text -> SqlDataType
textToSqlDataType = \case
  "INTEGER" -> INTEGER
  "FLOAT" -> FLOAT
  "BOOLEAN" -> BOOLEAN
  "BYTEA" -> BYTEA
  "STRING" -> STRING
  "DATE" -> DATE
  "TIME" -> TIME
  "TIMESTAMP" -> TIMESTAMP
  "INTERVAL" -> INTERVAL
  "JSONB" -> JSONB
  "NULL" -> NULL
  _ -> error "textToSqlDataType: no parse"

-- SQLite
instance S.FromField SqlDataValue where
  fromField :: S.Field -> S.Ok SqlDataValue
  fromField field = S.Ok $ h (words . show $ S.fieldData field)
    where
      h :: [String] -> SqlDataValue
      h ["SQLInteger", x] = VINTEGER $ read x
      h ["SQLFloat", x] = VFLOAT $ read x
      h ["SQLText", x] = VSTRING $ read x
      h ["SQLBlob", x] = VBYTEA $ read x
      h ["SQLNull"] = VNULL
      h _ = error "fromField: no parse @SqlDataValue"

----------------------------------------

randKvByColInfo :: [(T.Text, SqlDataType)] -> IO Kv
randKvByColInfo info =
  A.fromList
    <$> forM
      info
      ( \(k, v) -> do
          x <- case v of
            INTEGER -> h VINTEGER
            FLOAT -> h VFLOAT
            BOOLEAN -> h VBOOLEAN
            BYTEA -> h VBYTEA
            STRING -> h VSTRING
            DATE -> h VDATE
            TIME -> h VTIME
            TIMESTAMP -> h VTIMESTAMP
            INTERVAL -> h VINTERVAL
            JSONB -> undefined
            NULL -> pure VNULL
          pure (A.fromText k, x)
      )
  where
    h :: Arbitrary a => (a -> SqlDataValue) -> IO SqlDataValue
    h = (<$> generate arbitrary)
