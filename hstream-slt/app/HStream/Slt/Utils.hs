module Slt.Utils where

import           Control.Exception                    (SomeException)
import           Control.Monad
import           Control.Monad.Except                 (ExceptT)
import qualified Data.Aeson.Key                       as A
import qualified Data.Aeson.KeyMap                    as A
import           Data.Bifunctor
import qualified Data.ByteString                      as BS
import qualified Data.Text                            as T
import qualified Data.Text.Encoding                   as T
import qualified Data.Time                            as Time
import qualified Database.SQLite.Simple.FromField     as S
import qualified Database.SQLite.Simple.Ok            as S
import           Test.QuickCheck
import           Test.QuickCheck.Instances.ByteString ()
import           Test.QuickCheck.Instances.Text       ()
import           Test.QuickCheck.Instances.Time       ()

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

getSqlDataType :: SqlDataValue -> SqlDataType
getSqlDataType = \case
  VINTEGER _   -> INTEGER
  VFLOAT _     -> FLOAT
  VBOOLEAN _   -> BOOLEAN
  VBYTEA _     -> BYTEA
  VSTRING _    -> STRING
  VDATE _      -> DATE
  VTIME _      -> TIME
  VTIMESTAMP _ -> TIMESTAMP
  VINTERVAL _  -> INTERVAL
  VJSONB _     -> JSONB
  VNULL        -> NULL

textToSqlDataType :: T.Text -> SqlDataType
textToSqlDataType = \case
  "INTEGER"   -> INTEGER
  "FLOAT"     -> FLOAT
  "BOOLEAN"   -> BOOLEAN
  "BYTEA"     -> BYTEA
  "STRING"    -> STRING
  "DATE"      -> DATE
  "TIME"      -> TIME
  "TIMESTAMP" -> TIMESTAMP
  "INTERVAL"  -> INTERVAL
  "JSONB"     -> JSONB
  "NULL"      -> NULL
  _           -> error "textToSqlDataType: no parse"

-- SQLite
instance S.FromField SqlDataValue where
  fromField :: S.Field -> S.Ok SqlDataValue
  fromField field = S.Ok $ h (words . show $ S.fieldData field)
    where
      h :: [String] -> SqlDataValue
      h ["SQLInteger", x] = VINTEGER $ read x
      h ["SQLFloat", x]   = VFLOAT $ read x
      h ["SQLText", x]    = VSTRING $ read x
      h ["SQLBlob", x]    = VBYTEA $ read x
      h ["SQLNull"]       = VNULL
      h _                 = error "fromField: no parse @SqlDataValue"

----------------------------------------

randKvByColInfo :: [(T.Text, SqlDataType)] -> IO Kv
randKvByColInfo info =
  A.fromList
    <$> forM
      info
      ( \(k, v) -> do
          x <- randSqlDataValue v
          pure (A.fromText k, x)
      )

randSqlDataValue :: SqlDataType -> IO SqlDataValue
randSqlDataValue = \case
  INTEGER   -> h VINTEGER
  FLOAT     -> h VFLOAT
  BOOLEAN   -> h VBOOLEAN
  BYTEA     -> h VBYTEA
  STRING    -> h VSTRING
  DATE      -> h VDATE
  TIME      -> h VTIME
  TIMESTAMP -> h VTIMESTAMP
  INTERVAL  -> h VINTERVAL
  JSONB     -> error "currently unsupported"
  NULL      -> pure VNULL
  where
    h :: Arbitrary a => (a -> SqlDataValue) -> IO SqlDataValue
    h = (<$> generate arbitrary)

----------------------------------------

sqlDataTypeToAnsiLiteral :: SqlDataValue -> T.Text
sqlDataTypeToAnsiLiteral = \case
  VINTEGER x   -> h x
  VFLOAT x     -> h x
  VBOOLEAN x   -> h x
  VBYTEA x     -> esc $ T.decodeUtf8 x
  VSTRING x    -> esc x
  VDATE x      -> h x
  VTIME x      -> h x
  VTIMESTAMP x -> h x
  VINTERVAL x  -> h x
  VJSONB _     -> error "currently unsupported"
  VNULL        -> "NULL"
  where
    h :: Show a => a -> T.Text
    h = T.pack . show
    esc x = "'" <> T.tail (T.init x) <> "'"
