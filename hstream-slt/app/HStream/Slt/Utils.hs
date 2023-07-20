module Slt.Utils where

import           Control.Exception    (SomeException)
import           Control.Monad.Except (ExceptT)
import qualified Data.Aeson           as A
import qualified Data.Aeson.Key       as A
import qualified Data.Aeson.KeyMap    as A
import qualified Data.ByteString      as BS
import           Data.Functor
import qualified Data.Text            as T
import qualified Data.Time            as Time

----------------------------------------

type Kv = A.KeyMap A.Value

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
    "JSONB"
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
  deriving (Show)

-- data SqlDataTypeValue
--   = VINTEGER Int
--   | VFLOAT Double
--   | VBOOLEAN Bool
--   | VBYTEA BS.ByteString
--   | VSTRING T.Text
--   | VDATE Time.Day
--   | VTIME Time.TimeOfDay
--   | VINTERVAL Time.CalendarDiffTime
--   | VJSONB Kv

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
  _           -> error "textToSqlDataType: no parse"

----------------------------------------

randKvByColInfo :: [(T.Text, SqlDataType)] -> Kv
randKvByColInfo info =
  A.fromList $
    info <&> \(k, v) ->
      (,) (A.fromText k) $ case v of {}
