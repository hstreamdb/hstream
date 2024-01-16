{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.Kafka.Common.Security where

import           Data.Text (Text)
import qualified Data.Text as T

-- | A kafka principal. It is a 2-tuple of non-null type and non-null name.
--   For default authorizer, type is "User".
data Principal = Principal
  { principalType :: Text
  , principalName :: Text
  } deriving (Eq, Ord)
instance Show Principal where
  show Principal{..}
    -- FIXME: Temporarily allow creating principal with empty type and name
    --        and show it as empty text. However reading from empty text is
    --        not allowed. We should distinguish between empty text and null.
    | T.null principalType && T.null principalName = ""
    | otherwise = T.unpack principalType <> ":" <> T.unpack principalName

wildcardPrincipal :: Principal
wildcardPrincipal = Principal "User" "*"

-- FIXME: error
-- | Build a 'Principal' from 'Text' with format "type:name".
--   Neither type nor name can be empty.
--   May throw exceptions if the format is invalid.
principalFromText :: Text -> Principal
principalFromText t = case T.splitOn ":" t of
  [t1, t2] -> if T.null t1 then error "principalFromText: empty type" else
                if T.null t2 then error "principalFromText: empty name" else
                  Principal t1 t2
  _        -> error "principalFromText: invalid principal"

wildcardHost :: Text
wildcardHost = "*"
