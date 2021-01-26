{-# LANGUAGE FlexibleInstances #-}

module HStream.SQL.Codegen.Boilerplate where

import           Data.Aeson
import qualified Data.Binary                 as B
import           Data.Maybe                  (fromJust)
import qualified Data.Text.Lazy              as TL
import qualified Data.Text.Lazy.Encoding     as TLE
import           HStream.Processing.Encoding (Deserializer (Deserializer),
                                              Serde (..),
                                              Serializer (Serializer))
import           RIO                         (Void)

textSerde :: Serde TL.Text
textSerde =
  Serde
  { serializer   = Serializer   TLE.encodeUtf8
  , deserializer = Deserializer TLE.decodeUtf8
  }

objectSerde :: Serde Object
objectSerde =
  Serde
  { serializer   = Serializer   encode
  , deserializer = Deserializer $ fromJust . decode
  }

intSerde :: Serde Int
intSerde =
  Serde
  { serializer = Serializer B.encode
  , deserializer = Deserializer B.decode
  }

voidSerde :: Serde Void
voidSerde =
  Serde
  { serializer = Serializer B.encode
  , deserializer = Deserializer B.decode
  }
