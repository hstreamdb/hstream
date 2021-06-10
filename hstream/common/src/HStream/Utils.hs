{-# LANGUAGE OverloadedStrings #-}

module HStream.Utils
  ( module HStream.Utils.Converter
  , module HStream.Utils.Format
  , getKeyWordFromException
  , flattenJSON
  ) where

import           Control.Exception                 (Exception (..))
import qualified Data.Text.Lazy                    as TL

import           Control.Monad                     (join)
import           Data.Aeson                        as Aeson
import           Data.Bifunctor                    (first)
import qualified Data.HashMap.Strict               as HM
import           Data.Text                         (Text)
import qualified Data.Text                         as Text
import           HStream.Utils.Converter
import           HStream.Utils.Format
import           ThirdParty.Google.Protobuf.Struct

getKeyWordFromException :: Exception a => a -> TL.Text
getKeyWordFromException =  TL.pack . takeWhile (/='{') . show

-- | Flatten all JSON structures.
--
-- >>> flatten (HM.fromList [("a", Aeson.Object $ HM.fromList [("b", Aeson.Number 1)])])
-- fromList [("a.b",Number 1.0)]
flattenJSON :: HM.HashMap Text Aeson.Value -> HM.HashMap Text Aeson.Value
flattenJSON jsonMap =
  let flattened = join $ map (flattenJSON' "." Text.empty) (HM.toList jsonMap)
   in HM.fromList $ map (first Text.tail) flattened

flattenJSON' :: Text -> Text -> (Text, Aeson.Value) -> [(Text, Aeson.Value)]
flattenJSON' splitor prefix (k, v) = do
  -- TODO: we will not support array?
  case v of
    Aeson.Object o -> join $ map (flattenJSON' splitor (prefix <> splitor <> k)) (HM.toList o)
    _              -> [(prefix <> splitor <> k, v)]
