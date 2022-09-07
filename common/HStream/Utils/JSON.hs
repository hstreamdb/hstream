{-# OPTIONS_GHC -Wno-orphans #-}

module HStream.Utils.JSON
  ( toQuietSnakeAesonOpt
  , toQuietSnakeAesonOpt'
  , flattenJSON
  , fillWithJsonString
  , fillWithJsonString'
  , extractJsonStringDef
  , extractJsonStringDef'
  ) where

import           Control.Monad           (join)
import qualified Data.Aeson              as A
import           Data.Bifunctor          (first)
import           Data.Text               (Text)
import qualified Data.Text               as Text
import           Text.Casing             (fromHumps, toQuietSnake)
import           Z.Data.CBytes           (CBytes)

import qualified HStream.Utils.Aeson     as A
import           HStream.Utils.Common
import           HStream.Utils.Converter

-- | Flatten all JSON structures.
--
-- >>> flatten (A.fromList [("a", A.Object $ A.fromList [("b", A.Number 1)])])
-- fromList [("a.b",Number 1.0)]
flattenJSON :: A.Object -> A.Object
flattenJSON jsonMap =
  let flattened = join $ map (flattenJSON' splitor prefix) (A.toList jsonMap)
      splitor = A.fromText "."
      prefix = A.fromText Text.empty
   in A.fromList $ map (first $ A.fromText . Text.tail . A.toText) flattened

flattenJSON' :: A.Key -> A.Key -> (A.Key, A.Value) -> [(A.Key, A.Value)]
flattenJSON' splitor prefix (k, v) = do
  -- TODO: we will not support array?
  case v of
    A.Object o -> join $ map (flattenJSON' splitor (prefix <> splitor <> k)) (A.toList o)
    _          -> [(prefix <> splitor <> k, v)]

toQuietSnakeAesonOpt :: String -> A.Options
toQuietSnakeAesonOpt p = A.defaultOptions
  { A.fieldLabelModifier = toQuietSnake . fromHumps . withoutPrefix p }

toQuietSnakeAesonOpt' :: String -> A.Options
toQuietSnakeAesonOpt' p = A.defaultOptions
  { A.fieldLabelModifier     = toQuietSnake . fromHumps . withoutPrefix p
  , A.constructorTagModifier = toQuietSnake . fromHumps . withoutPrefix p
  }

fillWithJsonString :: Text -> A.Object -> Text
fillWithJsonString key obj =
  maybe ("No such " <> key <> " key") extractJsonStringDef $
    A.lookup (A.fromText key) obj

extractJsonStringDef :: A.Value -> Text
extractJsonStringDef (A.String x) = x
extractJsonStringDef x = "Expecting string value, but got " <> Text.pack (show x)
{-# INLINE extractJsonStringDef #-}

fillWithJsonString' :: Text -> A.Object -> String
fillWithJsonString' key obj =
  let msg = "No such " <> Text.unpack key <> " key"
   in maybe msg extractJsonStringDef' $ A.lookup (A.fromText key) obj

extractJsonStringDef' :: A.Value -> String
extractJsonStringDef' (A.String x) = Text.unpack x
extractJsonStringDef' x = "Expecting string value, but got " <> show x
{-# INLINE extractJsonStringDef' #-}

--------------------------------------------------------------------------------

instance A.FromJSON CBytes where
  parseJSON v = let pText = A.parseJSON v in textToCBytes <$> pText

instance A.ToJSON CBytes where
  toJSON cb = A.toJSON (cBytesToText cb)

instance A.FromJSONKey CBytes
instance A.ToJSONKey CBytes
