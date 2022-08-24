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
import           Data.Aeson              as Aeson
import           Data.Bifunctor          (first)
import qualified Data.HashMap.Strict     as HM
import           Data.Text               (Text)
import qualified Data.Text               as Text
import           Text.Casing             (fromHumps, toQuietSnake)
import           Z.Data.CBytes           (CBytes)

import           HStream.Utils.Common
import           HStream.Utils.Converter

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

toQuietSnakeAesonOpt :: String -> Aeson.Options
toQuietSnakeAesonOpt p = Aeson.defaultOptions
  { Aeson.fieldLabelModifier = toQuietSnake . fromHumps . withoutPrefix p }

toQuietSnakeAesonOpt' :: String -> Aeson.Options
toQuietSnakeAesonOpt' p = Aeson.defaultOptions
  { Aeson.fieldLabelModifier     = toQuietSnake . fromHumps . withoutPrefix p
  , Aeson.constructorTagModifier = toQuietSnake . fromHumps . withoutPrefix p
  }

fillWithJsonString :: Text -> Aeson.Object -> Text
fillWithJsonString key obj =
  maybe ("No such " <> key <> " key") extractJsonStringDef $ HM.lookup key obj

extractJsonStringDef :: Aeson.Value -> Text
extractJsonStringDef (Aeson.String x) = x
extractJsonStringDef x = "Expecting string value, but got " <> Text.pack (show x)
{-# INLINE extractJsonStringDef #-}

fillWithJsonString' :: Text -> Aeson.Object -> String
fillWithJsonString' key obj =
  let msg = "No such " <> Text.unpack key <> " key"
   in maybe msg extractJsonStringDef' $ HM.lookup key obj

extractJsonStringDef' :: Aeson.Value -> String
extractJsonStringDef' (Aeson.String x) = Text.unpack x
extractJsonStringDef' x = "Expecting string value, but got " <> show x
{-# INLINE extractJsonStringDef' #-}

--------------------------------------------------------------------------------

instance FromJSON CBytes where
  parseJSON v = let pText = parseJSON v in textToCBytes <$> pText

instance ToJSON CBytes where
  toJSON cb = toJSON (cBytesToText cb)

instance FromJSONKey CBytes
instance ToJSONKey CBytes
