module HStream.Connector.Converter
where

import           Control.Monad       (join)
import           Data.Aeson          as Aeson
import           Data.Bifunctor      (first)
import qualified Data.HashMap.Strict as HM
import           Data.Text           (Text)
import qualified Data.Text           as Text

-- | Flatten all JSON structures.
--
-- >>> flatten (HM.fromList [("a", Aeson.Object $ HM.fromList [("b", Aeson.Number 1)])])
-- fromList [("a.b",Number 1.0)]
flatten :: HM.HashMap Text Aeson.Value -> HM.HashMap Text Aeson.Value
flatten jsonMap =
  let flattened = join $ map (flatten' "." Text.empty) (HM.toList jsonMap)
   in HM.fromList $ map (first Text.tail) flattened

flatten' :: Text -> Text -> (Text, Aeson.Value) -> [(Text, Aeson.Value)]
flatten' splitor prefix (k, v) = do
  -- TODO: we will not support array?
  case v of
    Aeson.Object o -> join $ map (flatten' splitor (prefix <> splitor <> k)) (HM.toList o)
    _              -> [(prefix <> splitor <> k, v)]
