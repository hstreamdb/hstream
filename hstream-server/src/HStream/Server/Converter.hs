module HStream.Server.Converter
where

import           Data.Aeson
import qualified Data.HashMap.Strict as HM
import           Data.Text

delimit :: Text
delimit = pack "."

flatten :: HM.HashMap Text Value -> HM.HashMap Text Value
flatten jsonMap = do
        -- jsonMap
        let inner = HM.toList jsonMap
        -- all the key will add `_` prefix
        let flattened = ((fmap (flatten' empty) inner) >>= id)
        let flattened_ = fmap (\(k,v)->(Data.Text.tail k, v)) flattened
        HM.fromList flattened_
    where
        flatten' :: Text -> (Text, Value) -> [(Text, Value)]
        flatten' prefix (k, v) = do
            -- TODO: we will not support array?
            case v of
                Object o -> (fmap (flatten' (prefix `append` delimit `append` k)) (HM.toList o)) >>= id
                otherwise -> [((prefix `append` delimit `append` k), v)]
