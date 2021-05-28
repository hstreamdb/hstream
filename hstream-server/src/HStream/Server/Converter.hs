module HStream.Server.Converter 
where

import           Data.Aeson
import qualified Data.HashMap.Strict                             as HM
import  Data.Text

delimit :: Text
delimit = pack "."

flattern :: HM.HashMap Text Value -> HM.HashMap Text Value
flattern jsonMap = do
        -- jsonMap
        let inner = HM.toList jsonMap
        -- all the key will add `_` prefix
        let flatterned = ((fmap (flattern' empty) inner) >>= id)
        let flatterned_ = fmap (\(k,v)->(Data.Text.tail k, v)) flatterned
        HM.fromList flatterned_
    where
        flattern' :: Text -> (Text, Value) -> [(Text, Value)]
        flattern' prefix (k, v) = do
            -- TODO: we will not support array?
            case v of
                Object o -> (fmap (flattern' (prefix `append` delimit `append` k)) (HM.toList o)) >>= id
                otherwise -> [((prefix `append` delimit `append` k), v)]


