{-# LANGUAGE CPP #-}

module HStream.Utils.Aeson
  ( -- * Key
    Key
  , fromText
  , toText

    -- * KeyMap
  , KeyMap
  , fromHashMap
  , toHashMap
  , fromHashMapText
  , toHashMapText
  , fromList
  , toList
  , lookup
  , (!)
  , adjust
  ) where

import qualified Data.HashMap.Strict as HM
import           Prelude             hiding (lookup)

#if MIN_VERSION_aeson(2,0,0)
import           Data.Aeson.Key      (Key, fromText, toText)
import           Data.Aeson.KeyMap   (KeyMap, fromHashMap, fromHashMapText,
                                      fromList, lookup, toHashMap,
                                      toHashMapText, toList)
import qualified Data.Aeson.KeyMap   as A
import           Data.Maybe          (fromJust)
#else
import           Data.HashMap.Strict (HashMap, adjust, (!))
import           Data.Text           (Text)
#endif

-------------------------------------------------------------------------------
#if MIN_VERSION_aeson(2,0,0)
-------------------------------------------------------------------------------

(!) :: KeyMap v -> Key -> v
keymap ! key = fromJust $ A.lookup key keymap

-- TODO: performance improvements
adjust :: (a -> a) -> Key -> KeyMap a -> KeyMap a
adjust f k m = A.fromHashMap $ HM.adjust f k $ A.toHashMap m

-------------------------------------------------------------------------------
#else
-------------------------------------------------------------------------------

type Key = Text
type KeyMap = HashMap Key

fromText :: Text -> Text
fromText = id

toText :: Text -> Text
toText = id

fromHashMap :: HashMap Key v -> KeyMap v
fromHashMap = id

toHashMap :: KeyMap v -> HashMap Key v
toHashMap = id

fromHashMapText :: HashMap Text v -> KeyMap v
fromHashMapText = id

toHashMapText :: KeyMap v -> HashMap Text v
toHashMapText = id

fromList :: [(Key, v)] -> KeyMap v
fromList = HM.fromList

toList :: KeyMap v -> [(Key, v)]
toList = HM.toList

lookup :: Key -> KeyMap v -> Maybe v
lookup = HM.lookup

-------------------------------------------------------------------------------
#endif
-------------------------------------------------------------------------------
