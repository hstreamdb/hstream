module HStream.Utils
  ( bs2str
  , str2bs
  , textShow
  , encodeUtf8
  , (.|.)
  ) where

import           Data.ByteString    (ByteString)
import           Data.Text          (Text)
import qualified Data.Text          as Text
import qualified Data.Text.Encoding as Text

bs2str :: ByteString -> String
bs2str = Text.unpack . Text.decodeUtf8

str2bs :: String -> ByteString
str2bs = Text.encodeUtf8 . Text.pack

textShow :: Show a => a -> Text
textShow = Text.pack . show

encodeUtf8 :: Show a => a -> ByteString
encodeUtf8 = Text.encodeUtf8 . textShow

-- Note: @(.|.) = liftA2 (<|>)@ can get the same result, but it will
-- perform all @m (Maybe a)@ and then return the first "Just value".
(.|.) :: Monad m => m (Maybe a) -> m (Maybe a) -> m (Maybe a)
ma .|. mb = maybe mb (return . Just) =<< ma
