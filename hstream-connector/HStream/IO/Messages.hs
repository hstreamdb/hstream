module HStream.IO.Messages where

import qualified Data.Aeson                 as J
import qualified Data.Text                  as T


data InputCommand
  = InputCommandStop
  deriving (Show, Eq)

instance J.ToJSON InputCommand where
  toJSON InputCommandStop = J.object [("type" :: T.Text) J..= ("stop" :: T.Text)]
