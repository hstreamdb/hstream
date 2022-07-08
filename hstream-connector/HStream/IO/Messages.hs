{-# LANGUAGE TemplateHaskell #-}

module HStream.IO.Messages where

import qualified Data.Aeson                 as J
import qualified Data.Text                  as T
import qualified Data.Aeson.TH              as JT


data InputCommand
  = InputCommandStop
  deriving (Show, Eq)

instance J.ToJSON InputCommand where
  toJSON InputCommandStop = J.object [("type" :: T.Text) J..= ("stop" :: T.Text)]

data CheckResult
  = CheckResult
    { result :: Bool
    , message :: T.Text
    }

$(JT.deriveJSON JT.defaultOptions ''CheckResult)
