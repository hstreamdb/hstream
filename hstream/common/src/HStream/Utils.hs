{-# LANGUAGE OverloadedStrings #-}

module HStream.Utils
  ( module HStream.Utils.Converter
  , module HStream.Utils.Format
  , getKeyWordFromException
  ) where

import           Control.Exception                 (Exception (..))
import qualified Data.Text.Lazy                    as TL

import           HStream.Utils.Converter
import           HStream.Utils.Format
import           ThirdParty.Google.Protobuf.Struct

getKeyWordFromException :: Exception a => a -> TL.Text
getKeyWordFromException =  TL.pack . takeWhile (/='{') . show
