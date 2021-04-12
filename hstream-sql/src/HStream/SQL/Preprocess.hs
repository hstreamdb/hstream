{-# LANGUAGE OverloadedStrings #-}

module HStream.SQL.Preprocess
  ( preprocess
  ) where

import           Data.Text (Text, append, breakOn)
import qualified Data.Text as Text

commentStart :: Text
commentStart = "/*"

commentEnd :: Text
commentEnd = "*/"

removeComments :: Text -> Text
removeComments "" = ""
removeComments text = prefix `append` removeComments remained'
  where (prefix,match) = breakOn commentStart text
        remained       = Text.drop (Text.length commentStart) match
        (_,match')     = breakOn commentEnd remained
        remained'      = Text.drop (Text.length commentEnd) match'

preprocess :: Text -> Text
preprocess = removeComments
