{-# LANGUAGE OverloadedStrings #-}

module Language.SQL.Preprocess
  ( preprocess
  ) where

import Data.Text (Text, breakOn, toUpper, append)
import qualified Data.Text as Text

keywords :: [Text]
keywords =
  [ "SELECT", "AS", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "FULL", "CROSS"
  , "GROUP", "BY", "ORDER", "HAVING"
  , "CREATE", "STREAM", "WITH"
  , "INT", "STRING", "NUMBER"]

commentStart :: Text
commentStart = "/*"

commentEnd :: Text
commentEnd = "*/"

removeComments :: Text -> Text
removeComments "" = ""
removeComments text = prefix `append` removeComments remained'
  where (prefix, match)   = breakOn commentStart text
        remained          = Text.drop (Text.length commentStart) match
        (prefix', match') = breakOn commentEnd remained
        remained'         = Text.drop (Text.length commentEnd) match'

preprocess :: Text -> Text
preprocess text = Text.unwords $ helper <$> Text.words (removeComments text)
  where helper word = if toUpper word `elem` keywords
                      then toUpper word
                      else word
