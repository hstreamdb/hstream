{-# LANGUAGE OverloadedStrings #-}

module HStream.SQL.Preprocess
  ( preprocess
  ) where

import           Data.Text (Text, append, breakOn, toUpper)
import qualified Data.Text as Text

keywords :: [Text]
keywords =
  [ "SELECT", "AS", "FROM", "WHERE", "OR", "AND", "NOT", "BETWEEN"
  , "EMIT", "CHANGES"
  , "JOIN", "INNER", "LEFT", "OUTER"
  , "GROUP", "BY", "WITHIN", "ON", "ORDER", "HAVING"
  , "TUMBLING", "HOPPING", "SESSION"
  , "CREATE", "STREAM", "WITH", "FORMAT"
  , "DATE", "TIME", "YEAR", "MONTH", "WEEK", "DAY", "MINUTE", "SECOND", "INTERVAL"
  , "COUNT(*)", "COUNT", "AVG", "SUM", "MAX", "MIN"
  , "INSERT", "INTO", "VALUES"
  ]

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
preprocess text = Text.unwords $ helper <$> Text.words (removeComments text)
  where helper word = if toUpper word `elem` keywords
                      then toUpper word
                      else word
