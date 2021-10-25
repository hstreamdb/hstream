module HStream.Utils.TimeInterval (
    Interval (..)
  , parserInterval
  ,interval2ms
  ) where

import           Control.Applicative  ((<|>))
import           Data.Attoparsec.Text (Parser, choice, parseOnly, rational,
                                       string)
import qualified Data.Text            as T

data Interval
  = Milliseconds Double
  | Seconds Double
  | Minutes Double
  | Hours Double

instance Show Interval where
  show (Seconds x)      = showInInt x <> "s"
  show (Minutes x)      = showInInt x <> "min"
  show (Hours x)        = showInInt x <> "hr"
  show (Milliseconds x) = showInInt x <> "ms"

showInInt :: Double -> String
showInInt x | fromIntegral x' == x = show x'
            | otherwise            = show x
  where
    x' = floor x :: Int

interval2ms :: Interval -> Int
interval2ms (Milliseconds x) = round x
interval2ms (Seconds x)      = round (x * 1000)
interval2ms (Minutes x)      = round (x * 1000 * 60)
interval2ms (Hours x)        = round (x * 1000 * 60 * 60)

intervalParser :: Parser Interval
intervalParser = do
  x <- rational
  f <- intervalConstructorParser
  return (f x)

intervalConstructorParser :: Parser (Double -> Interval)
intervalConstructorParser =
      Milliseconds <$ choice (string <$> ["ms","milliseconds","millisecond"])
  <|> Seconds <$ choice (string <$> ["seconds","s","second"])
  <|> Minutes <$ choice (string <$> ["minutes","min","minute"])
  <|> Hours   <$ choice (string <$> ["hours","h","hr","hrs","hour"])

parserInterval :: String -> Either String Interval
parserInterval = parseOnly intervalParser . T.pack
