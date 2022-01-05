{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Utils.Table
  ( splitTable
  , defaultShowTable
  , defaultShowTableIO
  , defaultShowTableIO'
  , simpleShowTable
  ) where

import           Data.Default           (def)
import           System.Console.ANSI    (getTerminalSize)
import qualified Text.Layout.Table      as Table
import qualified Text.Layout.Table.Cell as Table

-- (Title, List of value)
type Column a = (String, Table.Col a)
type Columns a = [Column a]

splitTable :: forall a. (Columns a -> Bool) -> Columns a -> [Columns a]
splitTable isRowFill = f []
  where
    f :: [Columns a] -> [Column a] -> [Columns a]
    f [] [] = []
    f acc [] = reverse $ map reverse acc
    f [] (x:rs)
      | isRowFill [x] = f [[x]] rs
      | otherwise = f [[x]] rs
    f acc (x:xs)
      | isRowFill [x] = f ([x] : acc) xs
      | otherwise = if null acc
                       then f [[x]] xs
                       else if isRowFill (x : head acc)
                               then f ([x] : acc) xs
                               else f ((x : head acc) : tail acc) xs

defaultShowTable :: (Table.Cell a, Monoid a) => Int -> Columns a -> String
defaultShowTable maxWidth cell =
  Table.concatLines . map (Table.concatLines . consTable)
                    $ splitTable isRowFill cell
  where
    isRowFill = (>= maxWidth) . length . head . consTable

    consTable a_cell =
      let (titles, cols) = unzip a_cell
          rowGroup = Table.colsAllG Table.top cols
       in Table.tableLines (def <$ cols) Table.asciiS (Table.titlesH titles) [rowGroup]

defaultShowTableIO :: (Table.Cell a, Monoid a) => Columns a -> IO String
defaultShowTableIO cell = termialWidth >>= \w -> pure $ defaultShowTable w cell

defaultShowTableIO' :: (Table.Cell a, Monoid a)
                    => [String] -> [Table.Row a] -> IO String
defaultShowTableIO' titles rows =
  -- Note: here 'Table.colsAsRowsAll' is actually a 'rowAsColsAll'
  defaultShowTableIO $ zip titles (Table.colsAsRowsAll def rows)

termialWidth :: IO Int
termialWidth = maybe 80 snd <$> getTerminalSize

simpleShowTable :: [(String, Int, Table.Position Table.H)] -> [[String]] -> String
simpleShowTable _ [] = ""
simpleShowTable colconfs rols =
  let titles = map (\(t, _, _) -> t) colconfs
      colout = map (\(_, maxlen, pos) -> Table.column (Table.expandUntil maxlen) pos def def) colconfs
   in Table.tableString colout
                        Table.asciiS
                        (Table.titlesH titles)
                        [ Table.rowsG rols ]
