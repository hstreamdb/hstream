module HStream.Utils.Table
  ( simpleShowTable
  ) where

import           Data.Default      (def)
import qualified Text.Layout.Table as Table

simpleShowTable :: [(String, Int, Table.Position Table.H)] -> [[String]] -> String
simpleShowTable _ [] = ""
simpleShowTable colconfs rols =
  let titles = map (\(t, _, _) -> t) colconfs
      colout = map (\(_, maxlen, pos) -> Table.column (Table.expandUntil maxlen) pos def def) colconfs
   in Table.tableString colout
                        Table.asciiS
                        (Table.titlesH titles)
                        [ Table.rowsG rols ]
