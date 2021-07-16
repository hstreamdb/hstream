module HStream.Store.Admin.Format
  ( simpleShowTable
  , handleStoreError
  ) where

import qualified Control.Exception               as E
import           Data.Default                    (def)
import qualified HStream.Store                   as S
import qualified Options.Applicative.Help.Pretty as P
import qualified Text.Layout.Table               as Table
import qualified Z.Data.Text                     as T

simpleShowTable :: [(String, Int, Table.Position Table.H)] -> [[String]] -> String
simpleShowTable _ [] = ""
simpleShowTable colconfs rols =
  let titles = map (\(t, _, _) -> t) colconfs
      colout = map (\(_, maxlen, pos) -> Table.column (Table.expandUntil maxlen) pos def def) colconfs
   in Table.tableString colout
                        Table.asciiS
                        (Table.titlesH titles)
                        [ Table.rowsG rols ]

handleStoreError :: IO () -> IO ()
handleStoreError action =
  let putErr = P.putDoc . P.red . P.string . (\s -> "Error: " <> s <> "\n") .  T.toString . S.sseDescription
   in action `E.catches` [ E.Handler (\(S.StoreError ex) -> putErr ex)
                         , E.Handler (\(ex :: S.SomeHStoreException) -> print ex)
                         ]
