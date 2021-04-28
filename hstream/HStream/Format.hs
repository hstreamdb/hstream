module HStream.Format where

import qualified Data.Aeson          as A
import qualified Data.Aeson.Text     as A
import qualified Data.HashMap.Strict as HM
import qualified Data.Text           as T
import qualified Data.Text.Lazy      as TL

type Width = Int
type Box = (Width, String)
type Row = ([Width], [String])
type Table = ([Width], [Row])

vBorder :: [Width] -> String
vBorder = foldr ((++) . (:) '+' . flip replicate '-' . (+ 2)) "+"

box :: String -> Box
box str = (length str, str)

emptyRow :: ([Width], [String])
emptyRow = ([],[])

emptyTable :: ([Width], [Row])
emptyTable = (repeat 0, [])

addBox :: Row -> Box -> Row
addBox (ws, ss) (w, s)= (w : ws, s : ss)

addRow :: Table -> Row -> Table
addRow (tws, rs) (rws, ss) = let ws' = zipWith max rws tws in (ws', (ws', ss) : rs)

renderBox :: Box -> String
renderBox (w, s) = ' ' : s ++ replicate (w + 1 - length s) ' ' ++ "|"

renderRow :: Row -> String
renderRow (ws, ss) = '|' : (concatMap renderBox . zip ws) ss

renderTable :: Table -> [String]
renderTable (ws, rs) = vBorder ws : renderTable' rs ++ [vBorder ws]
  where
    renderTable' ((_, ss) : rs') = renderRow (ws, ss) : renderTable' rs'
    renderTable' []              = []

renderJSONObjectToTable :: A.Object -> Table
renderJSONObjectToTable hmap =
  emptyTable `addRow` foldr (flip addBox . box . TL.unpack . A.encodeToLazyText) emptyRow elems
             `addRow` foldr (flip addBox . box . T.unpack) emptyRow keys
  where
    keys  = HM.keys hmap
    elems = HM.elems hmap

renderJSONToTable :: A.Value -> Table
renderJSONToTable (A.Object hmap) = renderJSONObjectToTable hmap
renderJSONToTable x = emptyTable `addRow` (emptyRow `addBox` (box . show) x)
