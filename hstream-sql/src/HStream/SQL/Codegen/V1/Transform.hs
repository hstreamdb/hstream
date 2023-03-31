{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE StrictData        #-}

module HStream.SQL.Codegen.V1.Transform where

import qualified Data.List                          as L
import           HStream.SQL.AST
import           HStream.SQL.Codegen.V1.Boilerplate (winEndText, winStartText)

-- This function can only be applied to queries on views(select from view).
addGroupByKeysToProjectItems :: RSelect -> (RSelect, [ColumnCatalog], [ColumnCatalog])
addGroupByKeysToProjectItems select@(RSelect (RSel selItems) frm whr grp hav) =
  case grp of
    RGroupByEmpty -> (select, [], [])
    RGroupBy _ (Just _) -> error "Queries on views are not allowed to have window functions."
    RGroupBy keyNames Nothing ->
      -- error "unsupported"

      -- lookup the key in select items,
      -- if found, do nothing
      -- else, add it to the items.
      -- let keysToAdd = L.filter (notExistsInSelItems selItems) keyNames ++ [(Nothing, winStartText), (Nothing, winEndText)]
      let keysToAdd = L.filter (notExistsInSelItems selItems) keyNames
       in (RSelect (RSel (newSelItems keysToAdd)) frm whr grp hav, transKeyNames keyNames, toColumnCatalogs keysToAdd)
  where
    toColumnCatalogs :: [(Maybe StreamName, FieldName)] -> [ColumnCatalog]
    toColumnCatalogs = L.map toColumnCatalog

    toColumnCatalog :: (Maybe StreamName, FieldName) -> ColumnCatalog
    toColumnCatalog (s, f) = ColumnCatalog {columnName = f, columnStream = s}

    -- RSelectItemProject RValueExpr (Maybe SelectItemAlias)
    newSelItems :: [(Maybe StreamName, FieldName)] -> [RSelectItem]
    newSelItems cols =
      let items =
            L.map
              (\col@(sn, fn) ->
                  RSelectItemProject (RExprCol (show (toColumnCatalog col)) sn fn) Nothing
              )
              cols
       in
          selItems ++ items

    transKeyNames :: [(Maybe StreamName, FieldName)] -> [ColumnCatalog]
    transKeyNames =
      L.foldl'
        ( \acc col ->
            case lookupItems selItems col of
              Just (Just alias) -> acc ++ [toColumnCatalog (Nothing, alias)]
              _                 -> acc ++ [toColumnCatalog col]
        )
        []



    lookupItems :: [RSelectItem] -> (Maybe StreamName, FieldName) -> Maybe (Maybe SelectItemAlias)
    lookupItems items k =
      L.foldl'
        ( \prev it ->
            case prev of
              Nothing ->
                  case it of
                    RSelectItemProject (RExprCol _ sn fn) alias -> if (sn, fn) == k then Just alias else Nothing
                    _ -> Nothing
              Just _ -> prev
        )
        Nothing
        items


    notExistsInSelItems :: [RSelectItem] -> (Maybe StreamName, FieldName) -> Bool
    notExistsInSelItems items k =
      L.foldl'
        ( \ne it ->
            if ne
              then case it of
                RSelectItemProject (RExprCol _ sn fn) _ -> (sn, fn) /= k
                _                                       -> False
              else False
        )
        True
        items
