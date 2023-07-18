{-# LANGUAGE CPP               #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE StrictData        #-}

module HStream.SQL.Codegen.ColumnCatalogNew where

#ifdef HStreamEnableSchema
import qualified Data.HashMap.Strict       as HM
import qualified Data.List                 as L
import           Data.Text                 (Text)

import           HStream.SQL.Binder.Common
import           HStream.SQL.Rts

alwaysTrueJoinCond :: FlowObject -> FlowObject -> Bool
alwaysTrueJoinCond _ _ = True

getField :: (Int,Int) -> FlowObject -> Maybe (ColumnCatalog, FlowValue)
getField (si,ci) o =
  let filterCond = \ColumnCatalog{..} _ ->
                     columnStreamId == si && columnId == ci
   in case HM.toList (HM.filterWithKey filterCond o) of
        [(k,v)] -> Just (k, v)
        xs      -> Nothing

getField' :: (Int,Int) -> FlowObject -> (ColumnCatalog, FlowValue)
getField' (si,ci) o =
  case getField (si,ci) o of
    Nothing     ->
      let catalog = ColumnCatalog { columnId = ci
                                  , columnName = "NULL"
                                  , columnStreamId = si
                                  , columnStream = "" -- FIXME
                                  , columnType = BTypeInteger -- FIXME
                                  , columnIsNullable = True
                                  , columnIsHidden = False
                                  }
       in (catalog, FlowNull)
    Just (k, v) -> (k, v)
#endif
