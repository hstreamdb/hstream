{-# LANGUAGE FlexibleContexts #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module HStream.Instances () where

import qualified Text.Read                 as Read

import qualified HStream.Server.HStreamApi as API

-------------------------------------------------------------------------------
-- Orphan instances

instance Read API.SpecialOffset where
  readPrec = do
    i <- Read.lexP
    case i of
        Read.Ident "earliest" -> return API.SpecialOffsetEARLIEST
        Read.Ident "latest"  -> return API.SpecialOffsetLATEST
        x -> errorWithoutStackTrace $ "cannot parse value: " <> show x
