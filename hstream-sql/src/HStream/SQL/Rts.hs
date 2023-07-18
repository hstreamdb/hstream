{-# LANGUAGE CPP #-}

module HStream.SQL.Rts (
  undefined -- FIXME: Only used to make stylish-haskell work
#ifdef HStreamEnableSchema
  , module HStream.SQL.Rts.New
#else
  , module HStream.SQL.Rts.Old
#endif
   ) where

#ifdef HStreamEnableSchema
import           HStream.SQL.Rts.New
#else
import           HStream.SQL.Rts.Old
#endif
