{-# LANGUAGE CPP #-}

#if __GLASGOW_HASKELL__ >= 708
---------------------------------------------------------------------------
-- Pattern synonyms exist since ghc 7.8.

-- | BNF Converter: Error Monad.
--
-- Module for backwards compatibility.
--
-- The generated parser now uses @'Either' String@ as error monad.
-- This module defines a type synonym 'Err' and pattern synonyms
-- 'Bad' and 'Ok' for 'Left' and 'Right'.

{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeSynonymInstances #-}

module HStream.SQL.ErrM where

import Control.Monad       (MonadPlus(..))
import Control.Applicative (Alternative(..))

-- | Error monad with 'String' error messages.
type Err = Either String

pattern Bad msg = Left msg
pattern Ok  a   = Right a

#if __GLASGOW_HASKELL__ >= 808
instance MonadFail Err where
  fail = Bad
#endif

instance Alternative Err where
  empty           = Left "Err.empty"
  (<|>) Left{}    = id
  (<|>) x@Right{} = const x

instance MonadPlus Err where
  mzero = empty
  mplus = (<|>)

#else
---------------------------------------------------------------------------
-- ghc 7.6 and before: use old definition as data type.

-- | BNF Converter: Error Monad

-- Copyright (C) 2004  Author:  Aarne Ranta
-- This file comes with NO WARRANTY and may be used FOR ANY PURPOSE.

module HStream.SQL.ErrM where

-- the Error monad: like Maybe type with error msgs

import Control.Applicative (Applicative(..), Alternative(..))
import Control.Monad       (MonadPlus(..), liftM)

data Err a = Ok a | Bad String
  deriving (Read, Show, Eq, Ord)

instance Monad Err where
  return      = Ok
  Ok a  >>= f = f a
  Bad s >>= _ = Bad s

instance Applicative Err where
  pure = Ok
  (Bad s) <*> _ = Bad s
  (Ok f) <*> o  = liftM f o

instance Functor Err where
  fmap = liftM

instance MonadPlus Err where
  mzero = Bad "Err.mzero"
  mplus (Bad _) y = y
  mplus x       _ = x

instance Alternative Err where
  empty = mzero
  (<|>) = mplus

#endif
