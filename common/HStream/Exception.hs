{-# LANGUAGE CPP   #-}
{-# LANGUAGE GADTs #-}
{-# OPTIONS_GHC -pgmPcpphs -optP--cpp #-}

module HStream.Exception
  ( SomeHStreamException

    -- * Encode & Decode Exception
  , HStreamEncodingException
  , DecodeHStreamRecordErr (..)
  , NoRecordHeader (..)
  , UnknownCompressionType (..)
  , ZstdCompresstionErr (..)
  ) where

import           Control.Exception (Exception (..))
import qualified Control.Exception as E
import           Data.Typeable     (cast)

-------------------------------------------------------------------------------
-- The root exception type for all the exceptions in a hstream server

data SomeHStreamException = forall e . Exception e => SomeHStreamException e

instance Show SomeHStreamException where
  show (SomeHStreamException e) = show e

instance Exception SomeHStreamException

-- The format of this function's name is important because of the macro.
--
-- Format: "a <> ExceptionName(SomeHStreamException) <> ToException"
aSomeHStreamExceptionToException :: Exception e => e -> E.SomeException
aSomeHStreamExceptionToException = E.toException . SomeHStreamException

-- Format: "a <> ExceptionName(SomeHStreamException) <> FromException"
aSomeHStreamExceptionFromException :: Exception e => E.SomeException -> Maybe e
aSomeHStreamExceptionFromException x = do
  SomeHStreamException a <- E.fromException x
  cast a

#define MAKE_SUB_EX(BASE_E, SUB_E) \
data SUB_E = forall e . Exception e => SUB_E e;                                \
instance Show SUB_E where                                                      \
{ show (SUB_E e) = show e };                                                   \
instance Exception SUB_E where                                                 \
{ toException = a##BASE_E##ToException;                                        \
  fromException = a##BASE_E##FromException;                                    \
};                                                                             \
a##SUB_E##ToException :: Exception e => e -> E.SomeException;                  \
a##SUB_E##ToException = E.toException . SUB_E;                                 \
a##SUB_E##FromException :: Exception e => E.SomeException -> Maybe e;          \
a##SUB_E##FromException x = do SUB_E a <- E.fromException x; cast a;

#define MAKE_PARTICULAR_EX(ExCls, Name, Display) \
data Name = Name deriving (Show);                                              \
instance Exception Name where                                                  \
{ toException = a##ExCls##ToException;                                         \
  fromException = a##ExCls##FromException;                                     \
  displayException Name = Display;                                             \
};                                                                             \

#define MAKE_PARTICULAR_EX_1(ExCls, Name, Ty, ToString) \
newtype Name = Name Ty deriving (Show);                                        \
instance Exception Name where                                                  \
{ toException = a##ExCls##ToException;                                         \
  fromException = a##ExCls##FromException;                                     \
  displayException (Name s) = ToString s;                                      \
};                                                                             \

-------------------------------------------------------------------------------
-- Sub exception: Encoding & Decoding

MAKE_SUB_EX(SomeHStreamException, HStreamEncodingException)

MAKE_PARTICULAR_EX_1(HStreamEncodingException, DecodeHStreamRecordErr, String, )
MAKE_PARTICULAR_EX(HStreamEncodingException, NoRecordHeader, "HStreamRecord doesn't have a header.")
MAKE_PARTICULAR_EX(HStreamEncodingException, UnknownCompressionType, "UnknownCompressionType")
MAKE_PARTICULAR_EX_1(HStreamEncodingException, ZstdCompresstionErr, String, )
