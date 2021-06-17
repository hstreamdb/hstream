{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.SQL.Exception
  ( Position
  , SomeSQLException (..)
  , SomeSQLExceptionInfo (..)
  , SomeRuntimeException (..)
  , buildSQLException
  , throwSQLException
  ) where

import           Control.Exception (Exception, throw)
import           GHC.Stack         (CallStack, HasCallStack, callStack,
                                    prettyCallStack)
import           HStream.SQL.Abs   (BNFC'Position)

--------------------------------------------------------------------------------
-- | Position in a SQL input text. 'Nothing' means that the position information
-- has been erased.
type Position = BNFC'Position

--------------------------------------------------------------------------------

-- | The root type of all SQL exceptions, you can catch some SQL exception
-- by catching this root type.
data SomeSQLException where
  ParseException   :: HasCallStack => SomeSQLExceptionInfo -> SomeSQLException
  RefineException  :: HasCallStack => SomeSQLExceptionInfo -> SomeSQLException
  CodegenException :: HasCallStack => SomeSQLExceptionInfo -> SomeSQLException

instance Show SomeSQLException where
  show (ParseException info)   = "ParseException at "   ++ show info ++ "\n" ++ prettyCallStack callStack
  show (RefineException info)  = "RefineException at "  ++ show info ++ "\n" ++ prettyCallStack callStack
  show (CodegenException info) = "CodegenException at " ++ show info ++ "\n" ++ prettyCallStack callStack

instance Exception SomeSQLException

-- | SomeSQLException information.
data SomeSQLExceptionInfo = SomeSQLExceptionInfo
  { sqlExceptionPosition  :: Position  -- ^ position of SQL input text where exception was thrown
  , sqlExceptionMessage   :: String    -- ^ description for this exception
  , sqlExceptionCallStack :: CallStack -- ^ lightweight partial callstack
  }

instance Show SomeSQLExceptionInfo where
  show SomeSQLExceptionInfo{..} =
    let posInfo = case sqlExceptionPosition of
                Nothing    -> "<unknown position>"
                Just (l,c) -> "<line " ++ show l ++ ", column " ++ show c ++ ">"
     in posInfo ++ ": " ++ sqlExceptionMessage

-- | Build an SQL exception from its type, position and description. It won't be thrown.
buildSQLException :: (SomeSQLExceptionInfo -> SomeSQLException)
                  -> Position
                  -> String
                  -> SomeSQLException
buildSQLException exceptionType exceptionPos exceptionMsg =
  exceptionType (SomeSQLExceptionInfo exceptionPos exceptionMsg callStack)

-- | Build an SQL exception from its type, position and description then throw it.
throwSQLException :: (SomeSQLExceptionInfo -> SomeSQLException)
                  -> Position
                  -> String
                  -> a
throwSQLException exceptionType exceptionPos exceptionMsg =
  throw $ buildSQLException exceptionType exceptionPos exceptionMsg

--------------------------------------------------------------------------------
data SomeRuntimeException = SomeRuntimeException
  { runtimeExceptionMessage   :: String
  , runtimeExceptionCallStack :: CallStack
  }
instance Show SomeRuntimeException where
  show SomeRuntimeException{..} = runtimeExceptionMessage
instance Exception SomeRuntimeException
