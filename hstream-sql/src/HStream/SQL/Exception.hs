{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TypeApplications  #-}

module HStream.SQL.Exception
  ( Position
  , SomeSQLException (..)
  , SomeSQLExceptionInfo (..)
  , SomeRuntimeException (..)
  , buildSQLException
  , throwSQLException
  , throwRuntimeException
  , formatSomeSQLException
  , isEOF
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
  BindException    :: HasCallStack => SomeSQLExceptionInfo -> SomeSQLException
  PlanException    :: HasCallStack => SomeSQLExceptionInfo -> SomeSQLException
  RefineException  :: HasCallStack => SomeSQLExceptionInfo -> SomeSQLException
  CodegenException :: HasCallStack => SomeSQLExceptionInfo -> SomeSQLException

instance Show SomeSQLException where
  show (ParseException info)   = "ParseException at "   ++ show info ++ "\n" ++ prettyCallStack callStack
  show (BindException    info) = "BindException at "    ++ show info ++ "\n" ++ prettyCallStack callStack
  show (PlanException    info) = "PlanException at "    ++ show info ++ "\n" ++ prettyCallStack callStack
  show (RefineException info)  = "RefineException at "  ++ show info ++ "\n" ++ prettyCallStack callStack
  show (CodegenException info) = "CodegenException at " ++ show info ++ "\n" ++ prettyCallStack callStack

instance Exception SomeSQLException

formatSomeSQLException :: SomeSQLException -> String
formatSomeSQLException (ParseException   info) = "Parse exception " ++ formatParseExceptionInfo info
formatSomeSQLException (BindException    info) = "Bind exception at " ++ show info
formatSomeSQLException (PlanException    info) = "Plan exception at " ++ show info
formatSomeSQLException (RefineException  info) = "Refine exception at " ++ show info
formatSomeSQLException (CodegenException info) = "Codegen exception at " ++ show info

formatParseExceptionInfo :: SomeSQLExceptionInfo -> String
formatParseExceptionInfo SomeSQLExceptionInfo{..} =
  case words sqlExceptionMessage of
    "syntax" : "error" : "at" : "line" : x : "column" : y : ss ->
      "at <line " ++ x ++ "column " ++ y ++ ">: syntax error " ++ unwords ss ++ "."
    _ ->
      let detailedSqlExceptionMessage = if sqlExceptionMessage /= eofErrMsg
            then sqlExceptionMessage
            else sqlExceptionMessage <> ": expected a \";\" at the end of a statement"
      in posInfo ++ detailedSqlExceptionMessage ++ "."
  where
    posInfo = case sqlExceptionPosition of
        Just (l,c) -> "at <line " ++ show l ++ ", column " ++ show c ++ ">"
        Nothing    -> ""

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

isEOF :: SomeSQLException -> Bool
isEOF xs =
  case xs of
    ParseException info ->
      let SomeSQLExceptionInfo _ msg _ = info in
        msg == eofErrMsg
    _ -> False

eofErrMsg :: String
eofErrMsg = "syntax error at end of file"

--------------------------------------------------------------------------------
data SomeRuntimeException = SomeRuntimeException
  { runtimeExceptionMessage   :: String
  , runtimeExceptionCallStack :: CallStack
  }
instance Show SomeRuntimeException where
  show SomeRuntimeException{..} = runtimeExceptionMessage
instance Exception SomeRuntimeException

throwRuntimeException :: String -> a
throwRuntimeException msg =
  throw $ SomeRuntimeException { runtimeExceptionMessage = msg
                               , runtimeExceptionCallStack = callStack
                               }
