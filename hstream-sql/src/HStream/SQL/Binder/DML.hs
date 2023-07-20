{-# LANGUAGE DeriveAnyClass    #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies      #-}

module HStream.SQL.Binder.DML where

import qualified Data.Aeson                   as Aeson
import qualified Data.ByteString              as BS
import           Data.Text                    (Text)
import           Data.Text.Encoding           (decodeUtf8, encodeUtf8)
import           GHC.Generics                 (Generic)

import           HStream.SQL.Abs
import           HStream.SQL.Binder.Basic
import           HStream.SQL.Binder.Common
import           HStream.SQL.Binder.Select
import           HStream.SQL.Binder.ValueExpr
import           HStream.SQL.Extra

-- Insert
data BoundInsertPayloadType = BoundInsertPayloadTypeRaw
                            | BoundInsertPayloadTypeJson
  deriving (Eq, Show, Generic, Aeson.ToJSON, Aeson.FromJSON)

data BoundInsert
  = BoundInsertKVs Text [(Text,Constant)]
  | BoundInsertRawOrJson Text BS.ByteString BoundInsertPayloadType
  deriving (Eq, Show, Generic, Aeson.ToJSON, Aeson.FromJSON)

type instance BoundType Insert = BoundInsert
instance Bind Insert where
  bind (DInsert _ s fields exprs) = do
    s'      <- bind s
    fields' <- mapM bind      fields
    exprs'  <- mapM bindConst exprs
    return $ BoundInsertKVs s' (zip fields' exprs')
    where
      bindConst expr = do
        expr' <- bind expr
        case expr' of
          BoundExprConst _ c -> return c
          _                  -> error $ "INTERNAL ERROR: constant expr in DInsert is ensured by validate"
  bind (InsertRawOrJson _ s valExprCast) = do
    let (val, typ, _) = unifyValueExprCast valExprCast
        errMsg        = "INTERNAL ERROR: Insert RawRecord or HRecord syntax only supports string literals to be casted to `BYTEA` or `JSONB`, which is ensured by validate"
        rTyp          = case typ of
                          TypeByte _ -> BoundInsertPayloadTypeRaw
                          TypeJson _ -> BoundInsertPayloadTypeJson
                          _          -> error errMsg
    rVal <- case val of
              ExprString _ singleQuoted@(SingleQuoted _) -> bind singleQuoted
              _                                          -> error errMsg
    return $ BoundInsertRawOrJson (extractHIdent s) (encodeUtf8 rVal) rTyp

-- Explain
type BoundExplain = BoundSelect

type instance BoundType Explain = BoundExplain
instance Bind Explain where
  bind (ExplainSelect _ select)                    = bind select
  bind (ExplainCreate _ (CreateAs _ _ select))     = bind select
  bind (ExplainCreate _ (CreateAsOp _ _ select _)) = bind select
  bind (ExplainCreate _ (CreateView _ _ select))   = bind select
  bind (ExplainCreate pos _)                       = throwImpossible

-- Show
data BoundShow = BoundShow BoundShowOption
  deriving (Eq, Show, Generic, Aeson.ToJSON, Aeson.FromJSON)

type instance BoundType ShowQ = BoundShow
instance Bind ShowQ where
  bind (DShow _ showOp) = BoundShow <$> bind showOp

data BoundShowOption
  = BoundShowStreams
  | BoundShowQueries
  | BoundShowConnectors
  | BoundShowViews
  deriving (Eq, Show, Generic, Aeson.ToJSON, Aeson.FromJSON)

type instance BoundType ShowOption = BoundShowOption
instance Bind ShowOption where
  bind (ShowStreams _)    = return BoundShowStreams
  bind (ShowQueries _)    = return BoundShowQueries
  bind (ShowConnectors _) = return BoundShowConnectors
  bind (ShowViews _)      = return BoundShowViews

-- Terminate
data BoundTerminate
  = BoundTerminateQuery Text
  deriving (Eq, Show, Generic, Aeson.ToJSON, Aeson.FromJSON)

type instance BoundType Terminate = BoundTerminate
instance Bind Terminate where
  bind (TerminateQuery _ x) = BoundTerminateQuery <$> bind x

-- Pause
data BoundPause
  = BoundPauseConnector Text
  | BoundPauseQuery     Text
  deriving (Eq, Show, Generic, Aeson.ToJSON, Aeson.FromJSON)

type instance BoundType Pause = BoundPause
instance Bind Pause where
  bind (PauseConnector _ name) = BoundPauseConnector <$> bind name
  bind (PauseQuery _ name)     = BoundPauseQuery     <$> bind name

-- Resume
data BoundResume
  = BoundResumeConnector Text
  | BoundResumeQuery     Text
  deriving (Eq, Show, Generic, Aeson.ToJSON, Aeson.FromJSON)

type instance BoundType Resume = BoundResume
instance Bind Resume where
  bind (ResumeConnector _ name) = BoundResumeConnector <$> bind name
  bind (ResumeQuery _ name)     = BoundResumeQuery     <$> bind name
