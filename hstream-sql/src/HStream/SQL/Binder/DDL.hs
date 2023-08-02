{-# LANGUAGE DeriveAnyClass    #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies      #-}

module HStream.SQL.Binder.DDL where

import           Control.Monad
import           Control.Monad.Extra          (maybeM)
import           Control.Monad.State
import qualified Data.Aeson                   as Aeson
import           Data.Default
import qualified Data.HashMap.Strict          as HM
import qualified Data.IntMap                  as IntMap
import qualified Data.List                    as L
import           Data.Text                    (Text)
import qualified Data.Text                    as Text
import           Data.Word                    (Word32)
import           GHC.Generics

import           HStream.SQL.Abs
import           HStream.SQL.Binder.Common
import           HStream.SQL.Binder.Select
import           HStream.SQL.Binder.ValueExpr
import           HStream.SQL.Exception
import           HStream.SQL.Extra

-- Create
type instance BoundType (HIdent, [SchemaColumn]) = Schema
instance Bind (HIdent, [SchemaColumn]) where
  bind (hIdent, cols) = do
    sName <- bind hIdent
    tups <-
      mapM (\(i, (DSchemaColumn _ colIdent typ)) -> do
             colName <- bind colIdent
             colType <- bind typ
             let cata = ColumnCatalog
                      { columnId         = i
                      , columnName       = colName
                      , columnStreamId   = 0
                      , columnStream     = sName
                      , columnType       = colType
                      , columnIsNullable = True
                      , columnIsHidden   = False
                      }
             return (i, cata)
         ) ([0..] `zip` cols)
    return $ Schema { schemaOwner   = sName
                    , schemaColumns = IntMap.fromList tups
                    }

data BoundStreamOptions = BoundStreamOptions
  { bRepFactor       :: Int
  , bBacklogDuration :: Word32
  } deriving (Show, Generic, Aeson.ToJSON, Aeson.FromJSON)

instance Default BoundStreamOptions where
  def = BoundStreamOptions
      { bRepFactor       = 1
      , bBacklogDuration = 7 * 24 * 3600
      }

newtype BoundConnectorOptions
  = BoundConnectorOptions (HM.HashMap Text Aeson.Value)
  deriving (Show, Generic, Aeson.ToJSON, Aeson.FromJSON)

data BoundCreate
  = BoundCreate   Text Schema BoundStreamOptions
  | BoundCreateAs Text BoundSelect BoundStreamOptions
    -- BoundCreateConnector <SOURCE|SINK> <Name> <Target> <EXISTS> <OPTIONS>
  | BoundCreateConnector Text Text Text Bool BoundConnectorOptions
  | BoundCreateView Text BoundSelect
  deriving (Show, Generic, Aeson.ToJSON, Aeson.FromJSON)

type instance BoundType [StreamOption] = BoundStreamOptions
instance Bind [StreamOption] where
  bind options = do
    let factor_m = L.find (\x -> case x of
                                   OptionRepFactor{} -> True
                                   _                 -> False
                          ) options
        duration_m = L.find (\x -> case x of
                                   OptionDuration{} -> True
                                   _                -> False
                            ) options
        factor = maybe (bRepFactor def)
                       (\(OptionRepFactor _ n') -> fromInteger $ extractPNInteger n')
                       factor_m
    duration <- maybeM (return $ bBacklogDuration def)
                (\(OptionDuration _ interval) -> do
                    i <- bind (interval :: Interval)
                    return $ fromIntegral (calendarDiffTimeToMs i) `div` 1000
                ) (pure duration_m)
    return BoundStreamOptions { bRepFactor       = factor
                              , bBacklogDuration = duration
                              }

type instance BoundType [ConnectorOption] = BoundConnectorOptions
instance Bind [ConnectorOption] where
  bind ps = do
    opts <- foldM (\acc (ConnectorProperty _ key expr) -> do
                      k <- bind key
                      e <- bind expr
                      let v = toValue e
                      return $ HM.insert k v acc
                  ) HM.empty ps
    return $ BoundConnectorOptions opts
    where toValue (BoundExprConst _ c) = Aeson.toJSON c

type instance BoundType Create = BoundCreate
instance Bind Create where
  bind (DCreate  pos hIdent)                     =
    throwSQLException BindException pos $ "CREATE STREAM should have a schema. If you want to create a stream without schema, disable flag `hstream_enable_schema`."
  bind (CreateOp pos hIdent options)             = do
    throwSQLException BindException pos $ "CREATE STREAM should have a schema. If you want to create a stream without schema, disable flag `hstream_enable_schema`."
  bind (DCreateWithSchema _ hIdent schemaCols) = do
    ident  <- bind hIdent
    schema <- bind (hIdent, schemaCols)
    return $ BoundCreate ident schema def
  bind (CreateAs   _ hIdent select)            = do
    ident <- bind hIdent
    sel   <- bind select
    opt   <- bind ([] :: [StreamOption])
    return $ BoundCreateAs ident sel opt
  bind (CreateAsOp _ hIdent select options)    = do
    ident <- bind hIdent
    sel   <- bind select
    opt   <- bind options
    return $ BoundCreateAs ident sel opt
  bind (CreateSourceConnector _ s t options)   = do
    s'  <- bind s
    t'  <- bind t
    opt <- bind options
    return $ BoundCreateConnector "SOURCE" s' t' False opt
  bind (CreateSourceConnectorIf _ s t options) = do
    s'  <- bind s
    t'  <- bind t
    opt <- bind options
    return $ BoundCreateConnector "SOURCE" s' t' True opt
  bind (CreateSinkConnector _ s t options)     = do
    s'  <- bind s
    t'  <- bind t
    opt <- bind options
    return $ BoundCreateConnector "SINK" s' t' False opt
  bind (CreateSinkConnectorIf _ s t options)   = do
    s'  <- bind s
    t'  <- bind t
    opt <- bind options
    return $ BoundCreateConnector "SINK" s' t' True opt
  bind (CreateView _ s select)                 = do
    s'  <- bind s
    sel <- bind select
    return $ BoundCreateView s' sel

---- DROP
data BoundDrop
  = BoundDrop   BoundDropOption Text
  | BoundDropIf BoundDropOption Text
  deriving (Eq, Show, Generic, Aeson.ToJSON, Aeson.FromJSON)
type instance BoundType Drop = BoundDrop
instance Bind Drop where
  bind (DDrop  _ dropOp x) = do
    op' <- bind dropOp
    x'  <- bind x
    return $ BoundDrop op' x'
  bind (DropIf _ dropOp x) = do
    op' <- bind dropOp
    x'  <- bind x
    return $ BoundDropIf op' x'

data BoundDropOption
  = BoundDropConnector
  | BoundDropStream
  | BoundDropView
  | BoundDropQuery
  deriving (Eq, Show, Generic, Aeson.ToJSON, Aeson.FromJSON)
type instance BoundType DropOption = BoundDropOption
instance Bind DropOption where
  bind (DropConnector _) = return $ BoundDropConnector
  bind (DropStream _)    = return $ BoundDropStream
  bind (DropView   _)    = return $ BoundDropView
  bind (DropQuery   _)   = return $ BoundDropQuery
