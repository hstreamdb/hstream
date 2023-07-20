{-# LANGUAGE DeriveAnyClass    #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies      #-}

module HStream.SQL.Binder.SQL where

import qualified Data.Aeson                as Aeson
import           GHC.Generics              (Generic)

import           HStream.SQL.Abs
import           HStream.SQL.Binder.Common
import           HStream.SQL.Binder.DDL
import           HStream.SQL.Binder.DML
import           HStream.SQL.Binder.Select

data BoundSQL = BoundQSelect     BoundSelect
              | BoundQPushSelect BoundSelect
              | BoundQCreate     BoundCreate    -- DDL
              | BoundQDrop       BoundDrop      -- DDL
              | BoundQInsert     BoundInsert    -- DML
              | BoundQShow       BoundShow      -- DML
              | BoundQTerminate  BoundTerminate -- DML
              | BoundQExplain    BoundExplain   -- DML
              | BoundQPause      BoundPause     -- DML
              | BoundQResume     BoundResume    -- DML
  deriving (Show, Generic, Aeson.ToJSON, Aeson.FromJSON)

type instance BoundType SQL = BoundSQL
instance Bind SQL where
  bind (QSelect _ select) = do
    select' <- bind select
    return $ BoundQSelect select'
  bind (QPushSelect _ select) = do
    select' <- bind select
    return $ BoundQPushSelect select'
  bind (QCreate _ create) = do
    create' <- bind create
    return $ BoundQCreate create'
  bind (QDrop _ drop) = do
    drop' <- bind drop
    return $ BoundQDrop drop'
  bind (QInsert _ insert) = do
    insert' <- bind insert
    return $ BoundQInsert insert'
  bind (QShow _ show) = do
    show' <- bind show
    return $ BoundQShow show'
  bind (QTerminate _ terminate) = do
    terminate' <- bind terminate
    return $ BoundQTerminate terminate'
  bind (QExplain _ explain) = do
    explain' <- bind explain
    return $ BoundQExplain explain'
  bind (QPause _ pause) = do
    pause' <- bind pause
    return $ BoundQPause pause'
  bind (QResume _ resume) = do
    resume' <- bind resume
    return $ BoundQResume resume'
